# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Allows you to write surfaces in terms of logical Eventflow operations."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import functools
import random

from apitools.base.py import exceptions as api_exceptions
from googlecloudsdk.api_lib.events import broker
from googlecloudsdk.api_lib.events import cloud_run
from googlecloudsdk.api_lib.events import configmap
from googlecloudsdk.api_lib.events import custom_resource_definition
from googlecloudsdk.api_lib.events import iam_util
from googlecloudsdk.api_lib.events import metric_names
from googlecloudsdk.api_lib.events import source
from googlecloudsdk.api_lib.events import trigger
from googlecloudsdk.api_lib.run import secret
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.api_lib.util import apis_internal
from googlecloudsdk.command_lib.events import exceptions
from googlecloudsdk.command_lib.events import stages
from googlecloudsdk.command_lib.events import util
from googlecloudsdk.command_lib.run import serverless_operations
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.command_lib.util.apis import registry
from googlecloudsdk.core import metrics
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources
from googlecloudsdk.core import yaml


_EVENT_SOURCES_LABEL_SELECTOR = 'duck.knative.dev/source=true'

_METADATA_LABELS_FIELD = 'metadata.labels'

_SERVICE_ACCOUNT_KEY_COLLECTION = 'iam.projects.serviceAccounts.keys'

_ANTHOS_EVENTS_CLIENT_NAME = 'anthosevents'
_ANTHOS_EVENTS_CLIENT_VERSION = 'v1beta1'
_CORE_CLIENT_VERSION = 'v1'
_OPERATOR_CLIENT_VERSION = 'v1alpha1'

_CLOUD_RUN_EVENTS_NAMESPACE = 'cloud-run-events'
_DEFAULT_SOURCES_KEY = 'google-cloud-sources-key'
SOURCES_KEY = 'google-cloud-key'

_CLUSTER_INITIALIZED_ANNOTATION = 'events.cloud.google.com/initialized'
_CONTROL_PLANE_NAMESPACE = 'cloud-run-events'
_CONFIG_GCP_AUTH_NAME = 'config-gcp-auth'
_CONFIGMAP_COLLECTION = 'anthosevents.api.v1.namespaces.configmaps'
_SECRET_COLLECTION = 'anthosevents.api.v1.namespaces.secrets'

_CLOUD_RUN_RELATIVE_NAME = 'namespaces/cloud-run-system/cloudruns/cloud-run'


def Connect(conn_info):
  """Provide a AnthosEventsOperations instance to use.

  If we're using the GKE Serverless Add-on, connect to the relevant cluster.
  Otherwise, connect to the right region of GSE.

  Arguments:
    conn_info: a ConnectionInfo

  Returns:
    An AnthosEventsOperation instance.
  """
  # pylint: disable=protected-access
  v1beta1_client = apis_internal._GetClientInstance(
      _ANTHOS_EVENTS_CLIENT_NAME,
      _ANTHOS_EVENTS_CLIENT_VERSION,

      # Only check response if not connecting to GKE
      check_response_func=apis.CheckResponseForApiEnablement()
      if conn_info.supports_one_platform else None,
      http_client=conn_info.HttpClient())

  # This client is used for working with core resources (e.g. Secrets) in
  # Cloud Run for Anthos.
  v1_client = apis_internal._GetClientInstance(
      _ANTHOS_EVENTS_CLIENT_NAME,
      _CORE_CLIENT_VERSION,
      http_client=conn_info.HttpClient())

  v1alpha1_client = apis_internal._GetClientInstance(
      _ANTHOS_EVENTS_CLIENT_NAME,
      _OPERATOR_CLIENT_VERSION,
      http_client=conn_info.HttpClient())

  # pylint: enable=protected-access
  return AnthosEventsOperations(conn_info.api_version, conn_info.region,
                                v1_client, v1alpha1_client, v1beta1_client)


# TODO(b/149793348): Remove this grace period
_POLLING_GRACE_PERIOD = datetime.timedelta(seconds=15)


class TimeLockedUnfailingConditionPoller(serverless_operations.ConditionPoller):
  """Condition poller that never fails and is only done on success for a period of time.

  Knative Eventing occasionally returns Ready == False on a resource that will
  shortly become Ready == True. In these cases, we cannot rely upon that False
  status as an indication of a terminal failure. Instead, only Ready == True can
  be relied upon as a terminal state and all other statuses (False, Unknown)
  simply mean not currently successful, but provide no indication if this is a
  temporary or permanent state.

  This condition poller never fails a stage for that reason, and therefore is
  never done until successful.

  This behavior only exists for a period of time, after which it acts like a
  normal condition poller.
  """

  def __init__(self,
               getter,
               tracker,
               dependencies=None,
               grace_period=_POLLING_GRACE_PERIOD):
    super(TimeLockedUnfailingConditionPoller,
          self).__init__(getter, tracker, dependencies)
    self._grace_period = grace_period
    self._start_time = datetime.datetime.now()

  def _HasGracePeriodPassed(self):
    return datetime.datetime.now() - self._start_time > self._grace_period

  def IsDone(self, conditions):
    """Within grace period -  this only checks for IsReady rather than IsTerminal.

    Args:
      conditions: A condition.Conditions object.

    Returns:
      A bool indicating whether `conditions` is ready.
    """
    if self._HasGracePeriodPassed():
      return super(TimeLockedUnfailingConditionPoller, self).IsDone(conditions)

    if conditions is None:
      return False
    return conditions.IsReady()

  def Poll(self, unused_ref):
    """Within grace period - this polls like normal but does not raise on failure.

    Args:
      unused_ref: A string representing the operation reference. Unused and may
        be None.

    Returns:
      A condition.Conditions object or None if there's no conditions on the
        resource or if the conditions are not fresh (the generation on the
        resource doesn't match the observedGeneration)
    """
    if self._HasGracePeriodPassed():
      return super(TimeLockedUnfailingConditionPoller, self).Poll(unused_ref)

    conditions = self.GetConditions()

    if conditions is None or not conditions.IsFresh():
      return None

    conditions_message = conditions.DescriptiveMessage()
    if conditions_message:
      self._tracker.UpdateHeaderMessage(conditions_message)

    self._PollTerminalSubconditions(conditions, conditions_message)

    if conditions.IsReady():
      self._tracker.UpdateHeaderMessage(self._ready_message)
      # TODO(b/120679874): Should not have to manually call Tick()
      self._tracker.Tick()

    return conditions

  def _PossiblyFailStage(self, condition, message):
    """Within grace period - stages are never marked as failed."""
    if self._HasGracePeriodPassed():
      # pylint:disable=protected-access
      return super(TimeLockedUnfailingConditionPoller,
                   self)._PossiblyFailStage(condition, message)


class BrokerConditionPoller(TimeLockedUnfailingConditionPoller):
  """A ConditionPoller for brokers."""


class TriggerConditionPoller(TimeLockedUnfailingConditionPoller):
  """A ConditionPoller for triggers."""


class SourceConditionPoller(TimeLockedUnfailingConditionPoller):
  """A ConditionPoller for sources."""


class CloudRunConditionPoller(TimeLockedUnfailingConditionPoller):
  """A ConditionPoller for cloud run resources."""


class AnthosEventsOperations(object):
  """Client used by Eventflow to communicate with the actual API."""

  def __init__(self, api_version, region, v1_client, v1alpha1_client,
               v1beta1_client):
    """Inits EventflowOperations with given API clients.

    Args:
      api_version: Version of resources & clients (v1alpha1, v1beta1)
      region: str, The region of the control plane if operating against
        hosted Cloud Run, else None.
      v1_client: The API client for interacting with Kubernetes Cloud Run APIs.
      v1alpha1_client: The v1alpha1 API client
      v1beta1_client: The v1beta1 API client
    """

    self._api_version = api_version

    # used for triggers
    self._client = v1beta1_client

    # used for cloudsources
    self._cloudsources_client = v1_client

    # used for operator
    self._operator_client = v1alpha1_client

    # used for namespaces and secrets
    self._core_client = v1_client

    # used for customresourcedefinition
    self._crd_client = v1_client

    self._region = region

  def IsCluster(self):
    return True

  @property
  def api_version(self):
    return self._api_version

  @property
  def client(self):
    return self._client

  @property
  def messages(self):
    return self._client.MESSAGES_MODULE

  def GetTrigger(self, trigger_ref):
    """Returns the referenced trigger."""
    request = self.messages.AnthoseventsNamespacesTriggersGetRequest(
        name=trigger_ref.RelativeName())
    try:
      with metrics.RecordDuration(metric_names.GET_TRIGGER):
        response = self._client.namespaces_triggers.Get(request)
    except api_exceptions.HttpNotFoundError:
      return None
    return trigger.Trigger(response, self.messages)

  def CreateTrigger(self, trigger_ref, source_obj, event_type, trigger_filters,
                    target_service, broker_name):
    """Create a trigger that sends events to the target service.

    Args:
      trigger_ref: googlecloudsdk.core.resources.Resource, trigger resource.
      source_obj: source.Source. The source object to be created after the
        trigger. If creating a custom event, this may be None.
      event_type: str, the event type the source will filter by.
      trigger_filters: collections.OrderedDict()
      target_service: str, name of the Cloud Run service to subscribe.
      broker_name: str, name of the broker to act as a sink for the source.

    Returns:
      trigger.Trigger of the created trigger.
    """
    trigger_obj = trigger.Trigger.New(self._client, trigger_ref.Parent().Name())
    trigger_obj.name = trigger_ref.Name()
    if source_obj is not None:
      trigger_obj.dependency = source_obj
      # TODO(b/141617597): Set to str(random.random()) without prepended string
      trigger_obj.filter_attributes[
          trigger.SOURCE_TRIGGER_LINK_FIELD] = 'link{}'.format(random.random())
    trigger_obj.filter_attributes[trigger.EVENT_TYPE_FIELD] = event_type

    # event/flags.py ensures filter key doesn't include disallowed fields
    trigger_obj.filter_attributes.update(trigger_filters)

    trigger_obj.subscriber = target_service
    trigger_obj.broker = broker_name
    request = self.messages.AnthoseventsNamespacesTriggersCreateRequest(
        trigger=trigger_obj.Message(),
        parent=trigger_ref.Parent().RelativeName())
    try:
      with metrics.RecordDuration(metric_names.CREATE_TRIGGER):
        response = self._client.namespaces_triggers.Create(request)
    except api_exceptions.HttpConflictError:
      raise exceptions.TriggerCreationError(
          'Trigger [{}] already exists.'.format(trigger_obj.name))

    return trigger.Trigger(response, self.messages)

  def PollTrigger(self, trigger_ref, tracker):
    """Wait for trigger to be Ready == True."""
    trigger_getter = functools.partial(self.GetTrigger, trigger_ref)
    poller = TriggerConditionPoller(trigger_getter, tracker,
                                    stages.TriggerSourceDependencies())
    util.WaitForCondition(poller, exceptions.TriggerCreationError)

  def ListTriggers(self, namespace_ref):
    """Returns a list of existing triggers in the given namespace."""
    request = self.messages.AnthoseventsNamespacesTriggersListRequest(
        parent=namespace_ref.RelativeName())
    with metrics.RecordDuration(metric_names.LIST_TRIGGERS):
      response = self._client.namespaces_triggers.List(request)
    return [trigger.Trigger(item, self.messages) for item in response.items]

  def DeleteTrigger(self, trigger_ref):
    """Deletes the referenced trigger."""
    request = self.messages.AnthoseventsNamespacesTriggersDeleteRequest(
        name=trigger_ref.RelativeName())
    try:
      with metrics.RecordDuration(metric_names.DELETE_TRIGGER):
        self._client.namespaces_triggers.Delete(request)
    except api_exceptions.HttpNotFoundError:
      raise exceptions.TriggerNotFound(
          'Trigger [{}] not found.'.format(trigger_ref.Name()))

  def ClientFromCrd(self, source_crd):
    """Returns the correct client for the source."""
    if 'v1' in source_crd.getActiveSourceVersions():
      client = self._cloudsources_client
    else:
      client = self._client
    return client

  def _FindSourceMethod(self, source_crd, method_name):
    """Returns the given method for the given source kind.

    Because every source has its own methods for rpc requests, this helper is
    used to get the underlying methods for a request against a given source
    type. Preferred usage of this private message is via the public
    methods: self.Source{Method_name}Method.

    Args:
      source_crd: custom_resource_definition.SourceCustomResourceDefinition,
        source CRD of the type we want to make a request against.
      method_name: str, the method name (e.g. "get", "create", "list", etc.)

    Returns:
      registry.APIMethod, holds information for the requested method.
    """
    if 'v1' in source_crd.getActiveSourceVersions():
      source_api_version = 'v1'
    else:
      source_api_version = 'v1beta1'
    return registry.GetMethod(
        util.ANTHOS_SOURCE_COLLECTION_NAME.format(
            plural_kind=source_crd.source_kind_plural),
        method_name,
        api_version=source_api_version)

  def SourceGetMethod(self, source_crd):
    """Returns the request method for a Get request of this source."""
    return self._FindSourceMethod(source_crd, 'get')

  def SourceCreateMethod(self, source_crd):
    """Returns the request method for a Create request of this source."""
    return self._FindSourceMethod(source_crd, 'create')

  def SourceDeleteMethod(self, source_crd):
    """Returns the request method for a Delete request of this source."""
    return self._FindSourceMethod(source_crd, 'delete')

  def GetSource(self, source_ref, source_crd):
    """Returns the referenced source."""
    client = self.ClientFromCrd(source_crd)
    messages = client.MESSAGES_MODULE

    request_method = self.SourceGetMethod(source_crd)
    request_message_type = request_method.GetRequestType()
    request = request_message_type(name=source_ref.RelativeName())
    try:
      with metrics.RecordDuration(metric_names.GET_SOURCE):
        response = request_method.Call(request, client=client)
    except api_exceptions.HttpNotFoundError:
      return None
    return source.Source(response, messages, source_crd.source_kind)

  def CreateSource(self, source_obj, source_crd, owner_trigger, namespace_ref,
                   broker_name, parameters):
    """Create an source with the specified event type and owner trigger.

    Args:
      source_obj: source.Source. The source object being created.
      source_crd: custom_resource_definition.SourceCRD, the source crd for the
        source to create
      owner_trigger: trigger.Trigger, trigger to associate as an owner of the
        source.
      namespace_ref: googlecloudsdk.core.resources.Resource, namespace resource.
      broker_name: str, name of the broker to act as a sink.
      parameters: dict, additional parameters to set on the source spec.

    Returns:
      source.Source of the created source.
    """
    client = self.ClientFromCrd(source_crd)
    messages = client.MESSAGES_MODULE

    source_obj.ce_overrides[trigger.SOURCE_TRIGGER_LINK_FIELD] = (
        owner_trigger.filter_attributes[trigger.SOURCE_TRIGGER_LINK_FIELD])

    source_obj.owners.append(
        messages.OwnerReference(
            apiVersion=owner_trigger.apiVersion,
            kind=owner_trigger.kind,
            name=owner_trigger.name,
            uid=owner_trigger.uid,
            controller=True))
    source_obj.set_sink(broker_name, self._api_version)

    # Parse parameters flags into source's spec
    source.ParseDynamicFieldsIntoMessage(source_obj.spec, parameters)
    source.SourceFix(source_obj)

    request_method = self.SourceCreateMethod(source_crd)
    request_message_type = request_method.GetRequestType()
    request = request_message_type(**{
        request_method.request_field: source_obj.Message(),
        'parent': namespace_ref.RelativeName()})
    try:
      with metrics.RecordDuration(metric_names.CREATE_SOURCE):
        response = request_method.Call(request, client=client)
    except api_exceptions.HttpConflictError:
      raise exceptions.SourceCreationError(
          'Source [{}] already exists.'.format(source_obj.name))

    return source.Source(response, messages, source_crd.source_kind)

  def PollSource(self, source_obj, event_type, tracker):
    """Wait for source to be Ready == True."""
    source_ref = util.GetSourceRef(source_obj.name, source_obj.namespace,
                                   event_type.crd, True)
    source_getter = functools.partial(
        self.GetSource, source_ref, event_type.crd)
    poller = SourceConditionPoller(source_getter, tracker,
                                   stages.TriggerSourceDependencies())
    util.WaitForCondition(poller, exceptions.SourceCreationError)
    # Manually complete the stage indicating source readiness because we can't
    # track the Ready condition in the ConditionPoller.
    tracker.CompleteStage(stages.SOURCE_READY)

  def DeleteSource(self, source_ref, source_crd):
    """Deletes the referenced source."""
    client = self.ClientFromCrd(source_crd)

    request_method = self.SourceDeleteMethod(source_crd)
    request_message_type = request_method.GetRequestType()
    request = request_message_type(name=source_ref.RelativeName())
    try:
      with metrics.RecordDuration(metric_names.DELETE_SOURCE):
        request_method.Call(request, client=client)
    except api_exceptions.HttpNotFoundError:
      raise exceptions.SourceNotFound(
          '{} events source [{}] not found.'.format(
              source_crd.source_kind, source_ref.Name()))

  def ListSourceCustomResourceDefinitions(self):
    """Returns a list of CRDs for event sources."""
    # Passing the parent field is only needed for hosted, but shouldn't hurt
    # against an actual cluster
    namespace_ref = resources.REGISTRY.Parse(
        properties.VALUES.core.project.Get(),
        collection='anthosevents.namespaces',
        api_version=self._api_version)

    messages = self._crd_client.MESSAGES_MODULE
    request = messages.AnthoseventsCustomresourcedefinitionsListRequest(
        parent=namespace_ref.RelativeName(),
        labelSelector=_EVENT_SOURCES_LABEL_SELECTOR)
    with metrics.RecordDuration(metric_names.LIST_SOURCE_CRDS):
      response = self._crd_client.customresourcedefinitions.List(request)
    source_crds = [
        custom_resource_definition.SourceCustomResourceDefinition(
            item, messages) for item in response.items
    ]

    # The customresourcedefinition received in listResponse is missing their
    # apiVersion's (intended), so manually insert apiVersion from listResponse.
    for source_crd in source_crds:
      source_crd.setApiVersion(response.apiVersion)

    # Only include CRDs for source kinds that are defined in the api.
    return [s for s in source_crds if hasattr(self.messages, s.source_kind)]

  def CreateBroker(self, namespace_name, broker_name):
    """Creates a broker."""

    messages = self._client.MESSAGES_MODULE

    broker_obj = broker.Broker.New(self._client, namespace_name)
    broker_obj.name = broker_name

    # Validation webhook requires a spec to be provided
    broker_obj.spec = messages.BrokerSpec()

    namespace_full_name = 'namespaces/' + namespace_name

    request = messages.AnthoseventsNamespacesBrokersCreateRequest(
        broker=broker_obj.Message(), parent=namespace_full_name)
    try:
      response = self._client.namespaces_brokers.Create(request)
    except api_exceptions.HttpConflictError:
      raise exceptions.BrokerAlreadyExists(
          'Broker [{}] already exists.'.format(broker_name))
    return response

  def PollBroker(self, broker_full_name, tracker):
    """Wait for broker to be Ready == True."""
    broker_getter = functools.partial(self.GetBroker, broker_full_name)
    poller = BrokerConditionPoller(
        broker_getter, tracker, grace_period=datetime.timedelta(seconds=300))
    util.WaitForCondition(poller, exceptions.BrokerCreationError)

  def GetBroker(self, broker_full_name):
    """Returns the referenced broker.

    Args:
      broker_full_name: name of broker to fetch in the form of
        'namespaces/<namespace>/brokers/<broker>'
    """
    messages = self._client.MESSAGES_MODULE
    request = messages.AnthoseventsNamespacesBrokersGetRequest(
        name=broker_full_name)
    try:
      response = self._client.namespaces_brokers.Get(request)
    except api_exceptions.HttpNotFoundError:
      return None
    return broker.Broker(response, messages)

  def ListBrokers(self, namespace_full_name):
    """Returns a list of existing brokers in the given namespace."""
    messages = self._client.MESSAGES_MODULE
    request = messages.AnthoseventsNamespacesBrokersListRequest(
        parent=namespace_full_name)
    response = self._client.namespaces_brokers.List(request)
    return [broker.Broker(item, messages) for item in response.items]

  def DeleteBroker(self, namespace_name, broker_name):
    """Deletes the referenced broker."""

    # represents namespaces/<namespace_name>/brokers/<broker_name>
    broker_full_name = 'namespaces/{}/brokers/{}'.format(
        namespace_name, broker_name)

    messages = self._client.MESSAGES_MODULE
    request = self.messages.AnthoseventsNamespacesBrokersDeleteRequest(
        name=broker_full_name)
    try:
      self._client.namespaces_brokers.Delete(request)
    except api_exceptions.HttpNotFoundError:
      raise exceptions.BrokerNotFound(
          'Broker [{}] not found.'.format(broker_name))

  def CreateOrReplaceSourcesSecret(self, namespace_ref):
    """Create or replace the namespaces' sources secret.

    Retrieves default sources secret 'google-cloud-sources-key' from
    cloud-run-events and copies into secret 'google-cloud-key' into target
    namespace.

    Args:
      namespace_ref: googolecloudsdk.core.resources.Resource, namespace resource

    Returns:
      None
    """
    messages = self._core_client.MESSAGES_MODULE
    default_secret_full_name = 'namespaces/{}/secrets/{}'.format(
        _CLOUD_RUN_EVENTS_NAMESPACE, _DEFAULT_SOURCES_KEY)
    secret_ref = resources.REGISTRY.Parse(
        SOURCES_KEY,
        params={'namespacesId': namespace_ref.Name()},
        collection=_SECRET_COLLECTION,
        api_version='v1')

    # Retrieve default sources secret.
    try:
      request = messages.AnthoseventsApiV1NamespacesSecretsGetRequest(
          name=default_secret_full_name)
      response = self._core_client.api_v1_namespaces_secrets.Get(request)
    except api_exceptions.HttpNotFoundError:
      raise exceptions.SecretNotFound(
          'Secret [{}] not found in namespace [{}].'.format(
              _DEFAULT_SOURCES_KEY, _CLOUD_RUN_EVENTS_NAMESPACE))

    existing_secret_obj = secret.Secret(response, messages)

    secret_obj = secret.Secret.New(self._core_client,
                                   secret_ref.Parent().Name())
    secret_obj.name = secret_ref.Name()
    secret_obj.data['key.json'] = existing_secret_obj.data['key.json']

    try:
      # Create secret or replace if already exists.
      request = messages.AnthoseventsApiV1NamespacesSecretsCreateRequest(
          secret=secret_obj.Message(),
          parent=secret_ref.Parent().RelativeName())
      self._core_client.api_v1_namespaces_secrets.Create(request)
    except api_exceptions.HttpConflictError:
      request = messages.AnthoseventsApiV1NamespacesSecretsReplaceSecretRequest(
          secret=secret_obj.Message(), name=secret_ref.RelativeName())
      response = self._core_client.api_v1_namespaces_secrets.ReplaceSecret(
          request)

  def CreateOrReplaceServiceAccountSecret(self, secret_ref,
                                          service_account_ref):
    """Create a new secret or replace an existing one.

    Secret data contains the key of the given service account.

    Args:
      secret_ref: googlecloudsdk.core.resources.Resource, secret resource.
      service_account_ref: googlecloudsdk.core.resources.Resource, service
        account whose key will be used to create/replace the secret.

    Returns:
      (secret.Secret, googlecloudsdk.core.resources.Resource): tuple of the
        wrapped Secret resource and a ref to the created service account key.
    """
    secret_obj = secret.Secret.New(
        self._core_client, secret_ref.Parent().Name())
    secret_obj.name = secret_ref.Name()
    key = iam_util.CreateServiceAccountKey(service_account_ref)
    secret_obj.data['key.json'] = key.privateKeyData
    key_ref = resources.REGISTRY.ParseResourceId(
        _SERVICE_ACCOUNT_KEY_COLLECTION, key.name, {})

    messages = self._core_client.MESSAGES_MODULE
    with metrics.RecordDuration(metric_names.CREATE_OR_REPLACE_SECRET):
      # Create secret or replace if already exists.
      try:
        request = messages.AnthoseventsApiV1NamespacesSecretsCreateRequest(
            secret=secret_obj.Message(),
            parent=secret_ref.Parent().RelativeName())
        response = self._core_client.api_v1_namespaces_secrets.Create(request)
      except api_exceptions.HttpConflictError:
        request = messages.AnthoseventsApiV1NamespacesSecretsReplaceSecretRequest(
            secret=secret_obj.Message(), name=secret_ref.RelativeName())
        response = self._core_client.api_v1_namespaces_secrets.ReplaceSecret(
            request)
    return secret.Secret(response, messages), key_ref

  def IsClusterInitialized(self):
    """Returns whether the cluster has been initialized for eventing."""
    configmap_obj = self._GetConfigMap(
        _ConfigMapRef(_CONTROL_PLANE_NAMESPACE, _CONFIG_GCP_AUTH_NAME))
    if configmap_obj is None:
      return False
    return configmap_obj.annotations.get(
        _CLUSTER_INITIALIZED_ANNOTATION) == 'true'

  def MarkClusterInitialized(self):
    """Marks the cluster as initialized for eventing.

    This creates or updates a ConfigMap which involves adding an annotation
    and setting some default configuration for eventing to use.
    """
    configmap_obj = self._GetConfigMap(
        _ConfigMapRef(_CONTROL_PLANE_NAMESPACE, _CONFIG_GCP_AUTH_NAME))
    if configmap_obj is None:
      configmap_obj = configmap.ConfigMap.New(self._core_client,
                                              _CONTROL_PLANE_NAMESPACE)
      configmap_obj.name = _CONFIG_GCP_AUTH_NAME
      self._PopulateDefaultAuthConfig(configmap_obj)
      self._CreateConfigMap(configmap_obj)
    else:
      self._PopulateDefaultAuthConfig(configmap_obj)
      self._ReplaceConfigMap(configmap_obj)

  def _PopulateDefaultAuthConfig(self, configmap_obj):
    """Populates the default eventing config and adds an annotation."""
    auth_config = yaml.load(
        configmap_obj.data.get('default-auth-config', '{}'))
    auth_config['clusterDefaults'] = {
        'secret': {
            'name': 'google-cloud-key',
            'key': 'key.json',
        }
    }
    configmap_obj.data['default-auth-config'] = yaml.dump(auth_config)
    configmap_obj.annotations[_CLUSTER_INITIALIZED_ANNOTATION] = 'true'

  def _GetConfigMap(self, configmap_ref):
    messages = self._core_client.MESSAGES_MODULE
    request = messages.AnthoseventsApiV1NamespacesConfigmapsGetRequest(
        name=configmap_ref.RelativeName(),
    )
    try:
      response = self._core_client.api_v1_namespaces_configmaps.Get(request)
    except api_exceptions.HttpNotFoundError:
      return None
    return configmap.ConfigMap(response, messages)

  def _CreateConfigMap(self, configmap_obj):
    messages = self._core_client.MESSAGES_MODULE
    ref = _ConfigMapRef(configmap_obj.namespace, configmap_obj.name)
    request = messages.AnthoseventsApiV1NamespacesConfigmapsCreateRequest(
        configMap=configmap_obj.Message(),
        parent=ref.Parent().RelativeName(),
    )
    self._core_client.api_v1_namespaces_configmaps.Create(request)

  def _ReplaceConfigMap(self, configmap_obj):
    messages = self._core_client.MESSAGES_MODULE
    ref = _ConfigMapRef(configmap_obj.namespace, configmap_obj.name)
    request = messages.AnthoseventsApiV1NamespacesConfigmapsReplaceConfigMapRequest(
        configMap=configmap_obj.Message(),
        name=ref.RelativeName(),
    )
    self._core_client.api_v1_namespaces_configmaps.ReplaceConfigMap(request)

  def GetCloudRun(self):
    """Returns operator's cloudrun resource."""
    messages = self._operator_client.MESSAGES_MODULE
    request = messages.AnthoseventsNamespacesCloudrunsGetRequest(
        name=_CLOUD_RUN_RELATIVE_NAME)
    try:
      response = self._operator_client.namespaces_cloudruns.Get(request)
    except api_exceptions.HttpNotFoundError:
      return None
    return cloud_run.CloudRun(response, messages)

  def UpdateCloudRunWithEventingEnabled(self):
    """Updates operator's cloud run resource spec.eventing.enabled to true."""
    messages = self._operator_client.MESSAGES_MODULE
    cloud_run_message = messages.CloudRun()
    arg_utils.SetFieldInMessage(cloud_run_message, 'spec.eventing.enabled',
                                True)

    # We need to specify a special content-type for k8s to accept our PATCH.
    # However, this appears to only be settable at the client level, not at
    # the request level. So we'll update the client for our request, and the
    # set it back to the old value afterwards.

    old_additional_headers = {}
    old_additional_headers = self._operator_client.additional_http_headers
    additional_headers = old_additional_headers.copy()
    additional_headers['content-type'] = 'application/merge-patch+json'
    try:
      self._operator_client.additional_http_headers = additional_headers

      request = messages.AnthoseventsNamespacesCloudrunsPatchRequest(
          name=_CLOUD_RUN_RELATIVE_NAME, cloudRun=cloud_run_message)
      response = self._operator_client.namespaces_cloudruns.Patch(request)
    finally:
      self._operator_client.additional_http_headers = old_additional_headers
    return response

  def PollCloudRunResource(self, tracker):
    """Wait for Cloud Run resource to be Ready."""
    cloud_run_getter = functools.partial(self.GetCloudRun)

    poller = CloudRunConditionPoller(
        cloud_run_getter, tracker, grace_period=datetime.timedelta(seconds=180))
    util.WaitForCondition(
        poller,
        exceptions.EventingInstallError(
            'Eventing failed to install within 180 seconds, please try rerunning the command'
        ))


def _ConfigMapRef(namespace, name):
  return resources.REGISTRY.Parse(
      name,
      params={'namespacesId': namespace},
      collection=_CONFIGMAP_COLLECTION,
      api_version=_CORE_CLIENT_VERSION)
