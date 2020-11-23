# -*- coding: utf-8 -*- #
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Utilities for dealing with AI Platform endpoints API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from apitools.base.py import extra_types
from apitools.base.py import list_pager
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import errors
from googlecloudsdk.command_lib.ai import flags
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources


def _ParseModel(model_id, location_id):
  """Parses a model ID into a model resource object."""
  return resources.REGISTRY.Parse(
      model_id,
      params={
          'locationsId': location_id,
          'projectsId': properties.VALUES.core.project.GetOrFail
      },
      collection='aiplatform.projects.locations.models')


def _ConvertPyListToMessageList(message_type, values):
  return [encoding.PyValueToMessage(message_type, v) for v in values]


class EndpointsClient(object):
  """High-level client for the AI Platform endpoints surface."""

  def __init__(self, client=None, messages=None, version=None):
    self.client = client or apis.GetClientInstance(
        constants.AI_PLATFORM_API_NAME,
        constants.AI_PLATFORM_API_VERSION[version])
    self.messages = messages or self.client.MESSAGES_MODULE

  def CreateBeta(self, location_ref, args):
    """Create a new endpoint."""
    labels = labels_util.ParseCreateArgs(
        args, self.messages.GoogleCloudAiplatformV1beta1Endpoint.LabelsValue)
    req = self.messages.AiplatformProjectsLocationsEndpointsCreateRequest(
        parent=location_ref.RelativeName(),
        googleCloudAiplatformV1beta1Endpoint=self.messages
        .GoogleCloudAiplatformV1beta1Endpoint(
            displayName=args.display_name,
            description=args.description,
            labels=labels))
    return self.client.projects_locations_endpoints.Create(req)

  def Delete(self, endpoint_ref):
    """Delete an existing endpoint."""
    req = self.messages.AiplatformProjectsLocationsEndpointsDeleteRequest(
        name=endpoint_ref.RelativeName())
    return self.client.projects_locations_endpoints.Delete(req)

  def Get(self, endpoint_ref):
    """Get details about an endpoint."""
    req = self.messages.AiplatformProjectsLocationsEndpointsGetRequest(
        name=endpoint_ref.RelativeName())
    return self.client.projects_locations_endpoints.Get(req)

  def List(self, location_ref):
    """List endpoints in the project."""
    req = self.messages.AiplatformProjectsLocationsEndpointsListRequest(
        parent=location_ref.RelativeName())
    return list_pager.YieldFromList(
        self.client.projects_locations_endpoints,
        req,
        field='endpoints',
        batch_size_attribute='pageSize')

  def PatchBeta(self, endpoint_ref, args):
    """Update a endpoint."""
    endpoint = self.messages.GoogleCloudAiplatformV1beta1Endpoint()
    update_mask = []

    def GetLabels():
      return self.Get(endpoint_ref).labels
    labels_update = labels_util.ProcessUpdateArgsLazy(
        args, self.messages.GoogleCloudAiplatformV1beta1Endpoint
        .LabelsValue, GetLabels)
    if labels_update.needs_update:
      endpoint.labels = labels_update.labels
      update_mask.append('labels')

    if args.display_name is not None:
      endpoint.displayName = args.display_name
      update_mask.append('display_name')

    if args.traffic_split is not None:
      additional_properties = []
      for key, value in sorted(args.traffic_split.items()):
        additional_properties.append(
            endpoint.TrafficSplitValue().AdditionalProperty(
                key=key, value=value))
      endpoint.trafficSplit = endpoint.TrafficSplitValue(
          additionalProperties=additional_properties)
      update_mask.append('traffic_split')

    if args.clear_traffic_split:
      endpoint.trafficSplit = None
      update_mask.append('traffic_split')

    if args.description is not None:
      endpoint.description = args.description
      update_mask.append('description')

    if not update_mask:
      raise errors.NoFieldsSpecifiedError('No updates requested.')

    req = self.messages.AiplatformProjectsLocationsEndpointsPatchRequest(
        name=endpoint_ref.RelativeName(),
        googleCloudAiplatformV1beta1Endpoint=endpoint,
        updateMask=','.join(update_mask))
    return self.client.projects_locations_endpoints.Patch(req)

  def PredictBeta(self, endpoint_ref, instances_json):
    """Send online prediction request to an endpoint."""
    predict_request = self.messages.GoogleCloudAiplatformV1beta1PredictRequest(
        instances=_ConvertPyListToMessageList(
            extra_types.JsonValue, instances_json['instances']))
    if 'parameters' in instances_json:
      predict_request.parameters = encoding.PyValueToMessage(
          extra_types.JsonValue, instances_json['parameters'])

    req = self.messages.AiplatformProjectsLocationsEndpointsPredictRequest(
        endpoint=endpoint_ref.RelativeName(),
        googleCloudAiplatformV1beta1PredictRequest=predict_request)
    return self.client.projects_locations_endpoints.Predict(req)

  def ExplainBeta(self, endpoint_ref, instances_json, args):
    """Send online explanation request to an endpoint."""
    explain_request = self.messages.GoogleCloudAiplatformV1beta1ExplainRequest(
        instances=_ConvertPyListToMessageList(
            extra_types.JsonValue, instances_json['instances']))
    if 'parameters' in instances_json:
      explain_request.parameters = encoding.PyValueToMessage(
          extra_types.JsonValue, instances_json['parameters'])
    if args.deployed_model_id is not None:
      explain_request.deployedModelId = args.deployed_model_id

    req = self.messages.AiplatformProjectsLocationsEndpointsExplainRequest(
        endpoint=endpoint_ref.RelativeName(),
        googleCloudAiplatformV1beta1ExplainRequest=explain_request)
    return self.client.projects_locations_endpoints.Explain(req)

  def DeployModelBeta(self, endpoint_ref, args):
    """Deploy a model to an existing endpoint."""
    model_ref = _ParseModel(args.model, args.region)

    deployed_model = None
    if args.machine_type is not None:
      # dedicated resources
      machine_spec = self.messages.GoogleCloudAiplatformV1beta1MachineSpec(
          machineType=args.machine_type)
      accelerator = flags.ParseAcceleratorFlag(args.accelerator,
                                               constants.BETA_VERSION)
      if accelerator is not None:
        machine_spec.acceleratorType = accelerator.acceleratorType
        machine_spec.acceleratorCount = accelerator.acceleratorCount

      dedicated_resources =\
          self.messages.GoogleCloudAiplatformV1beta1DedicatedResources(
              machineSpec=machine_spec)
      # min-replica-count is required and must be >= 1 if models use dedicated
      # resources. Default to 1 if not specified.
      dedicated_resources.minReplicaCount = args.min_replica_count or 1
      if args.max_replica_count is not None:
        dedicated_resources.maxReplicaCount = args.max_replica_count

      deployed_model =\
          self.messages.GoogleCloudAiplatformV1beta1DeployedModel(
              dedicatedResources=dedicated_resources,
              displayName=args.display_name,
              model=model_ref.RelativeName())
    else:
      # automatic resources
      automatic_resources =\
          self.messages.GoogleCloudAiplatformV1beta1AutomaticResources()
      if args.min_replica_count is not None:
        automatic_resources.minReplicaCount = args.min_replica_count
      if args.max_replica_count is not None:
        automatic_resources.maxReplicaCount = args.max_replica_count

      deployed_model = self.messages.GoogleCloudAiplatformV1beta1DeployedModel(
          automaticResources=automatic_resources,
          displayName=args.display_name,
          model=model_ref.RelativeName())

    deployed_model.enableAccessLogging = args.enable_access_logging
    deployed_model.enableContainerLogging = args.enable_container_logging

    if args.IsSpecified('service_account'):
      deployed_model.serviceAccount = args.service_account

    deployed_model_req =\
        self.messages.GoogleCloudAiplatformV1beta1DeployModelRequest(
            deployedModel=deployed_model)

    if args.traffic_split is not None:
      additional_properties = []
      for key, value in sorted(args.traffic_split.items()):
        additional_properties.append(
            deployed_model_req.TrafficSplitValue().AdditionalProperty(
                key=key, value=value))
      deployed_model_req.trafficSplit = deployed_model_req.TrafficSplitValue(
          additionalProperties=additional_properties)

    req = self.messages.AiplatformProjectsLocationsEndpointsDeployModelRequest(
        endpoint=endpoint_ref.RelativeName(),
        googleCloudAiplatformV1beta1DeployModelRequest=deployed_model_req)
    return self.client.projects_locations_endpoints.DeployModel(req)

  def UndeployModelBeta(self, endpoint_ref, args):
    """Undeploy a model from an endpoint."""
    undeployed_model_req =\
        self.messages.GoogleCloudAiplatformV1beta1UndeployModelRequest(
            deployedModelId=args.deployed_model_id)

    if args.traffic_split is not None:
      additional_properties = []
      for key, value in sorted(args.traffic_split.items()):
        additional_properties.append(
            undeployed_model_req.TrafficSplitValue().AdditionalProperty(
                key=key, value=value))
      undeployed_model_req.trafficSplit =\
          undeployed_model_req.TrafficSplitValue(
              additionalProperties=additional_properties)

    req =\
        self.messages.AiplatformProjectsLocationsEndpointsUndeployModelRequest(
            endpoint=endpoint_ref.RelativeName(),
            googleCloudAiplatformV1beta1UndeployModelRequest=\
            undeployed_model_req)
    return self.client.projects_locations_endpoints.UndeployModel(req)
