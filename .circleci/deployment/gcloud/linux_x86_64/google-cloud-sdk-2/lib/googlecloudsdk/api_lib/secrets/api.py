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
"""Shared utilities to access the Google Secret Manager API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions
from apitools.base.py import list_pager
from googlecloudsdk.api_lib.util import apis


def GetClient(version=None):
  """Get the default client."""
  return apis.GetClientInstance('secretmanager', version or
                                apis.ResolveVersion('secretmanager'))


def GetMessages(version=None):
  """Get the default messages module."""
  return apis.GetMessagesModule('secretmanager', version or
                                apis.ResolveVersion('secretmanager'))


def _FormatUpdateMask(update_mask):
  return ','.join(update_mask)


def _MakeReplicationMessage(messages, policy, locations, keys):
  """Create a replication message from its components."""
  replication = messages.Replication(automatic=messages.Automatic())
  if policy == 'automatic' and keys:
    replication = messages.Replication(
        automatic=messages.Automatic(
            customerManagedEncryption=messages.CustomerManagedEncryption(
                kmsKeyName=keys[0])))
  if policy == 'user-managed':
    replicas = []
    for i, location in enumerate(locations):
      if i < len(keys):
        replicas.append(
            messages.Replica(
                location=location,
                customerManagedEncryption=messages
                .CustomerManagedEncryption(kmsKeyName=keys[i])))
      else:
        replicas.append(messages.Replica(location=locations[i]))

    replication = messages.Replication(
        userManaged=messages.UserManaged(replicas=replicas))
  return replication


class Client(object):
  """Base class for all clients."""

  def __init__(self, client=None, messages=None):
    self.client = client or GetClient()
    self.messages = messages or self.client.MESSAGES_MODULE


class Locations(Client):
  """High-level client for locations."""

  def __init__(self, client=None, messages=None):
    super(Locations, self).__init__(client, messages)
    self.service = self.client.projects_locations

  def Get(self, location_ref):
    """Get the location with the given name."""
    return self.service.Get(
        self.messages.SecretmanagerProjectsLocationsGetRequest(
            name=location_ref.RelativeName()))

  def ListWithPager(self, project_ref, limit):
    """List secrets returning a pager object."""
    request = self.messages.SecretmanagerProjectsLocationsListRequest(
        name=project_ref.RelativeName())

    return list_pager.YieldFromList(
        service=self.service,
        request=request,
        field='locations',
        limit=limit,
        batch_size_attribute='pageSize')


class Secrets(Client):
  """High-level client for secrets."""

  def __init__(self, client=None, messages=None):
    super(Secrets, self).__init__(client, messages)
    self.service = self.client.projects_secrets

  def Create(self, secret_ref, policy, locations, labels, keys=None):
    """Create a secret."""
    keys = keys or []
    replication = _MakeReplicationMessage(self.messages, policy, locations,
                                          keys)
    return self.service.Create(
        self.messages.SecretmanagerProjectsSecretsCreateRequest(
            parent=secret_ref.Parent().RelativeName(),
            secretId=secret_ref.Name(),
            secret=self.messages.Secret(labels=labels,
                                        replication=replication)))

  def Delete(self, secret_ref):
    """Delete a secret."""
    return self.service.Delete(
        self.messages.SecretmanagerProjectsSecretsDeleteRequest(
            name=secret_ref.RelativeName()))

  def Get(self, secret_ref):
    """Get the secret with the given name."""
    return self.service.Get(
        self.messages.SecretmanagerProjectsSecretsGetRequest(
            name=secret_ref.RelativeName()))

  def GetOrNone(self, secret_ref):
    """Attempt to get the secret, returning None if the secret does not exist."""
    try:
      return self.Get(secret_ref=secret_ref)
    except apitools_exceptions.HttpNotFoundError:
      return None

  def ListWithPager(self, project_ref, limit):
    """List secrets returning a pager object."""
    request = self.messages.SecretmanagerProjectsSecretsListRequest(
        parent=project_ref.RelativeName())

    return list_pager.YieldFromList(
        service=self.service,
        request=request,
        field='secrets',
        limit=limit,
        batch_size_attribute='pageSize')

  def AddVersion(self, secret_ref, data):
    """Add a new version of an existing secret."""
    request = self.messages.SecretmanagerProjectsSecretsAddVersionRequest(
        parent=secret_ref.RelativeName(),
        addSecretVersionRequest=self.messages.AddSecretVersionRequest(
            payload=self.messages.SecretPayload(data=data)))
    return self.service.AddVersion(request)

  def Update(self, secret_ref, labels, update_mask):
    """Update a secret."""
    return self.service.Patch(
        self.messages.SecretmanagerProjectsSecretsPatchRequest(
            name=secret_ref.RelativeName(),
            secret=self.messages.Secret(labels=labels),
            updateMask=_FormatUpdateMask(update_mask)))

  def SetReplication(self, secret_ref, policy, locations, keys):
    """Set the replication policy on an existing secret.."""
    replication = _MakeReplicationMessage(self.messages, policy, locations,
                                          keys)
    return self.service.Patch(
        self.messages.SecretmanagerProjectsSecretsPatchRequest(
            name=secret_ref.RelativeName(),
            secret=self.messages.Secret(replication=replication),
            updateMask=_FormatUpdateMask(['replication'])))


class SecretsLatest(Client):
  """High-level client for latest secrets."""

  def __init__(self, client=None, messages=None):
    super(SecretsLatest, self).__init__(client, messages)
    self.service = self.client.projects_secrets_latest

  def Access(self, secret_ref):
    """Access the latest version of a secret."""
    return self.service.Access(
        self.messages.SecretmanagerProjectsSecretsLatestAccessRequest(
            name=secret_ref.RelativeName()))


class Versions(Client):
  """High-level client for secret versions."""

  def __init__(self, client=None, messages=None):
    super(Versions, self).__init__(client, messages)
    self.service = self.client.projects_secrets_versions

  def Access(self, version_ref):
    """Access a specific version of a secret."""
    return self.service.Access(
        self.messages.SecretmanagerProjectsSecretsVersionsAccessRequest(
            name=version_ref.RelativeName()))

  def Destroy(self, version_ref):
    """Destroy a secret version."""
    return self.service.Destroy(
        self.messages.SecretmanagerProjectsSecretsVersionsDestroyRequest(
            name=version_ref.RelativeName()))

  def Disable(self, version_ref):
    """Disable a secret version."""
    return self.service.Disable(
        self.messages.SecretmanagerProjectsSecretsVersionsDisableRequest(
            name=version_ref.RelativeName()))

  def Enable(self, version_ref):
    """Enable a secret version."""
    return self.service.Enable(
        self.messages.SecretmanagerProjectsSecretsVersionsEnableRequest(
            name=version_ref.RelativeName()))

  def Get(self, version_ref):
    """Get the secret version with the given name."""
    return self.service.Get(
        self.messages.SecretmanagerProjectsSecretsVersionsGetRequest(
            name=version_ref.RelativeName()))

  def List(self, secret_ref, limit):
    """List secrets and return an array."""
    request = self.messages.SecretmanagerProjectsSecretsVersionsListRequest(
        parent=secret_ref.RelativeName(), pageSize=limit)
    return self.service.List(request)

  def ListWithPager(self, secret_ref, limit):
    """List secrets returning a pager object."""
    request = self.messages.SecretmanagerProjectsSecretsVersionsListRequest(
        parent=secret_ref.RelativeName(), pageSize=0)
    return list_pager.YieldFromList(
        service=self.service,
        request=request,
        field='versions',
        limit=limit,
        batch_size=0,
        batch_size_attribute='pageSize')
