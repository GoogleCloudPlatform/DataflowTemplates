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
"""Module containing the KCC Declarative Client."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import io
import os
import sys

from googlecloudsdk.command_lib.util.anthos import binary_operations as bin_ops
from googlecloudsdk.command_lib.util.declarative.clients import client_base
from googlecloudsdk.core import execution_utils as exec_utils
from googlecloudsdk.core import properties
from googlecloudsdk.core import yaml
from googlecloudsdk.core.console import progress_tracker
from googlecloudsdk.core.credentials import store
from googlecloudsdk.core.util import files


class ResourceNotFoundException(client_base.ClientException):
  """General Purpose Exception."""


class KccClient(client_base.DeclarativeClient):
  """KRM Yaml Export based Declarative Client."""

  def __init__(self, gcp_account=None, impersonated=False):
    if not gcp_account:
      gcp_account = properties.VALUES.core.account.Get()
    self._export_service = bin_ops.CheckForInstalledBinary(
        'config-connector',
        custom_message='config-connector tool not installed.')
    self._use_account_impersonation = impersonated
    self._account = gcp_account

  def _NormalizeURI(self, uri):
    if 'www.googleapis.com/' in uri:
      api = uri.split('www.googleapis.com/')[1].split('/')[0]
      return uri.replace('www.googleapis.com/{api}'.format(api=api),
                         '{api}.googleapis.com/{api}'.format(api=api))
    return uri

  def _GetToken(self):
    try:
      cred = store.LoadFreshCredential(
          self._account,
          allow_account_impersonation=self._use_account_impersonation)
      return cred.access_token
    except Exception as e:  # pylint: disable=broad-except
      raise client_base.ClientException(
          'Error Configuring KCC Client: [{}]'.format(e))

  def Parse(self, file_path):
    raise NotImplementedError(
        'DeclarativeService Parse function must be implemented.')

  def SerializeAll(self, resources, path=None, scope_field=None):
    if path:
      if not os.path.isdir(path):
        raise client_base.ClientException(
            'Error executing export: "{}" must be an existing directory when --all is specified.'
            .format(path))
      for resource in resources:
        file_name = resource['metadata']['name']
        if scope_field:
          scope = resource['spec'][scope_field]
          file_name = '{}_{}'.format(scope, file_name)
        file_path = os.path.join(path, '{}.yaml'.format(file_name))
        try:
          with files.FileWriter(file_path) as stream:
            yaml.dump(resource, stream=stream)
        except (EnvironmentError, yaml.Error) as e:
          raise client_base.ClientException(
              'Error while serializing resource to file [{}]: "{}"'.format(
                  file_path, e))
    else:
      yaml.dump_all(documents=resources, stream=sys.stdout)

  def Serialize(self, resource, path=None):
    if path:
      if os.path.isdir(path):
        path = os.path.join(path,
                            '{}.yaml'.format(resource['metadata']['name']))
      try:
        with files.FileWriter(path) as stream:
          yaml.dump(resource, stream=stream)
      except (EnvironmentError, yaml.Error) as e:
        raise client_base.ClientException(
            'Error while serializing resource to file [{}]: "{}"'.format(
                path, e))
    else:
      yaml.dump(resource, stream=sys.stdout)

  def Get(self, **kwargs):
    """Excute execute command on underlying binary using resource_uri."""
    resource_uri = self._NormalizeURI(kwargs['resource_uri'])
    use_progress_tracker = kwargs.get('use_progress_tracker', False)

    if use_progress_tracker:
      with progress_tracker.ProgressTracker(
          'Getting resource configuration...', aborted_message='aborted'):
        return self._GetHelper(resource_uri)
    else:
      return self._GetHelper(resource_uri)

  def GetAll(self, **kwargs):
    resource_uris = [self._NormalizeURI(uri) for uri in kwargs['resource_uris']]
    use_progress_tracker = kwargs.get('use_progress_tracker', False)
    resource_documents = []
    if use_progress_tracker:
      with progress_tracker.ProgressTracker(
          'Getting resource configurations...', aborted_message='aborted'):
        for resource_uri in resource_uris:
          resource_documents.append(self._GetHelper(resource_uri))
    else:
      for resource_uri in resource_uris:
        resource_documents.append(self._GetHelper(resource_uri))
    return resource_documents

  def _GetHelper(self, resource_uri):
    cmd = [self._export_service, '--oauth2-token', self._GetToken()]
    cmd.extend(['export', resource_uri])
    output_value = io.StringIO()
    error_value = io.StringIO()

    exit_code = exec_utils.Exec(
        args=cmd,
        no_exit=True,
        out_func=output_value.write,
        err_func=error_value.write)
    if exit_code != 0:
      error = error_value.getvalue()
      if 'resource not found' in error:
        raise client_base.ResourceNotFoundException(
            'Could not fetch resource: \n - The resource [{}] does not exist.'
            .format(resource_uri))
      else:
        raise client_base.ClientException(
            'Error executing export:: [{}]'.format(error_value.getvalue()))
    resource = yaml.load(output_value.getvalue())
    return resource

  def Apply(self, **kwargs):
    raise NotImplementedError(
        'DeclarativeService Apply function must be implemented.')

  def Delete(self, **kwargs):
    raise NotImplementedError(
        'DeclarativeService Delete function must be implemented.')
