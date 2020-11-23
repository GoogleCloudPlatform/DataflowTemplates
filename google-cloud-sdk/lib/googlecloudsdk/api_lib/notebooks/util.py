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
"""A library that is used to support trace commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis as core_apis
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources


def GetClient():
  """Returns the client for the trace API."""
  return core_apis.GetClientInstance('notebooks', 'v1beta1')


def GetMessages():
  """Returns the messages for the trace API."""
  return core_apis.GetMessagesModule('notebooks', 'v1beta1')


def GetLocationResource(location, project=None):
  if not project:
    project = properties.VALUES.core.project.Get(required=True)
  return resources.REGISTRY.Parse(
      location,
      params={'projectsId': project},
      collection='notebooks.projects.locations')


def GetParentForInstance(args):
  if args.IsSpecified('instance'):
    instance = args.CONCEPTS.instance.Parse()
    return GetLocationResource(instance.locationsId,
                               instance.projectsId).RelativeName()


def GetParentForEnvironment(args):
  if args.IsSpecified('environment'):
    environment = args.CONCEPTS.environment.Parse()
    return GetLocationResource(environment.locationsId,
                               environment.projectsId).RelativeName()


def GetProjectResource(project):
  """Returns the resource for the current project."""
  return resources.REGISTRY.Parse(
      project or properties.VALUES.core.project.Get(required=True),
      collection='notebooks.projects')


def GetParentFromArgs(args):
  project = properties.VALUES.core.project.Get(required=True)
  if args.IsSpecified('location'):
    return GetLocationResource(args.location, project).RelativeName()
  elif properties.VALUES.notebooks.location.IsExplicitlySet():
    return GetLocationResource(
        properties.VALUES.notebooks.location.Get(required=True),
        project).RelativeName()


def GetOperationResource(name):
  return resources.REGISTRY.ParseRelativeName(
      name, collection='notebooks.projects.locations.operations')


def WaitForOperation(operation, message, service, is_delete=False):
  """Waits for the given google.longrunning.Operation to complete.

  Args:
    operation: The operation to poll.
    message: String to display for default progress_tracker.
    service: The service to get the resource after the long running operation
      completes.
    is_delete: Bool indicating is Poller should fetch resource post operation.

  Raises:
    apitools.base.py.HttpError: if the request returns an HTTP error

  Returns:
    The created Environment resource.
  """
  operation_ref = GetOperationResource(operation.name)
  if is_delete:
    poller = waiter.CloudOperationPollerNoResources(
        GetClient().projects_locations_operations)
  else:
    poller = waiter.CloudOperationPoller(
        service,
        GetClient().projects_locations_operations)
  return waiter.WaitFor(poller, operation_ref, message)
