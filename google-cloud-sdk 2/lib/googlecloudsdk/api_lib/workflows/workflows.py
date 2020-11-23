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
"""Utilities for Cloud Workflows API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as api_exceptions
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.api_lib.util import exceptions
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.api_lib.workflows import cache
from googlecloudsdk.api_lib.workflows import poller_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.command_lib.workflows import flags
from googlecloudsdk.core import resources


class UnsupportedReleaseTrackError(Exception):
  """Raised when requesting API version for an unsupported release track."""


def ReleaseTrackToApiVersion(release_track):
  if release_track == base.ReleaseTrack.ALPHA:
    return 'v1alpha1'
  elif release_track == base.ReleaseTrack.BETA:
    return 'v1beta'
  else:
    raise UnsupportedReleaseTrackError(release_track)


class WorkflowsClient(object):
  """Client for Workflows service in the Cloud Workflows API."""

  def __init__(self, api_version):
    self.client = apis.GetClientInstance('workflows', api_version)
    self.messages = self.client.MESSAGES_MODULE
    self._service = self.client.projects_locations_workflows

  def Get(self, workflow_ref):
    """Gets a Workflow.

    Args:
      workflow_ref: Resource reference to the Workflow to get.

    Returns:
      Workflow: The workflow if it exists, None otherwise.
    """
    get_req = self.messages.WorkflowsProjectsLocationsWorkflowsGetRequest(
        name=workflow_ref.RelativeName())
    try:
      return self._service.Get(get_req)
    except api_exceptions.HttpNotFoundError:
      return None

  def Create(self, workflow_ref, workflow):
    """Creates a Workflow.

    Args:
      workflow_ref: Resource reference to the Workflow to create.
      workflow: Workflow resource message to create.

    Returns:
      Long-running operation for create.
    """
    create_req = self.messages.WorkflowsProjectsLocationsWorkflowsCreateRequest(
        parent=workflow_ref.Parent().RelativeName(),
        workflow=workflow,
        workflowId=workflow_ref.Name())
    return self._service.Create(create_req)

  def Patch(self, workflow_ref, workflow, updated_fields):
    """Updates a Workflow.

    If updated fields are specified it uses patch semantics.

    Args:
      workflow_ref: Resource reference to the Workflow to update.
      workflow: Workflow resource message to update.
      updated_fields: List of the updated fields used in a patch request.

    Returns:
      Long-running operation for update.
    """
    update_mask = ','.join(sorted(updated_fields))
    patch_req = self.messages.WorkflowsProjectsLocationsWorkflowsPatchRequest(
        name=workflow_ref.RelativeName(),
        updateMask=update_mask,
        workflow=workflow)
    return self._service.Patch(patch_req)

  def BuildWorkflowFromArgs(self, args):
    """Create a workflow from command-line arguments."""
    workflow = self.messages.Workflow()
    updated_fields = []
    flags.SetSource(args, workflow, updated_fields)
    flags.SetDescription(args, workflow, updated_fields)
    flags.SetServiceAccount(args, workflow, updated_fields)
    labels = labels_util.ParseCreateArgs(args,
                                         self.messages.Workflow.LabelsValue)
    flags.SetLabels(labels, workflow, updated_fields)
    return workflow, updated_fields

  def WaitForOperation(self, operation, workflow_ref):
    """Waits until the given long-running operation is complete."""
    operation_ref = resources.REGISTRY.Parse(
        operation.name, collection='workflows.projects.locations.operations')
    operations = poller_utils.OperationsClient(self.client, self.messages)
    poller = poller_utils.WorkflowsOperationPoller(
        workflows=self, operations=operations, workflow_ref=workflow_ref)
    progress_string = 'Waiting for operation [{}] to complete'.format(
        operation_ref.Name())
    return waiter.WaitFor(poller, operation_ref, progress_string)


class WorkflowExecutionClient(object):
  """Client for Workflows Execution service in the Cloud Workflows Execution API."""

  def __init__(self, api_version):
    self.client = apis.GetClientInstance('workflowexecutions', api_version)
    self.messages = self.client.MESSAGES_MODULE
    self._service = self.client.projects_locations_workflows_executions

  def Create(self, workflow_ref, data):
    """Creates a Workflow execution.

    Args:
      workflow_ref: Resource reference to the Workflow to execute.
      data: Argments to use for executing the workflow.

    Returns:
      Execution: The workflow execution.
    """
    execution = self.messages.Execution()
    execution.argument = data
    create_req = self.messages.WorkflowexecutionsProjectsLocationsWorkflowsExecutionsCreateRequest(
        parent=workflow_ref.RelativeName(),
        execution=execution)
    try:
      return self._service.Create(create_req)
    except api_exceptions.HttpError as e:
      raise exceptions.HttpException(e, error_format='{message}')

  def Get(self, execution_ref):
    """Gets a workflow execution.

    Args:
      execution_ref: Resource reference to the Workflow execution to get.

    Returns:
      Workflow: The workflow execution if it exists, an error exception
      otherwise.
    """
    if execution_ref is None:
      execution_ref = cache.get_cached_execution_id()

    get_req = self.messages.WorkflowexecutionsProjectsLocationsWorkflowsExecutionsGetRequest(
        name=execution_ref.RelativeName())
    try:
      return self._service.Get(get_req)
    except api_exceptions.HttpError as e:
      raise exceptions.HttpException(e, error_format='{message}')

  def WaitForExecution(self, execution_ref):
    """Waits until the given execution is complete or the maximum wait time is reached."""

    if execution_ref is None:
      execution_ref = cache.get_cached_execution_id()

    poller = poller_utils.ExecutionsPoller(workflow_execution=self)
    progress_string = 'Waiting for execution [{}] to complete'.format(
        execution_ref.Name())
    try:
      return waiter.WaitFor(
          poller,
          execution_ref,
          progress_string,
          pre_start_sleep_ms=100,
          max_wait_ms=86400000,  # max wait time is 24 hours.
          exponential_sleep_multiplier=1.25,
          wait_ceiling_ms=60000)  # truncate sleep exponential at 1 minute.
    except waiter.TimeoutError:
      raise waiter.TimeoutError(
          'Execution {0} has not finished in 24 hours. {1}'.format(
              execution_ref, _TIMEOUT_MESSAGE))
    except waiter.AbortWaitError:
      raise waiter.AbortWaitError(
          'Aborting wait for execution {0}.'.format(execution_ref))


# Same message as the LRO time out error, modified with the word execution.
_TIMEOUT_MESSAGE = (
    'The execution may still be underway remotely and may still succeed; '
    'use gcloud list and describe commands or '
    'https://console.developers.google.com/ to check resource state.')
