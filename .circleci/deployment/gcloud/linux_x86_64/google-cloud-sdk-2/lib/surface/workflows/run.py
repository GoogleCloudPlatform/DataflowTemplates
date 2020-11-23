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
"""Execute a workflow and wait for the execution to complete."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.workflows import cache
from googlecloudsdk.api_lib.workflows import workflows
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.workflows import flags
from googlecloudsdk.core import resources

EXECUTION_COLLECTION = 'workflowexecutions.projects.locations.workflows.executions'


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class Run(base.DescribeCommand):
  """Execute a workflow and wait for the execution to complete."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
        To execute a workflow named my-workflow with the data that will be passed to the workflow, run:

          $ {command} my-workflow --data=my-data
        """,
  }

  @staticmethod
  def Args(parser):
    flags.AddWorkflowResourceArg(parser, verb='to execute')
    flags.AddDataArg(parser)

  def Run(self, args):
    """Execute a workflow and wait for the completion of the execution."""
    api_version = workflows.ReleaseTrackToApiVersion(self.ReleaseTrack())
    workflow_ref = flags.ParseWorkflow(args)
    client = workflows.WorkflowExecutionClient(api_version)
    execution = client.Create(workflow_ref, args.data)
    cache.cache_execution_id(execution.name)
    execution_ref = resources.REGISTRY.Parse(
        execution.name, collection=EXECUTION_COLLECTION)
    return client.WaitForExecution(execution_ref)
