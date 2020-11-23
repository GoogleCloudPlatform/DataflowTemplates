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
"""Wait for an execution to complete."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.workflows import workflows
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.workflows import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class Wait(base.DescribeCommand):
  """Wait for an execution to complete."""

  @staticmethod
  def Args(parser):
    flags.AddExecutionResourceArg(parser, verb='to wait on')

  def Run(self, args):
    """Starts the wait on the completion of the execution."""
    api_version = workflows.ReleaseTrackToApiVersion(self.ReleaseTrack())
    execution_ref = flags.ParseExecution(args)
    client = workflows.WorkflowExecutionClient(api_version)
    return client.WaitForExecution(execution_ref)
