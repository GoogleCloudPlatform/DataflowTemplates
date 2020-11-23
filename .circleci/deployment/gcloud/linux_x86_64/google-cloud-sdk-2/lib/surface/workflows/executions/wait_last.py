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
"""Wait for the last cached workflow execution to complete."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.workflows import workflows
from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class WaitLast(base.DescribeCommand):
  """Wait for the last cached workflow execution to complete."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
        To wait for the last cached workflow execution to complete, run:

          $ {command}
        """,
  }

  def Run(self, args):
    """Starts the wait on the completion of the execution."""
    api_version = workflows.ReleaseTrackToApiVersion(self.ReleaseTrack())
    client = workflows.WorkflowExecutionClient(api_version)
    return client.WaitForExecution(None)
