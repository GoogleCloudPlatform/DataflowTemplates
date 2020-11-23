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
"""Command to check stream logs of a custom job in AI Platform."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.ai.custom_jobs import client
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import endpoint_util
from googlecloudsdk.command_lib.ai import flags
from googlecloudsdk.command_lib.ai import log_util


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class StreamLogs(base.Command):
  """Show stream logs from a running AI Platform custom job.

     ## EXAMPLES

     To stream logs of a custom job, run:

     $ {command} stream-logs CUSTOM_JOB
  """

  @staticmethod
  def Args(parser):
    flags.AddCustomJobResourceArg(parser, 'to fetch stream log')
    flags.AddStreamLogsFlags(parser)
    parser.display_info.AddFormat(log_util.LOG_FORMAT)

  def Run(self, args):
    custom_job_ref = args.CONCEPTS.custom_job.Parse()
    region = custom_job_ref.AsDict()['locationsId']
    with endpoint_util.AiplatformEndpointOverrides(
        version=constants.BETA_VERSION, region=region):
      relative_name = custom_job_ref.RelativeName()
      return log_util.StreamLogs(
          custom_job_ref.AsDict()['customJobsId'],
          continue_function=client.CustomJobsClient(
              version=constants.BETA_VERSION).CheckJobComplete(relative_name),
          polling_interval=args.polling_interval,
          task_name=args.task_name,
          allow_multiline=args.allow_multiline_logs)
