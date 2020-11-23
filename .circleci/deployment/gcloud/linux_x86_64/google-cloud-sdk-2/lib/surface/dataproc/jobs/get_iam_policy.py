# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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

"""Get IAM job policy command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.dataproc import dataproc as dp
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.dataproc import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class GetIamPolicy(base.ListCommand):
  """Get IAM policy for a job.

  Gets the IAM policy for a job, given a job ID.

  ## EXAMPLES

  The following command prints the IAM policy for a job with the ID
  `example-job`:

    $ {command} example-job
  """

  @classmethod
  def Args(cls, parser):
    dataproc = dp.Dataproc(cls.ReleaseTrack())
    flags.AddJobResourceArg(parser, 'retrieve the policy for',
                            dataproc.api_version)
    base.URI_FLAG.RemoveFromParser(parser)

  def Run(self, args):
    dataproc = dp.Dataproc(self.ReleaseTrack())
    msgs = dataproc.messages

    job = args.CONCEPTS.job.Parse()
    request = msgs.DataprocProjectsRegionsJobsGetIamPolicyRequest(
        resource=job.RelativeName())

    return dataproc.client.projects_regions_jobs.GetIamPolicy(request)
