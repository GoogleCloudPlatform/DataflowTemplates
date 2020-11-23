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

"""Delete job command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.dataproc import dataproc as dp
from googlecloudsdk.api_lib.dataproc import util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.dataproc import flags
from googlecloudsdk.core import log
from googlecloudsdk.core.console import console_io


class Delete(base.DeleteCommand):
  """Delete the record of an inactive job.

  Delete the record of an inactive job.

  ## EXAMPLES

  To delete the record of a job, run:

    $ {command} job_id
  """

  @classmethod
  def Args(cls, parser):
    dataproc = dp.Dataproc(cls.ReleaseTrack())
    flags.AddJobResourceArg(parser, 'delete', dataproc.api_version)

  def Run(self, args):
    dataproc = dp.Dataproc(self.ReleaseTrack())

    job_ref = args.CONCEPTS.job.Parse()
    request = dataproc.messages.DataprocProjectsRegionsJobsDeleteRequest(
        projectId=job_ref.projectId,
        region=job_ref.region,
        jobId=job_ref.jobId)

    console_io.PromptContinue(
        message="The job '{0}' will be deleted.".format(args.job),
        cancel_on_no=True,
        cancel_string='Deletion aborted by user.')

    dataproc.client.projects_regions_jobs.Delete(request)

    def _GetJob(job_ref):
      return dataproc.client.projects_regions_jobs.Get(
          dataproc.messages.DataprocProjectsRegionsJobsGetRequest(
              projectId=job_ref.projectId,
              region=job_ref.region,
              jobId=job_ref.jobId))

    util.WaitForResourceDeletion(
        _GetJob, job_ref, message='Waiting for job deletion')

    log.DeletedResource(job_ref)
