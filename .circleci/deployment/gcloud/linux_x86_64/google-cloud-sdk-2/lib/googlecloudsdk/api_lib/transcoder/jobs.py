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
"""Utilities for Transcoder API Jobs."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from apitools.base.py import list_pager
from googlecloudsdk.api_lib.util import apis


def _GetClientInstance():
  return apis.GetClientInstance('transcoder', 'v1beta1')


class JobsClient(object):
  """Client for job service in the Transcoder API."""

  def __init__(self, client=None):
    self.client = client or _GetClientInstance()
    self.message = self.client.MESSAGES_MODULE
    self._service = self.client.projects_locations_jobs
    self._job_class = self.client.MESSAGES_MODULE.Job

  def Create(self, parent_ref, job_json=None, template_id=None, input_uri=None,
             output_uri=None, priority=None):
    """Create a job.

    Args:
      parent_ref: a Resource reference to a transcoder.projects.locations
        resource for the parent of this template.
      job_json: job details in json format
      template_id: template id associated. Default: preset/web-hd template
      input_uri: input video location in Google Cloud Storage
      output_uri: output directory (followed by a trailing forward slash) in
        Google Cloud Storage
      priority: job priority. Optional

    Returns:
      Job: Job created, including configuration and name.
    """

    if job_json is None:
      job = self.message.Job(inputUri=input_uri, outputUri=output_uri,
                             templateId=template_id, priority=priority)
    else:
      job = encoding.JsonToMessage(self._job_class, job_json)
      job.inputUri = input_uri
      job.outputUri = output_uri
      job.priority = job.priority if priority is None else priority

    req = self.message.TranscoderProjectsLocationsJobsCreateRequest(
        parent=parent_ref.RelativeName(), job=job)
    return self._service.Create(req)

  def Delete(self, job_ref):
    """Delete a job.

    Args:
      job_ref: a resource reference to a
        transcoder.projects.locations.jobs resource to delete

    Returns:
      Empty: An empty response message.
    """
    req = self.message.TranscoderProjectsLocationsJobsDeleteRequest(
        name=job_ref.RelativeName())
    return self._service.Delete(req)

  def Get(self, job_ref):
    """Get a job.

    Args:
      job_ref: a resource reference to a
        transcoder.projects.locations.jobs resource to get

    Returns:
      Job: if available, return the full job information.
    """
    req = self.message.TranscoderProjectsLocationsJobsGetRequest(
        name=job_ref.RelativeName())
    return self._service.Get(req)

  def List(self, parent_ref, page_size=100):
    """List jobs.

    Args:
      parent_ref: a Resource reference to a transcoder.projects.locations
        resource to list job for.
      page_size (optional): the number of jobs to fetch in each request (affects
        requests made, but not the yielded results).

    Returns:
      Jobs: a list of jobs in the specified location
    """
    req = self.message.TranscoderProjectsLocationsJobsListRequest(
        parent=parent_ref.RelativeName(), pageSize=page_size)
    resp = list_pager.YieldFromList(
        service=self._service,
        request=req,
        batch_size=page_size,
        field='jobs',
        batch_size_attribute='pageSize')
    return resp
