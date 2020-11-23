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
"""Utilities for querying custom jobs in AI Platform."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.api_lib.util import messages as messages_util
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import validation
from googlecloudsdk.core import yaml


class CustomJobsClient(object):
  """Client used for interacting with CustomJob endpoint."""

  def __init__(self, client=None, messages=None, version=None):
    self.client = client or apis.GetClientInstance(
        constants.AI_PLATFORM_API_NAME,
        constants.AI_PLATFORM_API_VERSION[version])
    self.messages = messages or self.client.MESSAGES_MODULE
    self._service = self.client.projects_locations_customJobs

  def GetShortMessage(self, short_message_name):
    return getattr(
        self.messages, '{prefix}{name}'.format(
            prefix=self._short_message_prefix, name=short_message_name), None)

  def Create(self,
             parent,
             specs=None,
             config_path=None,
             display_name=None,
             python_package_uri=None):
    """Constructs a request and sends it to the endpoint to create a custom job instance."""
    if not python_package_uri:
      python_package_uri = []

    job_spec = self.messages.GoogleCloudAiplatformV1beta1CustomJobSpec()
    if config_path:
      data = yaml.load_path(config_path)
      if data:
        job_spec = messages_util.DictToMessageWithErrorCheck(
            data, self.messages.GoogleCloudAiplatformV1beta1CustomJobSpec)

    worker_pool_specs = []
    if specs is not None:
      for spec in specs:
        machine_type = spec.get('machine-type')
        if not spec.get('replica-count'):
          replica_count = 1
        else:
          replica_count = int(spec.get('replica-count'))
        container_image_uri = spec.get('container-image-uri')
        python_image_uri = spec.get('python-image-uri')
        python_module = spec.get('python-module')
        machine_spec = (
            self.messages.GoogleCloudAiplatformV1beta1MachineSpec(
                machineType=machine_type))

        worker_pool_spec = (
            self.messages.GoogleCloudAiplatformV1beta1WorkerPoolSpec(
                replicaCount=replica_count, machineSpec=machine_spec))
        if container_image_uri:
          worker_pool_spec.containerSpec = (
              self.messages.GoogleCloudAiplatformV1beta1ContainerSpec(
                  imageUri=container_image_uri))

        # TODO(b/161753810): Pass args and commands to the python package
        # and container.
        if python_package_uri or python_image_uri or python_module:
          worker_pool_spec.pythonPackageSpec = (
              self.messages.GoogleCloudAiplatformV1beta1PythonPackageSpec(
                  executorImageUri=python_image_uri,
                  packageUris=python_package_uri,
                  pythonModule=python_module))
        worker_pool_specs.append(worker_pool_spec)

    if worker_pool_specs:
      job_spec.workerPoolSpecs = worker_pool_specs
    validation.ValidateWorkerPoolSpec(job_spec.workerPoolSpecs)

    custom_job = (
        self.messages.GoogleCloudAiplatformV1beta1CustomJob(
            displayName=display_name, jobSpec=job_spec))

    return self._service.Create(
        self.messages.AiplatformProjectsLocationsCustomJobsCreateRequest(
            parent=parent, googleCloudAiplatformV1beta1CustomJob=custom_job))

  def List(self, limit=None, region=None):
    return list_pager.YieldFromList(
        self._service,
        self.messages.AiplatformProjectsLocationsCustomJobsListRequest(
            parent=region),
        field='customJobs',
        batch_size_attribute='pageSize',
        limit=limit)

  def Get(self, name):
    request = self.messages.AiplatformProjectsLocationsCustomJobsGetRequest(
        name=name)
    return self._service.Get(request)

  def Cancel(self, name):
    request = self.messages.AiplatformProjectsLocationsCustomJobsCancelRequest(
        name=name)
    return self._service.Cancel(request)

  def CheckJobComplete(self, name):
    """Returns a function to decide if log fetcher should continue polling.

    Args:
      name: String id of job.

    Returns:
      A one-argument function decides if log fetcher should continue.
    """
    request = self.messages.AiplatformProjectsLocationsCustomJobsGetRequest(
        name=name)
    response = self._service.Get(request)

    def ShouldContinue(periods_without_logs):
      if periods_without_logs <= 1:
        return True
      return response.endTime is None

    return ShouldContinue
