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

"""Preferred method of generating a copy task."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.storage import storage_url
from googlecloudsdk.command_lib.storage.tasks.cp import daisy_chain_copy_task
from googlecloudsdk.command_lib.storage.tasks.cp import file_download_task
from googlecloudsdk.command_lib.storage.tasks.cp import file_upload_task
from googlecloudsdk.command_lib.storage.tasks.cp import intra_cloud_copy_task


def get_copy_task(source_resource, destination_resource):
  """Factory method that returns the correct copy task for the arguments.

  Args:
    source_resource (resource_reference.Resource): Reference to file to copy.
    destination_resource (resource_reference.Resource): Reference to
        destination to copy file to.

  Returns:
    Task object that can be executed to perform a copy.

  Raises:
    NotImplementedError: Cross-cloud copy.
    ValueError: Local filesystem copy.
  """
  source_url = source_resource.storage_url
  destination_url = destination_resource.storage_url

  if (isinstance(source_url, storage_url.FileUrl)
      and isinstance(destination_url, storage_url.FileUrl)):
    raise ValueError('Local copies not supported. Gcloud command-line tool is'
                     ' meant for cloud operations.')

  if (isinstance(source_url, storage_url.CloudUrl)
      and isinstance(destination_url, storage_url.FileUrl)):
    return file_download_task.FileDownloadTask(source_resource,
                                               destination_resource)

  if (isinstance(source_url, storage_url.FileUrl)
      and isinstance(destination_url, storage_url.CloudUrl)):
    return file_upload_task.FileUploadTask(source_resource,
                                           destination_resource)

  if (isinstance(source_url, storage_url.CloudUrl)
      and isinstance(destination_url, storage_url.CloudUrl)):
    if source_url.scheme != destination_url.scheme:
      return daisy_chain_copy_task.DaisyChainCopyTask(source_resource,
                                                      destination_resource)
    return intra_cloud_copy_task.IntraCloudCopyTask(source_resource,
                                                    destination_resource)
