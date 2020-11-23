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

"""Task for daisy-chain copies.

Typically executed in a task iterator:
googlecloudsdk.command_lib.storage.tasks.task_executor.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import io

from googlecloudsdk.api_lib.storage import api_factory
from googlecloudsdk.command_lib.storage import storage_url
from googlecloudsdk.command_lib.storage.tasks import task


class DaisyChainCopyTask(task.Task):
  """Represents an operation to copy by downloading and uploading.

  Command operation which copies between two buckets by first downloading to the
  local machine, then uploading to the destination bucket. Can copy between
  different cloud providers or inside the same one.
  """

  def __init__(self, source_resource, destination_resource):
    """Initializes task.

    Args:
      source_resource (resource_reference.ObjectResource): Must
          contain the full object path of existing object.
          Directories will not be accepted.
      destination_resource (resource_reference.UnknownResource): Must
          contain the full object path. Object may not exist yet.
          Existing objects at the this location will be overwritten.
          Directories will not be accepted.
    """
    super().__init__()
    if (not isinstance(source_resource.storage_url, storage_url.CloudUrl)
        or not isinstance(destination_resource.storage_url,
                          storage_url.CloudUrl)):
      raise ValueError('DaisyChainCopyTask is for copies between cloud'
                       ' providers.')

    self._source_resource = source_resource
    self._destination_resource = destination_resource

  def execute(self, callback=None):
    """Copies file by downloading and uploading."""
    # TODO (b/168712813): Rewrite to use Data Transfer component.
    print('WARNING: The daisy-chaining copy holds all items being copied in'
          ' memory. This may crash your system if the size of your copy is'
          ' greater than available memory. Consider using Data Transfer:'
          ' https://cloud.google.com/products/data-transfer')

    provider = self._source_resource.storage_url.scheme
    client = api_factory.get_api(provider)

    # TODO(b/168489606): Buffer transfer to avoid holding entire download in
    # memory at once.
    with io.BytesIO() as daisy_chain_stream:
      client.download_object(self._source_resource, daisy_chain_stream)
      client.upload_object(daisy_chain_stream, self._destination_resource)
