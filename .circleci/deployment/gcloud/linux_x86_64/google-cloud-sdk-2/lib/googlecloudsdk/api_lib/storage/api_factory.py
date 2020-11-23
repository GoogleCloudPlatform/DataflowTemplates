# -*- coding: utf-8 -*- #
# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Returns correct API client instance for user command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.storage import gcs_api
from googlecloudsdk.api_lib.storage import s3_api
from googlecloudsdk.command_lib.storage import storage_url


def get_api(provider):
  """Returns API instance for cloud provider.

  Args:
    provider (ProviderPrefix): Cloud provider prefix.

  Returns:
    CloudApi instance for specific cloud provider.

  Raises:
    ValueError: Invalid API provider.
  """
  # TODO(b/167685797): Use thread-local.
  if provider == storage_url.ProviderPrefix.GCS:
    return gcs_api.GcsApi()
  elif provider == storage_url.ProviderPrefix.S3:
    return s3_api.S3Api()
  raise ValueError('Provider API value must be "gs" or "s3".')
