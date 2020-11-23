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
"""Request hooks for On-Demand Scanning commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import properties

PARENT_TEMPLATE = 'projects/{}/locations/{}'


def FormatScanRequestParent(ref, args, req):
  """Request hook for the YAML scan command to supply the request with the correct parent value.

  Args:
    ref: Unused, this is None for the command using this request hook.
    args: Arguments passed into scan command.
    req: The scanContainerImage request message.

  Returns:
    request: The specified req with the parent correctly filled in.

  Raises:
    InvalidArgumentException: when resource_url does not begin with a valid GCR
    region.
  """
  del ref

  project = properties.VALUES.core.project.Get(required=True)

  resource_url = args.resource_url
  region = ''
  if (resource_url.startswith('https://gcr.io') or
      resource_url.startswith('gcr.io')):
    region = 'us'
  elif (resource_url.startswith('https://us.gcr.io') or
        resource_url.startswith('us.gcr.io')):
    region = 'us'
  elif (resource_url.startswith('https://eu.gcr.io') or
        resource_url.startswith('eu.gcr.io')):
    region = 'europe'
  elif (resource_url.startswith('https://asia.gcr.io') or
        resource_url.startswith('asia.gcr.io')):
    region = 'asia'
  else:
    raise exceptions.InvalidArgumentException(
        'resource_url',
        '"resource_url" must start with "[https://][us.|eu.|asia.]gcr.io"')

  req.parent = PARENT_TEMPLATE.format(project, region)

  return req
