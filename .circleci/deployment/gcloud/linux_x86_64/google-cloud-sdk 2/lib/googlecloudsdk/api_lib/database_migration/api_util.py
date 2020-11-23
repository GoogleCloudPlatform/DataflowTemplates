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
"""Cloud Database Migration API utilities."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import uuid

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.core import resources
import six

_DEFAULT_API_VERSION = 'v1alpha2'


def GetClientInstance(api_version=_DEFAULT_API_VERSION, no_http=False):
  return apis.GetClientInstance('datamigration', api_version, no_http=no_http)


def GetMessagesModule(api_version=_DEFAULT_API_VERSION):
  return apis.GetMessagesModule('datamigration', api_version)


def GetResourceParser(api_version=_DEFAULT_API_VERSION):
  resource_parser = resources.Registry()
  resource_parser.RegisterApiByName('datamigration', api_version)
  return resource_parser


def ParentRef(project, location):
  """Get the resource name of the parent collection.

  Args:
    project: the project of the parent collection.
    location: the GCP region of the membership.

  Returns:
    the resource name of the parent collection in the format of
    `projects/{project}/locations/{location}`.
  """

  return 'projects/{}/locations/{}'.format(project, location)


def GenerateRequestId():
  """Generates a UUID to use as the request ID.

  Returns:
    string, the 40-character UUID for the request ID.
  """
  return six.text_type(uuid.uuid4())
