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
"""Utilities for the Eventarc service catalog."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import os

from googlecloudsdk.core import exceptions
from googlecloudsdk.core.util import encoding
from googlecloudsdk.core.util import files

_SERVICE_CATALOG_FILE_NAME = 'service_catalog.json'
_SERVICE_CATALOG_PATH = os.path.join(
    os.path.dirname(encoding.Decode(__file__)), _SERVICE_CATALOG_FILE_NAME)


class InvalidServiceName(exceptions.Error):
  """Error when a given serviceName is invalid."""


def GetServices():
  with files.FileReader(_SERVICE_CATALOG_PATH) as f:
    catalog = json.load(f)
    return catalog['services']


def GetMethods(service_name):
  for service in GetServices():
    if service['serviceName'] == service_name:
      return service['methods']
  raise InvalidServiceName(
      '"{}" is not a known value for the serviceName CloudEvents attribute.'
      .format(service_name))
