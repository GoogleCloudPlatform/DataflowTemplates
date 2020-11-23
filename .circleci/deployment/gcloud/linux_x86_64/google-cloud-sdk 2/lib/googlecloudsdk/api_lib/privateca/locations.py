# Lint as: python3
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
"""Helpers for dealing with Private CA locations."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions
from googlecloudsdk.api_lib.privateca import base
from googlecloudsdk.core import log
from googlecloudsdk.core import properties


# Hard-coded locations to use if the call to ListLocations fails.
_FallbackLocations = [
    'asia-southeast1',
    'europe-west1',
    'europe-west4',
    'us-central1',
    'us-east1',
    'us-west1',
]


def GetSupportedLocations():
  """Gets a list of supported Private CA locations for the current project."""
  client = base.GetClientInstance()
  messages = base.GetMessagesModule()

  project = properties.VALUES.core.project.GetOrFail()

  try:
    response = client.projects_locations.List(
        messages.PrivatecaProjectsLocationsListRequest(
            name='projects/{}'.format(project)))
    return [location.locationId for location in response.locations]
  except exceptions.HttpError as e:
    log.debug('ListLocations failed: %r.', e)
    log.debug('Falling back to hard-coded list.')
    return _FallbackLocations
