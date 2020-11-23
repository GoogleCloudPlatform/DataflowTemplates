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
"""A library that is used to support Cloud Pub/Sub Lite commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import properties

import six
from six.moves.urllib.parse import urlparse

# Resource path constants
PROJECTS_RESOURCE_PATH = 'projects/'
LOCATIONS_RESOUCE_PATH = 'locations/'
TOPICS_RESOURCE_PATH = 'topics/'


class UnexpectedResourceField(exceptions.Error):
  """Error for having and unknown resource field."""


def DurationToSeconds(duration):
  """Convert Duration object to total seconds for backend compatibility."""
  return six.text_type(int(duration.total_seconds)) + 's'


def StripPathFromUrl(url):
  """Returns the url stripped of the path.

  (e.g.

  Args:
    url: A str of the form `https://base.url.com/v1/foobar/path`.

  Returns:
    The url with the path removed. Example: `https://base.url.com/`.
  """
  parsed = urlparse(url)
  return parsed.scheme + '://' + parsed.netloc + '/'


def DeriveRegionFromZone(zone):
  """Returns the region from a zone string.

  Args:
    zone: A str of the form `<region>-<zone>`. Example: `us-central1-a`. Any
      other form will cause undefined behavior.

  Returns:
    The str region. Example: `us-central1`.
  """
  return zone[:zone.rindex('-')]


def DeriveRegionFromEndpoint(endpoint):
  """Returns the region from a endpoint string.

  Args:
    endpoint: A str of the form `https://<region-><environment->base.url.com/`.
      Example `https://us-central-base.url.com/`,
      `https://us-central-autopush-base.url.com/`, or `https://base.url.com/`.

  Returns:
    The str region if it exists, otherwise None.
  """
  parsed = urlparse(endpoint)
  dash_splits = parsed.netloc.split('-')
  if len(dash_splits) > 2:
    return dash_splits[0] + '-' + dash_splits[1]
  else:
    return None


def CreateRegionalEndpoint(region, url):
  """Returns a new endpoint string with the defined `region` prefixed to the netlocation."""
  url_parts = url.split('://')
  url_scheme = url_parts[0]
  return url_scheme + '://' + region + '-' + url_parts[1]


def RemoveRegionFromEndpoint(endpoint):
  """Returns a new endpoint string stripped of the region if one exists."""
  region = DeriveRegionFromEndpoint(endpoint)
  if region:
    endpoint = endpoint.replace(region + '-', '')
  return endpoint


def GetResourceInfo(request):
  """Returns a tuple of the resource and resource name from the `request`.

  Args:
    request: A Request object instance.

  Returns:
    A tuple of the resource string path and the resource name.

  Raises:
    UnexpectedResourceField: The `request` had a unsupported resource.
  """
  resource = ''
  resource_name = ''

  if hasattr(request, 'parent'):
    resource = request.parent
    resource_name = 'parent'
  elif hasattr(request, 'name'):
    resource = request.name
    resource_name = 'name'
  else:
    raise UnexpectedResourceField(
        'The resource specified for this command is unknown!')

  return resource, resource_name


def DeriveZoneFromResource(resource):
  """Returns the zone from a resource string."""
  zone = resource[resource.index(LOCATIONS_RESOUCE_PATH) +
                  len(LOCATIONS_RESOUCE_PATH):]
  zone = zone.split('/')[0]
  return zone


def DeriveProjectFromResource(resource):
  """Returns the project from a resource string."""
  project = resource[resource.index(PROJECTS_RESOURCE_PATH) +
                     len(PROJECTS_RESOURCE_PATH):]
  project = project.split('/')[0]
  return project


def ParseResource(request):
  """Returns an updated `request` with the resource path parsed."""
  resource, resource_name = GetResourceInfo(request)
  new_resource = resource[resource.rindex(PROJECTS_RESOURCE_PATH):]
  setattr(request, resource_name, new_resource)

  return request


def OverrideEndpointWithRegion(request, url):
  """Sets the pubsublite endpoint override to include the region."""
  resource, _ = GetResourceInfo(request)
  region = DeriveRegionFromZone(DeriveZoneFromResource(resource))

  endpoint = StripPathFromUrl(url)

  # Remove any region from the endpoint in case it was previously set.
  # Specifically this effects scenario tests where multiple tests are run in a
  # single instance.
  endpoint = RemoveRegionFromEndpoint(endpoint)

  regional_endpoint = CreateRegionalEndpoint(region, endpoint)
  properties.VALUES.api_endpoint_overrides.pubsublite.Set(regional_endpoint)


def ProjectIdToProjectNumber(project_id):
  """Returns the Cloud project number associated with the `project_id`."""
  crm_message_module = apis.GetMessagesModule('cloudresourcemanager', 'v1')
  resource_manager = apis.GetClientInstance('cloudresourcemanager', 'v1')

  req = crm_message_module.CloudresourcemanagerProjectsGetRequest(
      projectId=project_id)
  project = resource_manager.projects.Get(req)

  return project.projectNumber


def OverrideProjectIdToProjectNumber(request):
  """Returns an updated `request` with the Cloud project number."""
  resource, resource_name = GetResourceInfo(request)
  project_id = DeriveProjectFromResource(resource)
  project_number = ProjectIdToProjectNumber(project_id)
  setattr(request, resource_name,
          resource.replace(project_id, six.text_type(project_number)))

  return request


def UpdateAdminRequest(resource_ref, args, request):
  """Returns an updated `request` with values for a valid Admin request."""
  # Unused resource reference.
  del args

  request = ParseResource(request)
  request = OverrideProjectIdToProjectNumber(request)
  OverrideEndpointWithRegion(request, resource_ref.SelfLink())

  return request


def AddSubscriptionTopicResource(resource_ref, args, request):
  """Returns an updated `request` with a resource path on the topic."""
  # Unused resource reference and arguments.
  del resource_ref, args

  resource, _ = GetResourceInfo(request)
  request.subscription.topic = '{}/{}{}'.format(resource, TOPICS_RESOURCE_PATH,
                                                request.subscription.topic)

  return request
