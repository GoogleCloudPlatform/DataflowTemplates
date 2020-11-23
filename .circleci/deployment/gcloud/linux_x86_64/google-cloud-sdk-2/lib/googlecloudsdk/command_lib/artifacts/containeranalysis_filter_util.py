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
"""Utility for creating filters with containeranalysis API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

# The maximum number of resource URLs by which to filter when showing
# occurrences. This is required since filtering by too many causes the
# API request to be too large. Instead, the requests are chunkified.
_MAXIMUM_RESOURCE_URI_CHUNK_SIZE = 5


class ContainerAnalysisFilter:
  """Utility class for creating filters to send to containeranalysis API.

  If passed to a request, only occurrences that have the resource prefix, is of
  one of the kinds in self._kinds, is for one of the resources in self._resource
  and satisfies self._custom_filter will be retrieved.

  Properties:
    resource_prefix: str, the resource prefix filter added to this filter.
    custom_filter: str, the user provided filter added to this filter.
    kinds: list, metadata kinds added to this filter.
    resources: list, resource URLs added to this filter.
  """

  def __init__(self):
    self._resource_prefix = ''
    self._custom_filter = ''
    self._kinds = []
    self._resources = []

  @property
  def resource_prefix(self):
    return self._resource_prefix

  @property
  def custom_filter(self):
    return self._custom_filter

  @property
  def kinds(self):
    return self._kinds

  @property
  def resources(self):
    return self._resources

  def WithKinds(self, kinds):
    """Add metadata kinds to this filter."""
    self._kinds = [_HasField('kind', k) for k in kinds]
    return self

  def WithResources(self, resources):
    """Add resources to this filter."""
    self._resources = [_HasField('resourceUrl', r) for r in resources]
    return self

  def WithCustomFilter(self, custom_filter):
    """Add a custom filter to this filter."""
    self._custom_filter = custom_filter
    return self

  def WithResourcePrefix(self, resource_prefix):
    """Add a resource prefix to this filter."""
    self._resource_prefix = resource_prefix
    return self

  def GetFilter(self):
    """Returns a filter string with filtering attributes set."""

    return _AndJoinFilters(
        _HasPrefix('resourceUrl', self.resource_prefix), self.custom_filter,
        _OrJoinFilters(*self.kinds), _OrJoinFilters(*self.resources))

  def GetChunkifiedFilters(self):
    r"""Returns a list of filter strings where each filter has an upper limit of resource filters.

    The upper limit of resource filters in a contructed filter string is set
    by _MAXIMUM_RESOURCE_URI_CHUNK_SIZE. This is to avoid having too many
    filters in one API request and getting the request rejected.


    For example, consider this ContainerAnalysisFilter object:
      ContainerAnalysisFilter() \
        .WithKinds('VULNERABILITY') \
        .WithResources([
          'url/to/resources/1', 'url/to/resources/2', 'url/to/resources/3',
          'url/to/resources/4', 'url/to/resources/5', 'url/to/resources/6'])

    Calling GetChunkifiedFilters will return the following result:
    [
      '''(kind="VULNERABILITY") AND (resource_url="'url/to/resources/1)"
       OR ("resource_url="'url/to/resources/2")
       OR ("resource_url="'url/to/resources/3")
       OR ("resource_url="'url/to/resources/4")
       OR ("resource_url="'url/to/resources/5")''',
      '(kind="VULNERABILITY") AND (resource_url="url/to/resources/6")'
    ]
    """
    base_filter = _AndJoinFilters(
        _HasPrefix('resourceUrl', self.resource_prefix), self.custom_filter,
        _OrJoinFilters(*self.kinds))

    if not self.resources:
      return [base_filter]

    chunks = [
        self.resources[i:i + _MAXIMUM_RESOURCE_URI_CHUNK_SIZE]
        for i in range(0, len(self.resources), _MAXIMUM_RESOURCE_URI_CHUNK_SIZE)
    ]
    return [
        _AndJoinFilters(base_filter, _OrJoinFilters(*chunk)) for chunk in chunks
    ]


def _AndJoinFilters(*filters):
  return ' AND '.join(['({})'.format(f) for f in filters if f])


def _OrJoinFilters(*filters):
  return ' OR '.join(['({})'.format(f) for f in filters if f])


def _HasPrefix(field, prefix):
  return 'has_prefix({}, "{}")'.format(field, prefix) if prefix else None


def _HasField(field, value):
  return '{} = "{}"'.format(field, value) if value else None
