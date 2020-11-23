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
"""Module for handling recursive expansion."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.storage import errors
from googlecloudsdk.command_lib.storage import plurality_checkable_iterator
from googlecloudsdk.command_lib.storage import wildcard_iterator


class NameExpansionIterator:
  """Expand all urls passed as arguments, and yield NameExpansionResult.

  For each url, expands wildcards, object-less bucket names,
  subdir bucket names, and directory names, and generates a flat listing of
  all the matching objects/files.
  The resulting objects/files are wrapped within a NameExpansionResult instance.
  See NameExpansionResult docstring for more info.
  """

  def __init__(self, urls, recursion_requested=False):
    """Instantiates NameExpansionIterator.

    Args:
      urls (Iterable[str]): The URLs to expand.
      recursion_requested (bool): True if recursion is requested, else False.
    """
    self._urls = urls
    self._recursion_requested = recursion_requested

  def __iter__(self):
    """Iterates over each URL in self._urls and yield the expanded result.

    Yields:
      NameExpansionResult instance.
    """
    for url in self._urls:
      resources = plurality_checkable_iterator.PluralityCheckableIterator(
          wildcard_iterator.get_wildcard_iterator(url))
      if resources.is_empty():
        raise errors.InvalidUrlError('{} matched no objects.'.format(url))

      # Iterate over all the resource_reference.Resource objects.
      for resource in resources:
        if self._recursion_requested and resource.is_container():
          # Append '**' to fetch all objects under this container
          new_storage_url = resource.storage_url.join('**')
          child_resources = wildcard_iterator.get_wildcard_iterator(
              new_storage_url.url_string)
          for child_resource in child_resources:
            yield NameExpansionResult(child_resource, resource.storage_url)
        else:
          yield NameExpansionResult(resource, resource.storage_url)


class NameExpansionResult:
  """Holds one fully expanded result from iterating over NameExpansionIterator.

  This class is required to pass the expanded_url information to the caller.
  This information is required for cp and rsync command, where the destination
  name is determined based on the expanded source url.
  For example, let's say we have the following objects:
  gs://bucket/dir1/a.txt
  gs://bucket/dir1/b/c.txt

  If we run the following command:
  cp -r gs://bucket/dir* foo

  We would need to know that gs://bucket/dir* was expanded to gs://bucket/dir1
  so that we can determine destination paths (foo/a.txt, foo/b/c.txt) assuming
  that foo does not exist.

  Attributes:
    resource (Resource): Yielded by the WildcardIterator.
    expanded_url (StorageUrl): The expanded wildcard url.
  """

  def __init__(self, resource, expanded_url):
    """Initialize NameExpansionResult.

    Args:
      resource (resource_reference.Resource): Yielded by the WildcardIterator.
      expanded_url (StorageUrl): The expanded url string without any wildcard.
          This should be same as the resource.storage_url if recursion was not
          requested. This field is only used for cp and rsync commands.
          For everything else, this field can be ignored.
    """
    self.resource = resource
    self.expanded_url = expanded_url

  def __str__(self):
    return self.resource.storage_url.url_string

  def __eq__(self, other):
    if not isinstance(other, type(self)):
      return NotImplemented
    return (self.resource == other.resource
            and self.expanded_url == other.expanded_url)
