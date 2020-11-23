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

"""Utilities for expanding wildcarded GCS pathnames."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import abc
import collections
import fnmatch
import glob
import os
import re

from googlecloudsdk.api_lib.storage import api_factory
from googlecloudsdk.api_lib.storage import cloud_api
from googlecloudsdk.api_lib.storage import errors as api_errors
from googlecloudsdk.command_lib.storage import errors
from googlecloudsdk.command_lib.storage import storage_url
from googlecloudsdk.command_lib.storage.resources import resource_reference

import six


COMPRESS_WILDCARDS_REGEX = re.compile(r'\*{3,}')
WILDCARD_REGEX = re.compile(r'[*?\[\]]')


def contains_wildcard(url_string):
  """Checks whether url_string contains a wildcard.

  Args:
    url_string: URL string to check.

  Returns:
    bool indicator.
  """
  return bool(WILDCARD_REGEX.search(url_string))


def get_wildcard_iterator(
    url_str, all_versions=False, fields_scope=cloud_api.FieldsScope.NO_ACL):
  """Instantiate a WildcardIterator for the given URL string.

  Args:
    url_str (str): URL string which may contain wildcard characters.
    all_versions (bool): If true, the iterator yields all versions of objects
        matching the wildcard.  If false, yields just the live object version.
    fields_scope (cloud_api.FieldsScope): Determines amount of metadata
        returned by API.
  Returns:
    A WildcardIterator object.
  """
  url = storage_url.storage_url_from_string(url_str)
  if isinstance(url, storage_url.CloudUrl):
    return CloudWildcardIterator(url, all_versions, fields_scope)
  elif isinstance(url, storage_url.FileUrl):
    return FileWildcardIterator(url)
  else:
    raise errors.InvalidUrlError('Unknown url type %s.' % url)


def _compress_url_wildcards(url):
  """Asterisk counts greater than two treated as single * to mimic globs.

  Args:
    url (StorageUrl): Url to compress wildcards in.

  Returns:
    StorageUrl built from string with compressed wildcards.
  """
  compressed_url_string = re.sub(COMPRESS_WILDCARDS_REGEX, '*',
                                 url.versionless_url_string)
  if url.generation is not None:
    compressed_url_string += '#' + url.generation
  return storage_url.storage_url_from_string(compressed_url_string)


class WildcardIterator(six.with_metaclass(abc.ABCMeta)):
  """Class for iterating over Google Cloud Storage strings containing wildcards.

  The base class is abstract; you should instantiate using the
  wildcard_iterator() static factory method, which chooses the right
  implementation depending on the base string.
  """

  def __repr__(self):
    """Returns string representation of WildcardIterator."""
    return 'WildcardIterator(%s)' % self.wildcard_url.url_string


class FileWildcardIterator(WildcardIterator):
  """Class to iterate over files and directories."""

  def __init__(self, url):
    """Initialize FileWildcardIterator instance.

    Args:
      url (FileUrl): A FileUrl instance representing a file path.
    """
    super().__init__()
    url = _compress_url_wildcards(url)
    self._path = url.object_name

  def __iter__(self):
    recursion_needed = '**' in self._path
    for path in glob.iglob(self._path, recursive=recursion_needed):
      # For pattern like foo/bar/**, glob returns first path as 'foo/bar/'
      # even when foo/bar does not exist. So we skip non-existing paths.
      # Glob also returns intermediate directories if called with **. We skip
      # them to be consistent with CloudWildcardIterator.
      if self._path.endswith('**') and (not os.path.exists(path)
                                        or os.path.isdir(path)):
        continue

      file_url = storage_url.FileUrl(path)
      if os.path.isdir(path):
        yield resource_reference.FileDirectoryResource(file_url)
      else:
        yield resource_reference.FileObjectResource(file_url)


class CloudWildcardIterator(WildcardIterator):
  """Class to iterate over Cloud Storage strings containing wildcards."""

  def __init__(
      self, url, all_versions=False, fields_scope=cloud_api.FieldsScope.NO_ACL):
    """Instantiates an iterator that matches the wildcard URL.

    Args:
      url (CloudUrl): CloudUrl that may contain wildcard that needs expansion.
      all_versions (bool): If true, the iterator yields all versions of objects
          matching the wildcard.  If false, yields just the live object version.
      fields_scope (cloud_api.FieldsScope): Determines amount of metadata
          returned by API.
    """
    super(CloudWildcardIterator, self).__init__()
    url = _compress_url_wildcards(url)
    self._url = url
    self._all_versions = all_versions
    self._fields_scope = fields_scope
    self._client = api_factory.get_api(url.scheme)

    if url.url_string.endswith(url.delimiter):
      # Forces the API to return prefixes instead of their contents.
      url = storage_url.storage_url_from_string(
          storage_url.rstrip_one_delimiter(url.url_string))

  def __iter__(self):
    if self._url.is_provider():
      for bucket_resource in self._client.list_buckets(self._fields_scope):
        yield bucket_resource
    else:
      for bucket_resource in self._fetch_buckets():
        if self._url.is_bucket():
          yield bucket_resource
        else:  # URL is an object or prefix.
          for obj_resource in self._fetch_objects(
              bucket_resource.storage_url.bucket_name):
            yield obj_resource

  def _fetch_objects(self, bucket_name):
    """Fetch all objects for the given bucket that match the URL."""
    if not contains_wildcard(self._url.object_name) and not self._all_versions:
      try:
        # Assume that the url represents a single object.
        return [
            self._client.get_object_metadata(bucket_name, self._url.object_name,
                                             self._url.generation,
                                             self._fields_scope)
        ]
      except api_errors.NotFoundError:
        # Object does not exist. Could be a prefix.
        pass
    return self._expand_object_path(bucket_name)

  def _expand_object_path(self, bucket_name):
    """If wildcard, expand object names.

    Recursively expand each folder with wildcard.

    Args:
      bucket_name (str): Name of the bucket.

    Yields:
      resource_reference.Resource objects where each resource can be
      an ObjectResource object or a PrefixResource object.
    """
    # Retain original name to see if user wants only prefixes.
    original_object_name = self._url.object_name
    # Force API to return prefix resource not the prefix's contents.
    object_name = storage_url.rstrip_one_delimiter(original_object_name)

    names_needing_expansion = collections.deque([object_name])
    while names_needing_expansion:
      name = names_needing_expansion.popleft()

      # Parse out the prefix, delimiter, filter_pattern and suffix.
      # Given a string 'a/b*c/d/e*f/g.txt', this will return
      # CloudWildcardParts(prefix='a/b', filter_pattern='*c',
      #                    delimiter='/', suffix='d/e*f/g.txt')
      wildcard_parts = CloudWildcardParts.from_string(name, self._url.delimiter)

      # Fetch all the objects and prefixes.
      resource_iterator = self._client.list_objects(
          all_versions=self._all_versions or bool(self._url.generation),
          bucket_name=bucket_name,
          delimiter=wildcard_parts.delimiter,
          fields_scope=self._fields_scope,
          prefix=wildcard_parts.prefix or None)

      # We have all the objects and prefixes that matched the
      # wildcard_parts.prefix. Use the filter_pattern to eliminate non-matching
      # objects and prefixes.
      filtered_resources = self._filter_resources(
          resource_iterator,
          wildcard_parts.prefix + wildcard_parts.filter_pattern)

      for resource in filtered_resources:
        if wildcard_parts.suffix:
          if isinstance(resource, resource_reference.PrefixResource):
            # Suffix is present, which indicates that we have more wildcards to
            # expand. Let's say object_name is a/b1c. Then the new string that
            # we want to expand will be a/b1c/d/e*f/g.txt
            names_needing_expansion.append(
                resource.storage_url.object_name  + wildcard_parts.suffix)
        else:
          # Make sure an object is not returned if the original query was for
          # a prefix.
          if (not resource.storage_url.object_name.endswith(self._url.delimiter)
              and original_object_name.endswith(self._url.delimiter)):
            continue
          yield resource

  def _filter_resources(self, resource_iterator, wildcard_pattern):
    """Filter out resources that do not match the wildcard_pattern.

    Args:
      resource_iterator (iterable): An iterable resource_reference.Resource
        objects.
      wildcard_pattern (str): The wildcard_pattern to filter the resources.

    Yields:
      resource_reference.Resource objects matching the wildcard_pattern
    """
    regex_string = fnmatch.translate(wildcard_pattern)
    regex_pattern = re.compile(regex_string)
    for resource in resource_iterator:
      # A prefix resource returned by the API will always end with a slash.
      # We strip the slash in the end to match cases like gs://bucket/folder1
      object_name = storage_url.rstrip_one_delimiter(
          resource.storage_url.object_name)
      if (self._url.generation and
          resource.storage_url.generation != self._url.generation):
        # Filter based on generation, if generation is present in the request.
        continue
      if regex_pattern.match(object_name):
        yield resource

  def _fetch_buckets(self):
    """Fetch the bucket(s) corresponding to the url.

    Returns:
      An iterable of BucketResource objects.
    """
    if contains_wildcard(self._url.bucket_name):
      return self._expand_bucket_wildcards(self._url.bucket_name)
    elif self._url.is_bucket():
      return [
          self._client.get_bucket(self._url.bucket_name, self._fields_scope)
      ]
    else:
      return [resource_reference.BucketResource(self._url)]

  def _expand_bucket_wildcards(self, bucket_name):
    """Expand bucket names with wildcard.

    Args:
      bucket_name (str): Bucket name with wildcard.

    Yields:
      BucketResource objects.
    """
    regex = fnmatch.translate(bucket_name)
    bucket_pattern = re.compile(regex)
    for bucket_resource in self._client.list_buckets(self._fields_scope):
      if bucket_pattern.match(bucket_resource.name):
        yield bucket_resource


class CloudWildcardParts:
  """Different parts of the wildcard string used for querying and filtering."""

  def __init__(self, prefix, filter_pattern, delimiter, suffix):
    """Initialize the CloudWildcardParts object.

    Args:
      prefix (str): The prefix string to be passed to the API request.
        This is the substring before the first occurrance of the wildcard.
      filter_pattern (str): The pattern to be used to filter out the results
        returned by the list_objects call. This is a substring starting from
        the first occurance of the wildcard upto the first delimiter.
      delimiter (str): The delimiter to be passed to the api request.
      suffix (str): The substirng after the first delimiter in the wildcard.
    """
    self.prefix = prefix
    self.filter_pattern = filter_pattern
    self.delimiter = delimiter
    self.suffix = suffix

  @classmethod
  def from_string(cls, string, delimiter=storage_url.CloudUrl.CLOUD_URL_DELIM):
    """Create a CloudWildcardParts instance from a string.

    Args:
      string (str): String that needs to be splitted into different parts.
      delimiter (str): The delimiter to be used for splitting the string.

    Returns:
      WildcardParts object.
    """
    # Let's assume name => "a/b/c/d*e/f/g*.txt".
    # prefix => "a/b/c/d", wildcard_string => "*e/f/g*.txt".
    prefix, wildcard_string = _split_on_wildcard(string)
    # We expand one level at a time. Hence, spliting on delimiter.
    # filter_pattern => "*e", suffix = "f/g*.txt".
    filter_pattern, _, suffix = wildcard_string.partition(delimiter)

    if '**' in filter_pattern:
      # Fetch all objects for ** pattern. No delimiter is required since we
      # want to fetch all the objects here.
      delimiter = None
      filter_pattern = wildcard_string
      # Since we have fetched all the objects, suffix is no longer required.
      suffix = None

    return cls(prefix, filter_pattern, delimiter, suffix)


def _split_on_wildcard(string):
  """Split the string into two such that first part does not have any wildcard.

  Args:
    string (str): The string to be split.

  Returns:
    A 2-tuple where first part doesn't have any wildcard, and second part does
    have a wildcard. If wildcard is not found, the second part is empty.
    If string starts with a wildcard then first part is empty.
    For example:
      _split_on_wildcard('a/b/c/d*e/f/*.txt') => ('a/b/c/d', '*e/f/*.txt')
      _split_on_wildcard('*e/f/*.txt') => ('', '*e/f/*.txt')
      _split_on_wildcard('a/b/c/d') => ('a/b/c/d', '')
  """
  match = WILDCARD_REGEX.search(string)
  if match is None:
    return string, ''
  first_wildcard_idx = match.start()
  prefix = string[:first_wildcard_idx]
  wildcard_str = string[first_wildcard_idx:]
  return prefix, wildcard_str
