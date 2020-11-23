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

"""File and Cloud URL representation classes."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import abc
import enum
import os

from googlecloudsdk.command_lib.storage import errors
from googlecloudsdk.core.util import platforms

import six


class ProviderPrefix(enum.Enum):
  """Provider prefix strings for storage URLs."""
  FILE = 'file'
  GCS = 'gs'
  S3 = 's3'


VALID_CLOUD_SCHEMES = frozenset([ProviderPrefix.GCS, ProviderPrefix.S3])
VALID_SCHEMES = frozenset([scheme.value for scheme in ProviderPrefix])


class StorageUrl(six.with_metaclass(abc.ABCMeta)):
  """Abstract base class for file and Cloud Storage URLs."""

  @abc.abstractproperty
  def delimiter(self):
    """Returns the delimiter for the url."""

  @abc.abstractproperty
  def url_string(self):
    """Returns the string representation of the instance."""

  @abc.abstractproperty
  def versionless_url_string(self):
    """Returns the string representation of the instance without the version."""

  def join(self, part):
    """Appends part at the end of url_string.

    The join is performed in 3 steps:
    1) Strip off one delimiter (if present) from the right of the url_string.
    2) Strip off one delimiter (if present) from the left of the part.
    3) Join the two strings with delimiter in between.

    Note that the behavior is slight different from os.path.join for cases
    where the part starts with a delimiter.
    os.path.join('a/b', '/c') => '/c'
    But this join method will return a StorageUrl with url_string as 'a/b/c'.
    This is done to be consistent across FileUrl and CloudUrl.

    The delimiter of the instance will be used. So, if you are trying to append
    a Windows path to a CloudUrl instance, you have to make sure to convert
    the Windows path before passing it to this method.

    Args:
      part (str): The part that needs to be appended.

    Returns:
      A StorageUrl instance.
    """
    left = rstrip_one_delimiter(self.url_string, self.delimiter)
    right = part[1:] if part.startswith(self.delimiter) else part
    new_url_string = '{}{}{}'.format(left, self.delimiter, right)
    return storage_url_from_string(new_url_string)

  def __eq__(self, other):
    if not isinstance(other, type(self)):
      return NotImplemented
    return self.url_string == other.url_string

  def __hash__(self):
    return hash(self.url_string)

  def __str__(self):
    return self.url_string


class FileUrl(StorageUrl):
  """File URL class providing parsing and convenience methods.

  This class assists with usage and manipulation of an
  (optionally wildcarded) file URL string.  Depending on the string
  contents, this class represents one or more directories or files.

  Attributes:
    scheme (ProviderPrefix): This will always be "file" for FileUrl.
    bucket_name (str): None for FileUrl.
    object_name (str): The file/directory path.
    generation (str): None for FileUrl.
  """

  def __init__(self, url_string):
    """Initialize FileUrl instance.

    Args:
      url_string (str): The string representing the filepath.
    """
    super(FileUrl, self).__init__()
    self.scheme = ProviderPrefix.FILE
    self.bucket_name = None
    self.generation = None
    if url_string.startswith('file://'):
      filename = url_string[len('file://'):]
    else:
      filename = url_string

    # On Windows, the pathname component separator is "\" instead of "/". If we
    # find an occurrence of "/", replace it with "\" so that other logic can
    # rely on being able to split pathname components on `os.sep`.
    if platforms.OperatingSystem.IsWindows():
      self.object_name = filename.replace('/', os.sep)
    else:
      self.object_name = filename

  @property
  def delimiter(self):
    """Returns the pathname separator character used by the OS."""
    return os.sep

  def exists(self):
    """Returns True if the file/directory exists."""
    return os.path.exists(self.object_name)

  def isdir(self):
    """Returns True if the path represents a directory."""
    return os.path.isdir(self.object_name)

  @property
  def url_string(self):
    """Returns the string representation of the instance."""
    return '%s://%s' % (self.scheme.value, self.object_name)

  @property
  def versionless_url_string(self):
    """Returns the string representation of the instance without the version."""
    return self.url_string


class CloudUrl(StorageUrl):
  """Cloud URL class providing parsing and convenience methods.

    This class assists with usage and manipulation of an
    (optionally wildcarded) cloud URL string.  Depending on the string
    contents, this class represents a provider, bucket(s), or object(s).

    This class operates only on strings.  No cloud storage API calls are
    made from this class.

    Attributes:
      scheme (ProviderPrefix): The cloud provider.
      bucket_name (str): The bucket name if url represents an object or bucket.
      object_name (str): The object name if url represents an object or prefix.
      generation (str): The generation number if present.
  """
  CLOUD_URL_DELIM = '/'

  def __init__(self, scheme, bucket_name=None, object_name=None,
               generation=None):
    super(CloudUrl, self).__init__()
    self.scheme = scheme if scheme else None
    self.bucket_name = bucket_name if bucket_name else None
    self.object_name = object_name if object_name else None
    self.generation = str(generation) if generation else None
    self._validate_scheme()
    self._validate_object_name()

  @classmethod
  def from_url_string(cls, url_string):
    """Parse the url string and return the storage url object.

    Args:
      url_string (str): Cloud storage url of the form gs://bucket/object

    Returns:
      CloudUrl object

    Raises:
      InvalidUrlError: Raised if the url_string is not a valid cloud url.
    """
    scheme = _get_scheme_from_url_string(url_string)

    # gs://a/b/c/d#num => a/b/c/d#num
    url_string = url_string[len(scheme.value + '://'):]

    # a/b/c/d#num => a, b/c/d#num
    bucket_name, _, object_name = url_string.partition(cls.CLOUD_URL_DELIM)

    # b/c/d#num => b/c/d, num
    object_name, _, generation = object_name.partition('#')

    return cls(scheme, bucket_name, object_name, generation)

  def _validate_scheme(self):
    if self.scheme not in VALID_CLOUD_SCHEMES:
      raise errors.InvalidUrlError('Unrecognized scheme "%s"' % self.scheme)

  def _validate_object_name(self):
    if self.object_name == '.' or self.object_name == '..':
      raise errors.InvalidUrlError('%s is an invalid root-level object name' %
                                   self.object_name)

  @property
  def url_string(self):
    url_str = self.versionless_url_string
    if self.generation:
      url_str += '#%s' % self.generation
    return url_str

  @property
  def versionless_url_string(self):
    if self.is_provider():
      return '%s://' % self.scheme.value
    elif self.is_bucket():
      return '%s://%s' % (self.scheme.value, self.bucket_name)
    return '%s://%s/%s' % (self.scheme.value, self.bucket_name,
                           self.object_name)

  @property
  def delimiter(self):
    return self.CLOUD_URL_DELIM

  def is_bucket(self):
    return bool(self.bucket_name and not self.object_name)

  def is_object(self):
    return bool(self.bucket_name and self.object_name)

  def is_provider(self):
    return bool(self.scheme and not self.bucket_name)


def _get_scheme_from_url_string(url_str):
  """Returns scheme component of a URL string."""
  end_scheme_idx = url_str.find('://')
  if end_scheme_idx == -1:
    # File is the default scheme.
    return ProviderPrefix.FILE
  else:
    prefix_string = url_str[0:end_scheme_idx].lower()
    if prefix_string not in VALID_SCHEMES:
      raise errors.InvalidUrlError('Unrecognized scheme "%s"' % prefix_string)
    return ProviderPrefix(prefix_string)


def storage_url_from_string(url_str):
  """Static factory function for creating a StorageUrl from a string.

  Args:
    url_str (str): Cloud url or local filepath.

  Returns:
     StorageUrl object.

  Raises:
    InvalidUrlError if url string is invalid.
  """
  scheme = _get_scheme_from_url_string(url_str)
  if scheme == ProviderPrefix.FILE:
    return FileUrl(url_str)
  return CloudUrl.from_url_string(url_str)


def rstrip_one_delimiter(string, delimiter=CloudUrl.CLOUD_URL_DELIM):
  """Strip one delimiter char from the end.

  Args:
    string (str): String on which the action needs to be performed.
    delimiter (str): A delimiter char.

  Returns:
    str: String with trailing delimiter removed.
  """
  if string.endswith(delimiter):
    return string[:-len(delimiter)]
  return string
