# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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

"""Utilities for storage commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import enum
import hashlib

from googlecloudsdk.command_lib.storage import errors
from googlecloudsdk.core import properties
from googlecloudsdk.core.updater import installers


class HashAlgorithms(enum.Enum):
  """Algorithms available for hashing data."""

  MD5 = 'md5'
  # TODO(b/172048376): Add crc32c.


# pylint:disable=invalid-name
def SetBucket(resource_ref, namespace, request):
  """Helper used in a declarative hook to set the bucket in a storage request.

  This is needed because the buckets resource is not rooted under a project,
  but a project is required when creating a bucket or listing buckets.

  Args:
    resource_ref: The parsed bucket resource
    namespace: unused
    request: The request the declarative framework has generated.

  Returns:
    The request to issue.
  """
  del namespace
  request.project = properties.VALUES.core.project.Get(required=True)
  request.bucket.name = resource_ref.bucket
  return request
# pylint:enable=invalid-name


def get_md5_hash(byte_string=b''):
  """Returns md5 hash, avoiding incorrect FIPS error on Red Hat systems.

  Examples: get_md5_hash(b'abc')
            get_md5_hash(bytes('abc', encoding='utf-8'))

  Args:
    byte_string (bytes): String in bytes form to hash. Don't include for empty
      hash object, since md5(b'').digest() == md5().digest().

  Returns:
    md5 hash object.
  """
  try:
    return hashlib.md5(byte_string)
  except ValueError:
    # On Red Hat-based platforms, may catch a FIPS error.
    # "usedforsecurity" flag only available on Red Hat systems or Python 3.9+.
    # pylint:disable=unexpected-keyword-arg
    return hashlib.md5(byte_string, usedforsecurity=False)
    # pylint:enable=unexpected-keyword-arg


def get_hash_digest_from_file_stream(file_stream, hash_algorithm):
  """Reads file and returns its base64-encoded hash digest.

  core.util.files.Checksum does similar things but is different enough to merit
  this function. The primary differences are that this function:
  -Base64 encodes a normal digest instead of returning a raw hex digest.
  -Uses a FIPS-safe MD5 object.
  -Resets stream after consuming.

  Args:
    file_stream (stream): File to read.
    hash_algorithm (HashAlgorithm): Algorithm to hash file with.

  Returns:
    String of base64-encoded hash digest for file.
  """
  if hash_algorithm == HashAlgorithms.MD5:
    hash_object = get_md5_hash()
  else:
    # TODO(b/172048376): Add crc32c.
    return

  while True:
    # Avoids holding all of file in memory at once.
    data = file_stream.read(installers.WRITE_BUFFER_SIZE)
    if not data:
      break
    if isinstance(data, str):
      # read() can return strings or bytes. Hash objects need bytes.
      data = data.encode('utf-8')
    # Compresses each piece of added data.
    hash_object.update(data)

  # Hashing the file consumes the stream, so reset to avoid giving the
  # caller of this function any confusing bugs.
  file_stream.seek(0)

  # GCS and S3 base64 encode digests and return strings from the API.
  return base64.b64encode(hash_object.digest()).decode(encoding='utf-8')


def validate_object_hashes_match(object_url, source_hash, destination_hash):
  """Confirms hashes match for copied objects.

  Args:
    object_url (str): URL of object being validated.
    source_hash (str): Hash of source object.
    destination_hash (str): Hash of destination object.

  Raises:
    HashMismatchError: Hashes are not equal.
  """
  if source_hash != destination_hash:
    raise errors.HashMismatchError(
        'Source hash {} does not match destination hash {}'
        ' for object {}.'.format(source_hash, destination_hash, object_url))
  # TODO(b/172048376): Check crc32c if md5 not available.
