# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Common utilities and shared helpers for secrets."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import yaml
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.util import files

DEFAULT_MAX_BYTES = 65536


def ReadFileOrStdin(path, max_bytes=None, is_binary=True):
  """Read data from the given file path or from stdin.

  This is similar to the cloudsdk built in ReadFromFileOrStdin, except that it
  limits the total size of the file and it returns None if given a None path.
  This makes the API in command surfaces a bit cleaner.

  Args:
      path (str): path to the file on disk or "-" for stdin
      max_bytes (int): maximum number of bytes
      is_binary (bool): if true, data will be read as binary

  Returns:
      result (str): result of reading the file
  """
  if not path:
    return None

  max_bytes = max_bytes or DEFAULT_MAX_BYTES

  try:
    data = console_io.ReadFromFileOrStdin(path, binary=is_binary)
    if len(data) > max_bytes:
      raise exceptions.BadFileException(
          'The file [{path}] is larger than the maximum size of {max_bytes} '
          'bytes.'.format(path=path, max_bytes=max_bytes))
    return data
  except files.Error as e:
    raise exceptions.BadFileException(
        'Failed to read file [{path}]: {e}'.format(path=path, e=e))


def _ParseUserManagedPolicy(user_managed_policy):
  """"Reads user managed replication policy file and returns its data.

  Args:
      user_managed_policy (str): The json user managed message

  Returns:
      result (str): "user-managed"
      locations (list): Locations that are part of the user-managed replication
      keys (list): list of kms keys to be used for each replica.
  """
  if 'replicas' not in user_managed_policy or not user_managed_policy[
      'replicas']:
    raise exceptions.BadFileException(
        'Failed to find any replicas in user_managed policy.')
  keys = []
  locations = []
  for replica in user_managed_policy['replicas']:
    if 'location' not in replica:
      raise exceptions.BadFileException(
          'Failed to find a location in all replicas.')
    locations.append(replica['location'])
    if 'customerManagedEncryption' in replica:
      if 'kmsKeyName' in replica['customerManagedEncryption']:
        keys.append(replica['customerManagedEncryption']['kmsKeyName'])
      else:
        raise exceptions.BadFileException(
            'Failed to find a kmsKeyName in customerManagedEncryption for '
            'replica at least one replica.')
    if keys and len(keys) != len(locations):
      raise exceptions.BadFileException(
          'Only some replicas have customerManagedEncryption. Please either '
          'add the missing field to the remaining replicas or remove it from '
          'all replicas.')
  return 'user-managed', locations, keys


def _ParseAutomaticPolicy(automatic_policy):
  """"Reads automatic replication policy file and returns its data.

  Args:
      automatic_policy (str): The json user managed message

  Returns:
      result (str): "automatic"
      locations (list): empty list
      keys (list): 0 or 1 KMS keys depending on whether the policy has CMEK
  """
  if not automatic_policy:
    return 'automatic', [], []
  if 'customerManagedEncryption' not in automatic_policy:
    raise exceptions.BadFileException(
        'Failed to parse replication policy. Expected automatic to contain '
        'either nothing or customerManagedEncryption.')
  cmek = automatic_policy['customerManagedEncryption']
  if 'kmsKeyName' not in cmek:
    raise exceptions.BadFileException(
        'Failed to find a kmsKeyName in customerManagedEncryption.')
  return 'automatic', [], [cmek['kmsKeyName']]


def _ParseReplicationDict(replication_policy):
  """Reads replication policy dictionary and returns its data."""
  if 'userManaged' in replication_policy:
    return _ParseUserManagedPolicy(replication_policy['userManaged'])
  if 'automatic' in replication_policy:
    return _ParseAutomaticPolicy(replication_policy['automatic'])
  raise exceptions.BadFileException(
      'Expected to find either "userManaged" or "automatic" in replication, '
      'but found neither.')


def ParseReplicationFileContents(file_contents):
  """Reads replication policy file contents and returns its data.

  Reads the contents of a json or yaml replication policy file which conforms to
  https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets#replication
  and returns data needed to create a Secret with that policy. If the file
  doesn't conform to the expected format, a BadFileException is raised.

  For Secrets with an automtic policy, locations is empty and keys has
  either 0 or 1 entry depending on whether the policy includes CMEK. For Secrets
  with a user managed policy, the number of keys returns is either 0 or is equal
  to the number of locations returned with the Nth key corresponding to the Nth
  location.

  Args:
      file_contents (str): The unvalidated contents fo the replication file.

  Returns:
      result (str): Either "user-managed" or "automatic".
      locations (list): Locations that are part of the user-managed replication
      keys (list): list of kms keys to be used for each replica.
  """
  try:
    replication_policy = json.loads(file_contents)
    return _ParseReplicationDict(replication_policy)
  except ValueError:
    # Assume that this is yaml.
    pass
  try:
    replication_policy = yaml.load(file_contents)
    return _ParseReplicationDict(replication_policy)
  except yaml.YAMLParseError:
    raise exceptions.BadFileException(
        'Failed to parse replication policy file as json or yaml.')
