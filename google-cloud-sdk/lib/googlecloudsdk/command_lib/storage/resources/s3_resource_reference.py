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
"""S3 API-specific resource subclasses."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from googlecloudsdk.api_lib.storage import errors
from googlecloudsdk.command_lib.storage.resources import resource_reference
from googlecloudsdk.command_lib.storage.resources import resource_util


_INCOMPLETE_OBJECT_METADATA_WARNING = (
    'Use "-j", the JSON flag, to view additional S3 metadata.')


def _json_dump_recursion_helper(metadata):
  """See _get_json_dump docstring."""
  if isinstance(metadata, list):
    return [_json_dump_recursion_helper(item) for item in metadata]

  if not isinstance(metadata, dict):
    return resource_util.convert_to_json_parsable_type(metadata)

  # Sort by key to make sure dictionary always prints in correct order.
  formatted_dict = collections.OrderedDict(sorted(metadata.items()))
  for key, value in formatted_dict.items():
    if isinstance(value, dict):
      # Recursively handle dictionaries.
      formatted_dict[key] = _json_dump_recursion_helper(value)
    elif isinstance(value, list):
      # Recursively handled lists, which may contain more dicts, like ACLs.
      formatted_list = [_json_dump_recursion_helper(item) for item in value]
      if formatted_list:
        # Ignore empty lists.
        formatted_dict[key] = formatted_list
    elif value or resource_util.should_preserve_falsy_metadata_value(value):
      formatted_dict[key] = resource_util.convert_to_json_parsable_type(value)

  return formatted_dict


def _get_json_dump(resource):
  """Formats S3 resource metadata as JSON.

  Args:
    resource (S3BucketResource|S3ObjectResource): Resource object.

  Returns:
    Formatted JSON string.
  """
  return resource_util.configured_json_dumps(
      collections.OrderedDict([
          ('url', resource.storage_url.url_string),
          ('type', resource.TYPE_STRING),
          ('metadata', _json_dump_recursion_helper(resource.metadata)),
      ]))


def _get_error_or_exists_string(value):
  """Returns error if value is error or existence string."""
  if isinstance(value, errors.S3ApiError):
    return value
  else:
    return resource_util.get_exists_string(value)


def _get_formatted_acl_section(acl_metadata):
  """Returns formatted ACLs, error, or formatted none value."""
  if isinstance(acl_metadata, errors.S3ApiError):
    return resource_util.get_padded_metadata_key_value_line('ACL', acl_metadata)
  elif acl_metadata:
    return resource_util.get_metadata_json_section_string(
        'ACL', acl_metadata, _json_dump_recursion_helper)
  else:
    return resource_util.get_padded_metadata_key_value_line('ACL', '[]')


def _get_full_bucket_metadata_string(resource):
  """Formats S3 resource metadata as string with rows.

  Args:
    resource (S3BucketResource): Resource with metadata.

  Returns:
    Formatted multi-line string.
  """
  # Hardcoded strings found in Boto docs:
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
  logging_enabled_value = _get_error_or_exists_string(
      resource.metadata['LoggingEnabled'])
  website_value = _get_error_or_exists_string(resource.metadata['Website'])
  cors_value = _get_error_or_exists_string(resource.metadata['CORSRules'])
  lifecycle_configuration_value = _get_error_or_exists_string(
      resource.metadata['LifecycleConfiguration'])

  if isinstance(resource.metadata['Versioning'], errors.S3ApiError):
    versioning_enabled_value = resource.metadata['Versioning']
  else:
    versioning_status = resource.metadata['Versioning'].get('Status')
    if versioning_status == 'Enabled':
      versioning_enabled_value = True
    elif versioning_status == 'Suspended':
      versioning_enabled_value = False
    else:
      versioning_enabled_value = None

  if isinstance(resource.metadata['Payer'], errors.S3ApiError):
    requester_pays_value = resource.metadata['Payer']
  elif resource.metadata['Payer'] == 'Requester':
    requester_pays_value = True
  elif resource.metadata['Payer'] == 'BucketOwner':
    requester_pays_value = False
  else:
    requester_pays_value = None

  return (
      '{bucket_url}:\n'
      '{location_constraint_line}'
      '{versioning_enabled_line}'
      '{logging_config_line}'
      '{website_config_line}'
      '{cors_config_line}'
      '{lifecycle_config_line}'
      '{requester_pays_line}'
      '{acl_section}'
  ).format(
      bucket_url=resource.storage_url.versionless_url_string,
      location_constraint_line=resource_util.get_padded_metadata_key_value_line(
          'Location constraint', resource.metadata['LocationConstraint']),
      versioning_enabled_line=resource_util.get_padded_metadata_key_value_line(
          'Versioning enabled', versioning_enabled_value),
      logging_config_line=resource_util.get_padded_metadata_key_value_line(
          'Logging configuration', logging_enabled_value),
      website_config_line=resource_util.get_padded_metadata_key_value_line(
          'Website configuration', website_value),
      cors_config_line=resource_util.get_padded_metadata_key_value_line(
          'CORS configuration', cors_value),
      lifecycle_config_line=resource_util.get_padded_metadata_key_value_line(
          'Lifecycle configuration', lifecycle_configuration_value),
      requester_pays_line=resource_util.get_padded_metadata_key_value_line(
          'Requester Pays enabled', requester_pays_value),
      # Remove ending newline character because this is the last list item.
      acl_section=_get_formatted_acl_section(resource.metadata['ACL'])[:-1])


def _get_full_object_metadata_string(resource):
  """Formats S3 resource metadata as string with rows.

  Args:
    resource (S3ObjectResource): Resource with metadata.

  Returns:
    Formatted multi-line string.
  """
  # Hardcoded strings found in Boto docs:
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
  if 'LastModified' in resource.metadata:
    optional_time_updated_line = resource_util.get_padded_metadata_time_line(
        'Update time', resource.metadata['LastModified'])
  else:
    optional_time_updated_line = ''

  if 'StorageClass' in resource.metadata:
    optional_storage_class_line = resource_util.get_padded_metadata_key_value_line(
        'Storage class', resource.metadata['StorageClass'])
  else:
    optional_storage_class_line = ''

  if 'CacheControl' in resource.metadata:
    optional_cache_control_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Control', resource.metadata['CacheControl'])
  else:
    optional_cache_control_line = ''

  if 'CacheDisposition' in resource.metadata:
    optional_content_disposition_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Disposition', resource.metadata['CacheDisposition'])
  else:
    optional_content_disposition_line = ''

  if 'ContentEncoding' in resource.metadata:
    optional_content_encoding_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Encoding', resource.metadata['ContentEncoding'])
  else:
    optional_content_encoding_line = ''

  if 'ContentLanguage' in resource.metadata:
    optional_content_language_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Language', resource.metadata['ContentLanguage'])
  else:
    optional_content_language_line = ''

  if 'PartsCount' in resource.metadata:
    optional_component_count_line = (
        resource_util.get_padded_metadata_key_value_line(
            'Component-Count', resource.metadata['PartsCount']))
  else:
    optional_component_count_line = ''

  if 'SSECustomerAlgorithm' in resource.metadata:
    optional_encryption_algorithm_line = (
        resource_util.get_padded_metadata_key_value_line(
            'Encryption algorithm', resource.metadata['SSECustomerAlgorithm']))
  else:
    optional_encryption_algorithm_line = ''

  if resource.generation:
    optional_generation_line = resource_util.get_padded_metadata_key_value_line(
        'Generation', resource.generation)
  else:
    optional_generation_line = ''

  return (
      '{object_url}:\n'
      '{optional_time_updated_line}'
      '{optional_storage_class_line}'
      '{optional_cache_control_line}'
      '{optional_content_disposition_line}'
      '{optional_content_encoding_line}'
      '{optional_content_language_line}'
      '{content_length_line}'
      '{content_type_line}'
      '{optional_component_count_line}'
      '{optional_encryption_algorithm_line}'
      '{etag_line}'
      '{optional_generation_line}'
      '{acl_section}'
      '  {incomplete_warning}').format(
          object_url=resource.storage_url.versionless_url_string,
          optional_time_updated_line=optional_time_updated_line,
          optional_storage_class_line=optional_storage_class_line,
          optional_cache_control_line=optional_cache_control_line,
          optional_content_disposition_line=optional_content_disposition_line,
          optional_content_encoding_line=optional_content_encoding_line,
          optional_content_language_line=optional_content_language_line,
          content_length_line=resource_util.get_padded_metadata_key_value_line(
              'Content-Length', resource.size),
          content_type_line=resource_util.get_padded_metadata_key_value_line(
              'Content-Type', resource.metadata.get('ContentType')),
          optional_component_count_line=optional_component_count_line,
          optional_encryption_algorithm_line=optional_encryption_algorithm_line,
          etag_line=resource_util.get_padded_metadata_key_value_line(
              'ETag', resource.etag),
          optional_generation_line=optional_generation_line,
          acl_section=_get_formatted_acl_section(resource.metadata.get('ACL')),
          incomplete_warning=_INCOMPLETE_OBJECT_METADATA_WARNING)


class S3BucketResource(resource_reference.BucketResource):
  """API-specific subclass for handling metadata."""

  def get_full_metadata_string(self):
    return _get_full_bucket_metadata_string(self)

  def get_json_dump(self):
    return _get_json_dump(self)


class S3ObjectResource(resource_reference.ObjectResource):
  """API-specific subclass for handling metadata."""

  def get_full_metadata_string(self):
    return _get_full_object_metadata_string(self)

  def get_json_dump(self):
    return _get_json_dump(self)
