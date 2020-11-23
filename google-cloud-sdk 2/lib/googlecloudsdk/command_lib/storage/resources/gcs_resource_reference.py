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
"""GCS API-specific resource subclasses."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from apitools.base.protorpclite import messages
from googlecloudsdk.command_lib.storage.resources import resource_reference
from googlecloudsdk.command_lib.storage.resources import resource_util


def _json_dump_recursion_helper(metadata):
  """See _get_json_dump docstring."""
  if isinstance(metadata, list):
    return [_json_dump_recursion_helper(item) for item in metadata]

  if not isinstance(metadata, messages.Message):
    # Recursive function down the stack may have been processing a list.
    return resource_util.convert_to_json_parsable_type(metadata)

  # Construct dictionary from Apitools object.
  formatted_dict = collections.OrderedDict()
  # Similar to Apitools messages.py Message __repr__ implementation.
  for field in sorted(metadata.all_fields(), key=lambda f: f.name):
    value = metadata.get_assigned_value(field.name)
    if isinstance(value, messages.Message):
      # Recursively handled nested Apitools objects.
      formatted_dict[field.name] = _json_dump_recursion_helper(value)
    elif isinstance(value, list):
      # Recursively handled lists, which may contain Apitools objects.
      # Example: ACL list.
      formatted_list = [_json_dump_recursion_helper(item) for item in value]
      if formatted_list:
        # Ignore empty lists.
        formatted_dict[field.name] = formatted_list
    elif value or resource_util.should_preserve_falsy_metadata_value(value):
      # 0, 0.0, and False are acceptables Falsy-types. Lists handled later.
      formatted_dict[field.name] = (
          resource_util.convert_to_json_parsable_type(value))
  return formatted_dict


def _get_json_dump(resource):
  """Formats GCS resource metadata for printing.

  Args:
    resource (GcsBucketResource|GcsObjectResource): Resource object.

  Returns:
    Formatted JSON string for printing.
  """
  return resource_util.configured_json_dumps(
      collections.OrderedDict([
          ('url', resource.storage_url.url_string),
          ('type', resource.TYPE_STRING),
          ('metadata', _json_dump_recursion_helper(resource.metadata))
      ]))


def _get_full_bucket_metadata_string(resource):
  """Formats GCS resource metadata as string with rows.

  Args:
    resource (GCSBucketResource): Resource with metadata.

  Returns:
    Formatted multi-line string.
  """
  # Heavily-formatted sections.
  if resource.metadata.labels:
    labels_section = resource_util.get_metadata_json_section_string(
        'Labels', resource.metadata.labels, _json_dump_recursion_helper)
  else:
    labels_section = resource_util.get_padded_metadata_key_value_line(
        'Labels', 'None')

  if resource.metadata.acl:
    acl_section = resource_util.get_metadata_json_section_string(
        'ACL', resource.metadata.acl, _json_dump_recursion_helper)
  else:
    acl_section = resource_util.get_padded_metadata_key_value_line('ACL', '[]')

  if resource.metadata.defaultObjectAcl:
    default_acl_section = resource_util.get_metadata_json_section_string(
        'Default ACL', resource.metadata.defaultObjectAcl,
        _json_dump_recursion_helper)
  else:
    default_acl_section = resource_util.get_padded_metadata_key_value_line(
        'Default ACL', '[]')

  # Optional lines. Include all formatting since their presence is conditional.
  if resource.metadata.locationType:
    optional_location_type_line = resource_util.get_padded_metadata_key_value_line(
        'Location type', resource.metadata.locationType)
  else:
    optional_location_type_line = ''

  if resource.metadata.retentionPolicy:
    optional_retention_policy_line = resource_util.get_padded_metadata_key_value_line(
        'Retention Policy', 'Present')
  else:
    optional_retention_policy_line = ''

  if resource.metadata.defaultEventBasedHold:
    optional_default_event_based_hold_line = (
        resource_util.get_padded_metadata_key_value_line(
            'Default Event-Based Hold',
            resource.metadata.defaultEventBasedHold))
  else:
    optional_default_event_based_hold_line = ''

  if resource.metadata.timeCreated:
    optional_time_created_line = resource_util.get_padded_metadata_time_line(
        'Time created', resource.metadata.timeCreated)
  else:
    optional_time_created_line = ''

  if resource.metadata.updated:
    optional_time_updated_line = resource_util.get_padded_metadata_time_line(
        'Time updated', resource.metadata.updated)
  else:
    optional_time_updated_line = ''

  if resource.metadata.metageneration:
    optional_metageneration_line = resource_util.get_padded_metadata_key_value_line(
        'Metageneration', resource.metadata.metageneration)
  else:
    optional_metageneration_line = ''

  bucket_policy_only_object = getattr(resource.metadata.iamConfiguration,
                                      'bucketPolicyOnly', None)
  if bucket_policy_only_object:
    optional_bucket_policy_only_enabled_line = (
        resource_util.get_padded_metadata_key_value_line(
            'Bucket Policy Only enabled', bucket_policy_only_object.enabled))
  else:
    optional_bucket_policy_only_enabled_line = ''

  return (
      '{bucket_url}:\n'
      '{storage_class_line}'
      '{optional_location_type_line}'
      '{location_constraint_line}'
      '{versioning_enabled_line}'
      '{logging_config_line}'
      '{website_config_line}'
      '{cors_config_line}'
      '{lifecycle_config_line}'
      '{requester_pays_line}'
      '{optional_retention_policy_line}'
      '{optional_default_event_based_hold_line}'
      '{labels_section}'
      '{default_kms_key_line}'
      '{optional_time_created_line}'
      '{optional_time_updated_line}'
      '{optional_metageneration_line}'
      '{optional_bucket_policy_only_enabled_line}'
      '{acl_section}'
      '{default_acl_section}'
  ).format(
      bucket_url=resource.storage_url.versionless_url_string,
      storage_class_line=resource_util.get_padded_metadata_key_value_line(
          'Storage class', resource.metadata.storageClass),
      optional_location_type_line=optional_location_type_line,
      location_constraint_line=resource_util.get_padded_metadata_key_value_line(
          'Location constraint', resource.metadata.location),
      versioning_enabled_line=resource_util.get_padded_metadata_key_value_line(
          'Versioning enabled', (resource.metadata.versioning and
                                 resource.metadata.versioning.enabled)),
      logging_config_line=resource_util.get_padded_metadata_key_value_line(
          'Logging configuration',
          resource_util.get_exists_string(resource.metadata.logging)),
      website_config_line=resource_util.get_padded_metadata_key_value_line(
          'Website configuration',
          resource_util.get_exists_string(resource.metadata.website)),
      cors_config_line=resource_util.get_padded_metadata_key_value_line(
          'CORS configuration',
          resource_util.get_exists_string(resource.metadata.cors)),
      lifecycle_config_line=resource_util.get_padded_metadata_key_value_line(
          'Lifecycle configuration',
          resource_util.get_exists_string(resource.metadata.lifecycle)),
      requester_pays_line=resource_util.get_padded_metadata_key_value_line(
          'Requester Pays enabled', (resource.metadata.billing and
                                     resource.metadata.billing.requesterPays)),
      optional_retention_policy_line=optional_retention_policy_line,
      optional_default_event_based_hold_line=(
          optional_default_event_based_hold_line),
      labels_section=labels_section,
      default_kms_key_line=resource_util.get_padded_metadata_key_value_line(
          'Default KMS key',
          resource_util.get_exists_string(
              getattr(resource.metadata.encryption, 'defaultKmsKeyName',
                      None))),
      optional_time_created_line=optional_time_created_line,
      optional_time_updated_line=optional_time_updated_line,
      optional_metageneration_line=optional_metageneration_line,
      optional_bucket_policy_only_enabled_line=(
          optional_bucket_policy_only_enabled_line),
      acl_section=acl_section,
      # Remove ending newline character because this is the last list item.
      default_acl_section=default_acl_section[:-1])


def _get_full_object_metadata_string(resource):
  """Formats GCS resource metadata as string with rows.

  Args:
    resource (GCSObjectResource): Resource with metadata.

  Returns:
    Formatted multi-line string.
  """
  # Non-optional item that will always display.
  if resource.metadata.acl:
    acl_section = resource_util.get_metadata_json_section_string(
        'ACL', resource.metadata.acl, _json_dump_recursion_helper)
  else:
    acl_section = resource_util.get_padded_metadata_key_value_line('ACL', '[]')

  # Optional items that will conditionally display.
  if resource.creation_time:
    optional_time_created_line = resource_util.get_padded_metadata_time_line(
        'Creation time', resource.creation_time)
  else:
    optional_time_created_line = ''

  if resource.metadata.updated:
    optional_time_updated_line = resource_util.get_padded_metadata_time_line(
        'Update time', resource.metadata.updated)
  else:
    optional_time_updated_line = ''

  if resource.metadata.timeStorageClassUpdated and (
      resource.metadata.timeStorageClassUpdated !=
      resource.metadata.timeCreated):
    optional_time_storage_class_created_line = resource_util.get_padded_metadata_time_line(
        'Storage class update time', resource.metadata.timeStorageClassUpdated)
  else:
    optional_time_storage_class_created_line = ''

  if resource.metadata.storageClass:
    optional_storage_class_line = resource_util.get_padded_metadata_key_value_line(
        'Storage class', resource.metadata.storageClass)
  else:
    optional_storage_class_line = ''

  if resource.metadata.temporaryHold:
    optional_temporary_hold_line = resource_util.get_padded_metadata_key_value_line(
        'Temporary Hold', 'Enabled')
  else:
    optional_temporary_hold_line = ''

  if resource.metadata.eventBasedHold:
    optional_event_based_hold_line = resource_util.get_padded_metadata_key_value_line(
        'Event-Based Hold', 'Enabled')
  else:
    optional_event_based_hold_line = ''

  if resource.metadata.retentionExpirationTime:
    optional_retention_expiration_time_line = (
        resource_util.get_padded_metadata_key_value_line(
            'Retention Expiration', 'Enabled'))
  else:
    optional_retention_expiration_time_line = ''

  if resource.metadata.kmsKeyName:
    optional_kms_key_name_line = resource_util.get_padded_metadata_key_value_line(
        'KMS key', resource.metadata.kmsKeyName)
  else:
    optional_kms_key_name_line = ''

  if resource.metadata.cacheControl:
    optional_cache_control_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Control', resource.metadata.cacheControl)
  else:
    optional_cache_control_line = ''

  if resource.metadata.contentDisposition:
    optional_content_disposition_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Disposition', resource.metadata.contentDisposition)
  else:
    optional_content_disposition_line = ''

  if resource.metadata.contentEncoding:
    optional_content_encoding_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Encoding', resource.metadata.contentEncoding)
  else:
    optional_content_encoding_line = ''

  if resource.metadata.contentLanguage:
    optional_content_language_line = resource_util.get_padded_metadata_key_value_line(
        'Cache-Language', resource.metadata.contentLanguage)
  else:
    optional_content_language_line = ''

  if resource.metadata.componentCount:
    optional_component_count_line = resource_util.get_padded_metadata_key_value_line(
        'Component-Count', resource.metadata.componentCount)
  else:
    optional_component_count_line = ''

  if resource.metadata.customTime:
    optional_custom_time_line = resource_util.get_padded_metadata_key_value_line(
        'Custom-Time', resource.metadata.customTime)
  else:
    optional_custom_time_line = ''

  if resource.metadata.timeDeleted:
    optional_noncurrent_time_line = resource_util.get_padded_metadata_time_line(
        'Noncurrent time', resource.metadata.timeDeleted)
  else:
    optional_noncurrent_time_line = ''

  if getattr(resource.metadata.metadata, 'additionalProperties', None):
    optional_metadata_section = resource_util.get_metadata_json_section_string(
        'Additional Properties',
        resource.metadata.metadata.additionalProperties,
        _json_dump_recursion_helper)
  else:
    optional_metadata_section = ''

  if resource.metadata.crc32c:
    optional_crc32c_line = resource_util.get_padded_metadata_key_value_line(
        'Hash (crc32c)', resource.metadata.crc32c)
  else:
    if resource.metadata.customerEncryption:
      optional_crc32c_line = resource_util.get_padded_metadata_key_value_line(
          'Hash (crc32c)', 'encrypted')
    else:
      optional_crc32c_line = ''

  if resource.metadata.md5Hash:
    optional_md5_line = resource_util.get_padded_metadata_key_value_line(
        'Hash (md5)', resource.metadata.md5Hash)
  else:
    if resource.metadata.customerEncryption:
      optional_md5_line = resource_util.get_padded_metadata_key_value_line(
          'Hash (md5)', 'encrypted')
    else:
      optional_md5_line = ''

  if getattr(resource.metadata.customerEncryption, 'encryptionAlgorithm', None):
    optional_encryption_algorithm_line = resource_util.get_padded_metadata_key_value_line(
        'Encryption algorithm',
        resource.metadata.customerEncryption.encryptionAlgorithm)
  else:
    optional_encryption_algorithm_line = ''

  if getattr(resource.metadata.customerEncryption, 'keySha256', None):
    optional_encryption_key_sha_256_line = resource_util.get_padded_metadata_key_value_line(
        'Encryption key SHA256', resource.metadata.customerEncryption.keySha256)
  else:
    optional_encryption_key_sha_256_line = ''

  if resource.generation:
    optional_generation_line = resource_util.get_padded_metadata_key_value_line(
        'Generation', resource.generation)
  else:
    optional_generation_line = ''

  if resource.metageneration:
    optional_metageneration_line = resource_util.get_padded_metadata_key_value_line(
        'Metageneration', resource.metageneration)
  else:
    optional_metageneration_line = ''

  return (
      '{object_url}:\n'
      '{optional_time_created_line}'
      '{optional_time_updated_line}'
      '{optional_time_storage_class_created_line}'
      '{optional_storage_class_line}'
      '{optional_temporary_hold_line}'
      '{optional_event_based_hold_line}'
      '{optional_retention_expiration_time_line}'
      '{optional_kms_key_name_line}'
      '{optional_cache_control_line}'
      '{optional_content_disposition_line}'
      '{optional_content_encoding_line}'
      '{optional_content_language_line}'
      '{content_length_line}'
      '{content_type_line}'
      '{optional_component_count_line}'
      '{optional_custom_time_line}'
      '{optional_noncurrent_time_line}'
      '{optional_metadata_section}'
      '{optional_crc32c_line}'
      '{optional_md5_line}'
      '{optional_encryption_algorithm_line}'
      '{optional_encryption_key_sha_256_line}'
      '{etag_line}'
      '{optional_generation_line}'
      '{optional_metageneration_line}'
      '{acl_section}'
  ).format(
      object_url=resource.storage_url.versionless_url_string,
      optional_time_created_line=optional_time_created_line,
      optional_time_updated_line=optional_time_updated_line,
      optional_time_storage_class_created_line=(
          optional_time_storage_class_created_line),
      optional_storage_class_line=optional_storage_class_line,
      optional_temporary_hold_line=optional_temporary_hold_line,
      optional_event_based_hold_line=optional_event_based_hold_line,
      optional_retention_expiration_time_line=(
          optional_retention_expiration_time_line),
      optional_kms_key_name_line=optional_kms_key_name_line,
      optional_cache_control_line=optional_cache_control_line,
      optional_content_disposition_line=optional_content_disposition_line,
      optional_content_encoding_line=optional_content_encoding_line,
      optional_content_language_line=optional_content_language_line,
      content_length_line=resource_util.get_padded_metadata_key_value_line(
          'Content-Length', resource.size),
      content_type_line=resource_util.get_padded_metadata_key_value_line(
          'Content-Type', resource.metadata.contentType),
      optional_component_count_line=optional_component_count_line,
      optional_custom_time_line=optional_custom_time_line,
      optional_noncurrent_time_line=optional_noncurrent_time_line,
      optional_metadata_section=optional_metadata_section,
      optional_crc32c_line=optional_crc32c_line,
      optional_md5_line=optional_md5_line,
      optional_encryption_algorithm_line=optional_encryption_algorithm_line,
      optional_encryption_key_sha_256_line=optional_encryption_key_sha_256_line,
      etag_line=resource_util.get_padded_metadata_key_value_line(
          'ETag', resource.etag),
      optional_generation_line=optional_generation_line,
      optional_metageneration_line=optional_metageneration_line,
      # Remove ending newline character because this is the last list item.
      acl_section=acl_section[:-1])


class GcsBucketResource(resource_reference.BucketResource):
  """API-specific subclass for handling metadata."""

  def get_full_metadata_string(self):
    return _get_full_bucket_metadata_string(self)

  def get_json_dump(self):
    return _get_json_dump(self)


class GcsObjectResource(resource_reference.ObjectResource):
  """API-specific subclass for handling metadata."""

  def get_full_metadata_string(self):
    return _get_full_object_metadata_string(self)

  def get_json_dump(self):
    return _get_json_dump(self)
