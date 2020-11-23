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
"""Implementation of CloudApi for s3 using boto3."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import boto3
import botocore
from googlecloudsdk.api_lib.storage import cloud_api
from googlecloudsdk.api_lib.storage import errors
from googlecloudsdk.command_lib.storage import storage_url
from googlecloudsdk.command_lib.storage.resources import resource_reference
from googlecloudsdk.command_lib.storage.resources import s3_resource_reference
from googlecloudsdk.core import exceptions as core_exceptions


def _catch_client_error_raise_s3_api_error(format_str=None):
  """Decorator that catches botocore ClientErrors and raises S3ApiErrors.

  Args:
    format_str (str): A googlecloudsdk.api_lib.storage.errors.S3ErrorPayload
      format string. Note that any properties that are accessed here are on the
      S3ErrorPayload object, not the object returned from botocore.

  Returns:
    A decorator that catches botocore.exceptions.ClientError and returns an
      S3ApiError with a formatted error message.
  """

  return errors.catch_error_raise_cloud_api_error(
      botocore.exceptions.ClientError, errors.S3ApiError, format_str=format_str)


_GCS_TO_S3_PREDEFINED_ACL_TRANSLATION_DICT = {
    'authenticatedRead': 'authenticated-read',
    'bucketOwnerFullControl': 'bucket-owner-full-control',
    'bucketOwnerRead': 'bucket-owner-read',
    'private': 'private',
    'publicRead': 'public-read',
    'publicReadWrite': 'public-read-write'
}


def _translate_predefined_acl_string_to_s3(predefined_acl_string):
  """Translates Apitools predefined ACL enum key (as string) to S3 equivalent.

  Args:
    predefined_acl_string (str): Value representing user permissions.

  Returns:
    Translated ACL string.

  Raises:
    ValueError: Predefined ACL translation could not be found.
  """
  if predefined_acl_string not in _GCS_TO_S3_PREDEFINED_ACL_TRANSLATION_DICT:
    raise ValueError('Could not translate predefined_acl_string {} to'
                     ' AWS-accepted ACL.'.format(predefined_acl_string))
  return _GCS_TO_S3_PREDEFINED_ACL_TRANSLATION_DICT[predefined_acl_string]


def _get_object_url_from_s3_response(object_dict,
                                     bucket_name,
                                     object_name=None):
  """Creates storage_url.CloudUrl from S3 API response.

  Args:
    object_dict (dict): Dictionary representing S3 API response.
    bucket_name (str): Bucket to include in URL.
    object_name (str | None): Object to include in URL.

  Returns:
    storage_url.CloudUrl populated with data.
  """
  return storage_url.CloudUrl(
      scheme=storage_url.ProviderPrefix.S3,
      bucket_name=bucket_name,
      object_name=object_name,
      generation=object_dict.get('VersionId'))


def _get_object_resource_from_s3_response(object_dict,
                                          bucket_name,
                                          object_name=None):
  """Creates resource_reference.S3ObjectResource from S3 API response.

  Args:
    object_dict (dict): Dictionary representing S3 API response.
    bucket_name (str): Bucket response is relevant to.
    object_name (str | None): Object if relevant to query.

  Returns:
    resource_reference.S3ObjectResource populated with data.
  """
  object_url = _get_object_url_from_s3_response(
      object_dict, bucket_name, object_name or object_dict['Key'])
  etag = None
  if 'ETag' in object_dict:
    etag = object_dict['ETag']
  elif 'CopyObjectResult' in object_dict:
    etag = object_dict['CopyObjectResult']['ETag']
  size = object_dict.get('Size')
  if size is None:
    size = object_dict.get('ContentLength')

  return s3_resource_reference.S3ObjectResource(
      object_url, etag=etag, metadata=object_dict, size=size)


def _get_prefix_resource_from_s3_response(prefix_dict, bucket_name):
  """Creates resource_reference.PrefixResource from S3 API response.

  Args:
    prefix_dict (dict): The S3 API response representing a prefix.
    bucket_name (str): Bucket for the prefix.

  Returns:
    A resource_reference.PrefixResource instance.
  """
  prefix = prefix_dict['Prefix']
  return resource_reference.PrefixResource(
      storage_url.CloudUrl(
          scheme=storage_url.ProviderPrefix.S3,
          bucket_name=bucket_name,
          object_name=prefix),
      prefix=prefix)


# pylint:disable=abstract-method
class S3Api(cloud_api.CloudApi):
  """S3 Api client."""

  def __init__(self):
    self.client = boto3.client(storage_url.ProviderPrefix.S3.value)

  def get_bucket(self, bucket_name, fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    metadata = {'Name': bucket_name}
    # TODO (b/168716392): As new commands are implemented, they may want
    # specific error handling for different methods.
    try:
      # Low-bandwidth way to determine if bucket exists for FieldsScope.SHORT.
      metadata.update(self.client.get_bucket_location(
          Bucket=bucket_name))
    except botocore.exceptions.ClientError as error:
      metadata['LocationConstraint'] = errors.S3ApiError(error)

    if fields_scope is not cloud_api.FieldsScope.SHORT:
      # Data for FieldsScope.NO_ACL.
      for key, api_call, result_has_key in [
          ('CORSRules', self.client.get_bucket_cors, True),
          ('LifecycleConfiguration',
           self.client.get_bucket_lifecycle_configuration, False),
          ('LoggingEnabled', self.client.get_bucket_logging, True),
          ('Payer', self.client.get_bucket_request_payment, True),
          ('Versioning', self.client.get_bucket_versioning, False),
          ('Website', self.client.get_bucket_website, False)]:
        try:
          api_result = api_call(Bucket=bucket_name)
          # Some results are wrapped in dictionaries with keys matching "key".
          metadata[key] = api_result.get(key) if result_has_key else api_result
        except botocore.exceptions.ClientError as error:
          metadata[key] = errors.S3ApiError(error)

      # User requested ACL's with FieldsScope.FULL.
      if fields_scope is cloud_api.FieldsScope.FULL:
        try:
          metadata['ACL'] = self.client.get_bucket_acl(Bucket=bucket_name)
        except botocore.exceptions.ClientError as error:
          metadata['ACL'] = errors.S3ApiError(error)

    return s3_resource_reference.S3BucketResource(
        storage_url.CloudUrl(storage_url.ProviderPrefix.S3, bucket_name),
        metadata=metadata)

  def list_buckets(self, fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    try:
      response = self.client.list_buckets()
      for bucket in response['Buckets']:
        if fields_scope == cloud_api.FieldsScope.FULL:
          yield self.get_bucket(bucket['Name'], fields_scope)
        else:
          yield s3_resource_reference.S3BucketResource(
              storage_url.CloudUrl(
                  storage_url.ProviderPrefix.S3, bucket['Name']),
              metadata={'Bucket': bucket, 'Owner': response['Owner']})
    except botocore.exceptions.ClientError as error:
      core_exceptions.reraise(errors.S3ApiError(error))

  def list_objects(self,
                   bucket_name,
                   prefix=None,
                   delimiter=None,
                   all_versions=False,
                   fields_scope=None):
    """See super class."""
    if all_versions:
      api_method_name = 'list_object_versions'
      objects_key = 'Versions'
    else:
      api_method_name = 'list_objects_v2'
      objects_key = 'Contents'
    try:
      paginator = self.client.get_paginator(api_method_name)
      page_iterator = paginator.paginate(
          Bucket=bucket_name,
          Prefix=prefix if prefix is not None else '',
          Delimiter=delimiter if delimiter is not None else '')
      for page in page_iterator:
        for object_dict in page.get(objects_key, []):
          if fields_scope is cloud_api.FieldsScope.FULL:
            # The metadata present in the list_objects_v2 response or the
            # list_object_versions response is not enough
            # for a FULL scope. Hence, calling the GetObjectMetadata method
            # to get the additonal metadata and ACLs information.
            yield self.get_object_metadata(
                bucket_name=bucket_name,
                object_name=object_dict['Key'],
                generation=object_dict.get('VersionId'),
                fields_scope=fields_scope)
          else:
            yield _get_object_resource_from_s3_response(object_dict,
                                                        bucket_name)
        for prefix_dict in page.get('CommonPrefixes', []):
          yield _get_prefix_resource_from_s3_response(prefix_dict, bucket_name)
    except botocore.exceptions.ClientError as error:
      core_exceptions.reraise(errors.S3ApiError(error))

  @_catch_client_error_raise_s3_api_error()
  def copy_object(self,
                  source_resource,
                  destination_resource,
                  progress_callback=None,
                  request_config=None):
    """See super class."""
    del progress_callback

    source_kwargs = {'Bucket': source_resource.storage_url.bucket_name,
                     'Key': source_resource.storage_url.object_name}
    if source_resource.storage_url.generation:
      source_kwargs['VersionId'] = source_resource.storage_url.generation

    kwargs = {'Bucket': destination_resource.storage_url.bucket_name,
              'Key': destination_resource.storage_url.object_name,
              'CopySource': source_kwargs}

    if request_config and request_config.predefined_acl_string:
      kwargs['ACL'] = _translate_predefined_acl_string_to_s3(
          request_config.predefined_acl_string)

    response = self.client.copy_object(**kwargs)
    return _get_object_resource_from_s3_response(response, kwargs['Bucket'],
                                                 kwargs['Key'])

    # TODO(b/161900052): Implement resumable copies.

  # pylint: disable=unused-argument
  @_catch_client_error_raise_s3_api_error()
  def download_object(self,
                      cloud_resource,
                      download_stream,
                      compressed_encoding=False,
                      decryption_wrapper=None,
                      digesters=None,
                      download_strategy=cloud_api.DownloadStrategy.ONE_SHOT,
                      progress_callback=None,
                      serialization_data=None,
                      start_byte=0,
                      end_byte=None):
    """See super class."""
    kwargs = {'Bucket': cloud_resource.bucket, 'Key': cloud_resource.name}
    if cloud_resource.generation:
      kwargs['VersionId'] = cloud_resource.generation

    response = self.client.get_object(**kwargs)
    download_stream.write(response['Body'].read())
    return response.get('ContentEncoding', None)

    # TODO(b/161437901): Handle resumed download.
    # TODO(b/161460749): Handle download retries.
    # pylint:enable=unused-argument

  @_catch_client_error_raise_s3_api_error()
  def get_object_metadata(self,
                          bucket_name,
                          object_name,
                          generation=None,
                          fields_scope=None):
    """See super class."""
    request = {'Bucket': bucket_name, 'Key': object_name}

    # The VersionId keyword argument to head_object is not nullable if it is
    # present, so only include it in the function call if it has a value.
    if generation is not None:
      request['VersionId'] = generation

    object_dict = self.client.head_object(**request)

    # User requested ACL's with FieldsScope.FULL.
    if fields_scope is cloud_api.FieldsScope.FULL:
      try:
        acl_response = self.client.get_object_acl(**request)
        acl_response.pop('ResponseMetadata', None)
        object_dict['ACL'] = acl_response
      except botocore.exceptions.ClientError as error:
        object_dict['ACL'] = errors.S3ApiError(error)

    return _get_object_resource_from_s3_response(object_dict, bucket_name,
                                                 object_name)

  @_catch_client_error_raise_s3_api_error()
  def upload_object(self,
                    source_stream,
                    destination_resource,
                    progress_callback=None,
                    request_config=None):
    """See super class."""
    # TODO(b/160998556): Implement resumable upload.
    del progress_callback

    if request_config is None:
      request_config = cloud_api.RequestConfig()

    kwargs = {
        'Bucket': destination_resource.storage_url.bucket_name,
        'Key': destination_resource.storage_url.object_name,
        'Body': source_stream.read(),
    }
    if request_config.predefined_acl_string:
      kwargs['ACL'] = _translate_predefined_acl_string_to_s3(
          request_config.predefined_acl_string)
    if request_config.md5_hash:
      kwargs['ContentMD5'] = request_config.md5_hash

    response = self.client.put_object(**kwargs)
    return _get_object_resource_from_s3_response(
        response, destination_resource.storage_url.bucket_name,
        destination_resource.storage_url.object_name)
