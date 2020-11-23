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
"""Client for interacting with Google Cloud Storage.

Implements CloudApi for the GCS JSON API. Example functions include listing
buckets, uploading objects, and setting lifecycle conditions.

TODO(b/160601969): Update class with remaining API methods for ls and cp.
    Note, this class has not been tested against the GCS API yet.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

from apitools.base.py import exceptions as apitools_exceptions
from apitools.base.py import list_pager
from apitools.base.py import transfer as apitools_transfer

from googlecloudsdk.api_lib.storage import cloud_api
from googlecloudsdk.api_lib.storage import errors as cloud_errors
# pylint: disable=unused-import
# Applies pickling patches:
from googlecloudsdk.api_lib.storage import patch_gcs_messages
# pylint: enable=unused-import
from googlecloudsdk.api_lib.util import apis as core_apis
from googlecloudsdk.command_lib.storage import storage_url
from googlecloudsdk.command_lib.storage.resources import gcs_resource_reference
from googlecloudsdk.command_lib.storage.resources import resource_reference
from googlecloudsdk.core import exceptions as core_exceptions
from googlecloudsdk.core import properties
from googlecloudsdk.core.credentials import transports


DEFAULT_CONTENT_TYPE = 'application/octet-stream'
# TODO(b/161346648) Retrieve number of retries from Boto config file.
DEFAULT_NUM_RETRIES = 23
# 100 MB in bytes.
DEFAULT_UPLOAD_CHUNK_SIZE = 104857600


def _catch_http_error_raise_gcs_api_error(format_str=None):
  """Decorator catches HttpError and returns GcsApiError with custom message.

  Args:
    format_str (str): A googlecloudsdk.api_lib.util.exceptions.HttpErrorPayload
      format string. Note that any properties that are accessed here are on the
      HttpErrorPayload object, not the object returned from the server.

  Returns:
    A decorator that catches apitools.HttpError and returns GcsApiError with a
      customizable error message.
  """
  return cloud_errors.catch_error_raise_cloud_api_error(
      apitools_exceptions.HttpError,
      cloud_errors.GcsApiError,
      format_str=format_str)


def _bucket_resource_from_metadata(metadata):
  """Helper method to generate a BucketResource instance from GCS metadata.

  Args:
    metadata (messages.Bucket): Extract resource properties from this.

  Returns:
    BucketResource with properties populated by metadata.
  """
  url = storage_url.CloudUrl(scheme=storage_url.ProviderPrefix.GCS,
                             bucket_name=metadata.name)
  return gcs_resource_reference.GcsBucketResource(
      url, etag=metadata.etag, metadata=metadata)


def _object_resource_from_metadata(metadata):
  """Helper method to generate a ObjectResource instance from GCS metadata.

  Args:
    metadata (messages.Object): Extract resource properties from this.

  Returns:
    ObjectResource with properties populated by metadata.
  """
  if metadata.generation is not None:
    # Generation may be 0 integer, which is valid although falsy.
    generation = str(metadata.generation)
  else:
    generation = None
  url = storage_url.CloudUrl(
      scheme=storage_url.ProviderPrefix.GCS,
      bucket_name=metadata.bucket,
      object_name=metadata.name,
      generation=generation)
  return gcs_resource_reference.GcsObjectResource(
      url,
      creation_time=metadata.timeCreated,
      etag=metadata.etag,
      md5_hash=metadata.md5Hash,
      metadata=metadata,
      metageneration=metadata.metageneration,
      size=metadata.size)


def _no_op_callback(unused_response, unused_object):
  """Disables Apitools' default print callbacks."""
  pass


class GcsRequestConfig(cloud_api.RequestConfig):
  """Arguments object for requests with custom GCS parameters.

  Attributes:
      decryption_wrapper (CryptoKeyWrapper):
          utils.encryption_helper.CryptoKeyWrapper for decrypting an object.
      encryption_wrapper (CryptoKeyWrapper):
          utils.encryption_helper.CryptoKeyWrapper for encrypting an object.
      gzip_encoded (bool): Whether to use gzip transport encoding for the
          upload.
      max_bytes_per_call (int): Integer describing maximum number of bytes
          to write per service call.
      md5_hash (str): MD5 digest to use for validation.
      precondition_generation_match (int): Perform request only if generation of
          target object matches the given integer. Ignored for bucket requests.
      precondition_metageneration_match (int): Perform request only if
          metageneration of target object/bucket matches the given integer.
      predefined_acl_string (str): Passed to parent class.
      size (int): Object size in bytes.
  """

  def __init__(self,
               decryption_wrapper=None,
               encryption_wrapper=None,
               gzip_encoded=False,
               max_bytes_per_call=None,
               md5_hash=None,
               precondition_generation_match=None,
               precondition_metageneration_match=None,
               predefined_acl_string=None,
               size=None):
    super(GcsRequestConfig, self).__init__(
        md5_hash=md5_hash, predefined_acl_string=predefined_acl_string)
    self.decryption_wrapper = decryption_wrapper
    self.encryption_wrapper = encryption_wrapper
    self.gzip_encoded = gzip_encoded
    self.max_bytes_per_call = max_bytes_per_call
    self.precondition_generation_match = precondition_generation_match
    self.precondition_metageneration_match = precondition_metageneration_match
    self.size = size

  def __eq__(self, other):
    return (super(GcsRequestConfig, self).__eq__(other) and
            self.decryption_wrapper == other.decryption_wrapper and
            self.encryption_wrapper == other.encryption_wrapper and
            self.gzip_encoded == other.gzip_encoded and
            self.max_bytes_per_call == other.max_bytes_per_call and
            self.precondition_generation_match ==
            other.precondition_generation_match and
            self.precondition_metageneration_match ==
            other.precondition_metageneration_match and self.size == other.size)


class GcsApi(cloud_api.CloudApi):
  """Client for Google Cloud Storage API."""

  def __init__(self):
    self.client = core_apis.GetClientInstance('storage', 'v1')
    self.messages = core_apis.GetMessagesModule('storage', 'v1')

  def _get_projection(self, fields_scope, message_class):
    """Generate query projection from fields_scope.

    Args:
      fields_scope (FieldsScope): Used to determine projection to return.
      message_class (object): Apitools message object that contains a projection
          enum.

    Returns:
      projection (ProjectionValueValuesEnum): Determines if ACL properties
          should be returned.

    Raises:
      ValueError: The fields_scope isn't recognized.
    """
    if fields_scope not in cloud_api.FieldsScope:
      raise ValueError('Invalid fields_scope.')
    projection_enum = message_class.ProjectionValueValuesEnum

    if fields_scope == cloud_api.FieldsScope.FULL:
      return projection_enum.full
    return projection_enum.noAcl

  @_catch_http_error_raise_gcs_api_error()
  def create_bucket(self,
                    bucket_resource,
                    fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    projection = self._get_projection(fields_scope,
                                      self.messages.StorageBucketsInsertRequest)
    if not bucket_resource.metadata:
      bucket_resource.metadata = self.messages.Bucket(name=bucket_resource.name)

    request = self.messages.StorageBucketsInsertRequest(
        bucket=bucket_resource.metadata,
        project=properties.VALUES.core.project.GetOrFail(),
        projection=projection)

    created_bucket_metadata = self.client.buckets.Insert(request)
    return _bucket_resource_from_metadata(created_bucket_metadata)

  @_catch_http_error_raise_gcs_api_error()
  def delete_bucket(self, bucket_name, request_config=None):
    """See super class."""
    if not request_config:
      request_config = GcsRequestConfig()
    request = self.messages.StorageBucketsDeleteRequest(
        bucket=bucket_name,
        ifMetagenerationMatch=request_config.precondition_metageneration_match)
    # Success returns an empty body.
    # https://cloud.google.com/storage/docs/json_api/v1/buckets/delete
    self.client.buckets.Delete(request)

  @_catch_http_error_raise_gcs_api_error()
  def get_bucket(self, bucket_name, fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    projection = self._get_projection(fields_scope,
                                      self.messages.StorageBucketsGetRequest)
    request = self.messages.StorageBucketsGetRequest(
        bucket=bucket_name,
        projection=projection)

    metadata = self.client.buckets.Get(request)
    return _bucket_resource_from_metadata(metadata)

  def list_buckets(self, fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    projection = self._get_projection(fields_scope,
                                      self.messages.StorageBucketsListRequest)
    request = self.messages.StorageBucketsListRequest(
        project=properties.VALUES.core.project.GetOrFail(),
        projection=projection)

    global_params = None
    if fields_scope == cloud_api.FieldsScope.SHORT:
      global_params = self.messages.StandardQueryParameters()
      global_params.fields = 'items/name'
    # TODO(b/160238394) Decrypt metadata fields if necessary.
    bucket_iter = list_pager.YieldFromList(
        self.client.buckets,
        request,
        batch_size=cloud_api.NUM_ITEMS_PER_LIST_PAGE,
        global_params=global_params)
    try:
      for bucket in bucket_iter:
        yield _bucket_resource_from_metadata(bucket)
    except apitools_exceptions.HttpError as error:
      core_exceptions.reraise(cloud_errors.GcsApiError(error))

  def list_objects(self,
                   bucket_name,
                   prefix=None,
                   delimiter=None,
                   all_versions=None,
                   fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""
    projection = self._get_projection(fields_scope,
                                      self.messages.StorageObjectsListRequest)
    global_params = None
    if fields_scope == cloud_api.FieldsScope.SHORT:
      global_params = self.messages.StandardQueryParameters()
      global_params.fields = 'prefixes,items/name,items/size,items/generation'

    object_list = None
    while True:
      apitools_request = self.messages.StorageObjectsListRequest(
          bucket=bucket_name,
          prefix=prefix,
          delimiter=delimiter,
          versions=all_versions,
          projection=projection,
          pageToken=object_list.nextPageToken if object_list else None,
          maxResults=cloud_api.NUM_ITEMS_PER_LIST_PAGE)

      try:
        object_list = self.client.objects.List(
            apitools_request, global_params=global_params)
      except apitools_exceptions.HttpError as error:
        core_exceptions.reraise(cloud_errors.GcsApiError(error))

      # Yield objects.
      # TODO(b/160238394) Decrypt metadata fields if necessary.
      for object_metadata in object_list.items:
        object_metadata.bucket = bucket_name
        yield _object_resource_from_metadata(object_metadata)

      # Yield prefixes.
      for prefix_string in object_list.prefixes:
        yield resource_reference.PrefixResource(
            storage_url.CloudUrl(
                scheme=storage_url.ProviderPrefix.GCS,
                bucket_name=bucket_name,
                object_name=prefix_string),
            prefix=prefix_string
        )

      if not object_list.nextPageToken:
        break

  @_catch_http_error_raise_gcs_api_error()
  def delete_object(self,
                    bucket_name,
                    object_name,
                    generation=None,
                    request_config=None):
    """See super class."""
    if not request_config:
      request_config = GcsRequestConfig()

    request = self.messages.StorageObjectsDeleteRequest(
        bucket=bucket_name,
        object=object_name,
        generation=generation,
        ifGenerationMatch=request_config.precondition_generation_match,
        ifMetagenerationMatch=request_config.precondition_metageneration_match)
    # Success returns an empty body.
    # https://cloud.google.com/storage/docs/json_api/v1/objects/delete
    self.client.objects.Delete(request)

  @_catch_http_error_raise_gcs_api_error()
  def get_object_metadata(self,
                          bucket_name,
                          object_name,
                          generation=None,
                          fields_scope=cloud_api.FieldsScope.NO_ACL):
    """See super class."""

    # S3 requires a string, but GCS uses an int for generation.
    if generation:
      generation = int(generation)

    projection = self._get_projection(fields_scope,
                                      self.messages.StorageObjectsGetRequest)

    # TODO(b/160238394) Decrypt metadata fields if necessary.
    try:
      object_metadata = self.client.objects.Get(
          self.messages.StorageObjectsGetRequest(
              bucket=bucket_name,
              object=object_name,
              generation=generation,
              projection=projection))
    except apitools_exceptions.HttpNotFoundError:
      raise cloud_errors.NotFoundError(
          'Object not found: {}'.format(storage_url.CloudUrl(
              storage_url.ProviderPrefix.GCS, bucket_name, object_name,
              generation).url_string)
      )
    return _object_resource_from_metadata(object_metadata)

  @_catch_http_error_raise_gcs_api_error()
  def patch_object_metadata(self,
                            bucket_name,
                            object_name,
                            object_resource,
                            fields_scope=cloud_api.FieldsScope.NO_ACL,
                            generation=None,
                            request_config=None):
    """See super class."""
    # S3 requires a string, but GCS uses an int for generation.
    if generation:
      generation = int(generation)

    if not request_config:
      request_config = GcsRequestConfig()

    predefined_acl = None
    if request_config.predefined_acl_string:
      predefined_acl = getattr(self.messages.StorageObjectsPatchRequest.
                               PredefinedAclValueValuesEnum,
                               request_config.predefined_acl_string)

    projection = self._get_projection(fields_scope,
                                      self.messages.StorageObjectsPatchRequest)

    # Assume parameters are only for identifying what needs to be patched, and
    # the resource contains the desired patched metadata values.
    patched_metadata = object_resource.metadata
    if not patched_metadata:
      object_resource.metadata = self.messages.Object(
          name=object_resource.name, bucket=object_resource.bucket)

    request = self.messages.StorageObjectsPatchRequest(
        bucket=bucket_name,
        object=object_name,
        objectResource=object_resource.metadata,
        generation=generation,
        ifGenerationMatch=request_config.precondition_generation_match,
        ifMetagenerationMatch=request_config.precondition_metageneration_match,
        predefinedAcl=predefined_acl,
        projection=projection)

    updated_metadata = self.client.objects.Patch(request)
    return _object_resource_from_metadata(updated_metadata)

  @_catch_http_error_raise_gcs_api_error()
  def copy_object(self,
                  source_resource,
                  destination_resource,
                  progress_callback=None,
                  request_config=None):
    """See super class."""
    # TODO(b/161900052): Implement resumable copies.
    # TODO(b/161898251): Implement encryption and decryption.
    del progress_callback

    if not request_config:
      request_config = GcsRequestConfig()

    result_metadata = self.client.objects.Copy(
        self.messages.StorageObjectsCopyRequest(
            sourceBucket=source_resource.storage_url.bucket_name,
            sourceObject=source_resource.storage_url.object_name,
            destinationBucket=destination_resource.storage_url.bucket_name,
            destinationObject=destination_resource.storage_url.object_name,
            ifSourceGenerationMatch=(
                request_config.precondition_generation_match)))
    return _object_resource_from_metadata(result_metadata)

  # pylint: disable=unused-argument
  def _download_object(self,
                       cloud_resource,
                       download_stream,
                       apitools_download,
                       apitools_request,
                       compressed_encoding=False,
                       decryption_wrapper=None,
                       generation=None,
                       serialization_data=None,
                       start_byte=0,
                       end_byte=None):
    """GCS-specific download implementation.

    Args:
      cloud_resource (resource_reference.ObjectResource): Contains
          metadata and information about object being downloaded.
      download_stream (stream): Stream to send the object data to.
      apitools_download (apitools.transfer.Download): Apitools object for
          managing downloads.
      apitools_request (apitools.messages.StorageObjectsGetReqest):
          Holds call to GCS API.
      compressed_encoding (bool): If true, object is stored with a compressed
          encoding.
      decryption_wrapper (CryptoKeyWrapper):
          utils.encryption_helper.CryptoKeyWrapper that can optionally be added
          to decrypt an encrypted object.
      generation (int): Generation of the object to retrieve.
      serialization_data (str): Implementation-specific JSON string of a dict
          containing serialization information for the download.
      start_byte (int): Starting point for download (for resumable downloads and
          range requests). Can be set to negative to request a range of bytes
          (python equivalent of [:-3]).
      end_byte (int): Ending byte number, inclusive, for download (for range
          requests). If None, download the rest of the object.

    Returns:
      Encoding string for object if requested. Otherwise, None.
    """
    # Fresh download.
    if not serialization_data:
      self.client.objects.Get(apitools_request, download=apitools_download)

    # TODO(b/161453101): Optimize handling of gzip-encoded downloads.
    additional_headers = {}
    if compressed_encoding:
      additional_headers['accept-encoding'] = 'gzip'

    # TODO(b/161437904): Add decryption handling.

    if start_byte or end_byte:
      apitools_download.GetRange(additional_headers=additional_headers,
                                 start=start_byte,
                                 end=end_byte,
                                 use_chunks=False)
    else:
      apitools_download.StreamMedia(
          additional_headers=additional_headers,
          callback=_no_op_callback,
          finish_callback=_no_op_callback,
          use_chunks=False)
    return apitools_download.encoding

  # pylint: disable=unused-argument
  @_catch_http_error_raise_gcs_api_error()
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
    # S3 requires a string, but GCS uses an int for generation.
    generation = (
        int(cloud_resource.generation) if cloud_resource.generation else None)

    if not serialization_data:
      # New download.
      apitools_download = apitools_transfer.Download.FromStream(
          download_stream,
          auto_transfer=False,
          total_size=cloud_resource.size,
          num_retries=DEFAULT_NUM_RETRIES)
      apitools_download.bytes_http = transports.GetApitoolsTransport(
          response_encoding=None)
    else:
      # TODO(b/161437901): Handle resumed download.
      pass

    # TODO(b/161460749) Handle download retries.
    request = self.messages.StorageObjectsGetRequest(
        bucket=cloud_resource.bucket,
        object=cloud_resource.name,
        generation=generation)

    if download_strategy == cloud_api.DownloadStrategy.ONE_SHOT:
      return self._download_object(
          cloud_resource,
          download_stream,
          apitools_download,
          request,
          compressed_encoding=compressed_encoding,
          decryption_wrapper=decryption_wrapper,
          generation=generation,
          serialization_data=serialization_data,
          start_byte=start_byte,
          end_byte=end_byte)
    else:
      # TODO(b/161437901): Handle resumable download.
      pass

  # pylint: disable=unused-argument
  def _upload_object(self,
                     source_stream,
                     object_metadata,
                     request_config,
                     apitools_strategy=apitools_transfer.SIMPLE_UPLOAD,
                     progress_callback=None,
                     serialization_data=None,
                     total_size=0,
                     tracker_callback=None):
    # pylint: disable=g-doc-args
    """GCS-specific upload implementation. Adds args to Cloud API interface.

    Additional args:
      object_metadata (messages.Object): Apitools metadata for object to
          upload.
      apitools_strategy (str): SIMPLE_UPLOAD or RESUMABLE_UPLOAD constant in
          apitools.base.py.transfer.
      serialization_data (str): Implementation-specific JSON string of a dict
          containing serialization information for the download.
      total_size (int): Total size of the upload in bytes.
          If streaming, total size is None.
      tracker_callback (function): Callback that keeps track of upload progress.

    Returns:
      Uploaded object metadata in an ObjectResource.

    Raises:
      ValueError if an object can't be uploaded with the provided metadata.
    """
    predefined_acl = None
    if request_config.predefined_acl_string:
      predefined_acl = getattr(self.messages.StorageObjectsInsertRequest.
                               PredefinedAclValueValuesEnum,
                               request_config.predefined_acl_string)

    # TODO(b/160998052): Use encryption_wrapper to generate encryption headers.

    # Fresh upload. Prepare arguments.
    if not serialization_data:
      content_type = object_metadata.contentType

      if not content_type:
        content_type = DEFAULT_CONTENT_TYPE

      request = self.messages.StorageObjectsInsertRequest(
          bucket=object_metadata.bucket,
          object=object_metadata,
          ifGenerationMatch=request_config.precondition_generation_match,
          ifMetagenerationMatch=(
              request_config.precondition_metageneration_match),
          predefinedAcl=predefined_acl)

    if apitools_strategy == apitools_transfer.SIMPLE_UPLOAD:
      # One-shot upload.
      apitools_upload = apitools_transfer.Upload(
          source_stream,
          content_type,
          auto_transfer=True,
          chunksize=DEFAULT_UPLOAD_CHUNK_SIZE,
          gzip_encoded=request_config.gzip_encoded,
          num_retries=DEFAULT_NUM_RETRIES,
          total_size=request_config.size)

      result_object_metadata = self.client.objects.Insert(
          request, upload=apitools_upload)

      return _object_resource_from_metadata(result_object_metadata)
    else:
      # TODO(b/160998556): Implement resumable upload.
      pass

  @_catch_http_error_raise_gcs_api_error()
  def upload_object(self,
                    source_stream,
                    destination_resource,
                    progress_callback=None,
                    request_config=None):
    """See CloudApi class for function doc strings."""
    # Doing this as a default argument above can lead to unexpected bugs:
    # https://docs.python-guide.org/writing/gotchas/#mutable-default-arguments
    if isinstance(request_config, GcsRequestConfig):
      validated_request_config = request_config
    else:
      validated_request_config = cloud_api.convert_to_provider_request_config(
          request_config, GcsRequestConfig)
    # Calculate size, so apitools_transfer can pick optimal upload strategy.
    validated_request_config.size = os.path.getsize(source_stream.name)

    object_metadata = self.messages.Object(
        name=destination_resource.storage_url.object_name,
        bucket=destination_resource.storage_url.bucket_name,
        md5Hash=validated_request_config.md5_hash)

    return self._upload_object(
        source_stream,
        object_metadata,
        apitools_strategy=apitools_transfer.SIMPLE_UPLOAD,
        progress_callback=progress_callback,
        request_config=validated_request_config,
        serialization_data=None)
