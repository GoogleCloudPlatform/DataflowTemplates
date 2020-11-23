# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Accesses the google.logging.v2 ConfigServiceV2 API."""

import functools
import pkg_resources
import warnings

from google.oauth2 import service_account
import google.api_core.client_options
import google.api_core.gapic_v1.client_info
import google.api_core.gapic_v1.config
import google.api_core.gapic_v1.method
import google.api_core.gapic_v1.routing_header
import google.api_core.grpc_helpers
import google.api_core.page_iterator
import google.api_core.path_template
import grpc

from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2
from googlecloudsdk.third_party.logging_v2.gapic import config_service_v2_client_config
from googlecloudsdk.third_party.logging_v2.gapic import enums
from googlecloudsdk.third_party.logging_v2.gapic.transports import config_service_v2_grpc_transport
from googlecloudsdk.third_party.logging_v2.proto import logging_config_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_config_pb2_grpc



#_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution(
#    'google-cloud-logging',
#).version
_GAPIC_LIBRARY_VERSION = "52c89b6149379c0d59aff58bf89b47689cdf40b3"


class ConfigServiceV2Client(object):
    """Service for configuring sinks used to route log entries."""

    SERVICE_ADDRESS = 'logging.googleapis.com:443'
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = 'google.logging.v2.ConfigServiceV2'


    @classmethod
    def from_service_account_file(cls, filename, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
        file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            ConfigServiceV2Client: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(
            filename)
        kwargs['credentials'] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file


    @classmethod
    def billing_account_path(cls, billing_account):
        """Return a fully-qualified billing_account string."""
        return google.api_core.path_template.expand(
            'billingAccounts/{billing_account}',
            billing_account=billing_account,
        )

    @classmethod
    def billing_account_location_path(cls, billing_account, location):
        """Return a fully-qualified billing_account_location string."""
        return google.api_core.path_template.expand(
            'billingAccounts/{billing_account}/locations/{location}',
            billing_account=billing_account,
            location=location,
        )

    @classmethod
    def cmek_settings_path(cls, project):
        """Return a fully-qualified cmek_settings string."""
        return google.api_core.path_template.expand(
            'projects/{project}/cmekSettings',
            project=project,
        )

    @classmethod
    def folder_path(cls, folder):
        """Return a fully-qualified folder string."""
        return google.api_core.path_template.expand(
            'folders/{folder}',
            folder=folder,
        )

    @classmethod
    def folder_location_path(cls, folder, location):
        """Return a fully-qualified folder_location string."""
        return google.api_core.path_template.expand(
            'folders/{folder}/locations/{location}',
            folder=folder,
            location=location,
        )

    @classmethod
    def location_path(cls, project, location):
        """Return a fully-qualified location string."""
        return google.api_core.path_template.expand(
            'projects/{project}/locations/{location}',
            project=project,
            location=location,
        )

    @classmethod
    def log_bucket_path(cls, project, location, bucket):
        """Return a fully-qualified log_bucket string."""
        return google.api_core.path_template.expand(
            'projects/{project}/locations/{location}/buckets/{bucket}',
            project=project,
            location=location,
            bucket=bucket,
        )

    @classmethod
    def log_exclusion_path(cls, project, exclusion):
        """Return a fully-qualified log_exclusion string."""
        return google.api_core.path_template.expand(
            'projects/{project}/exclusions/{exclusion}',
            project=project,
            exclusion=exclusion,
        )

    @classmethod
    def log_sink_path(cls, project, sink):
        """Return a fully-qualified log_sink string."""
        return google.api_core.path_template.expand(
            'projects/{project}/sinks/{sink}',
            project=project,
            sink=sink,
        )

    @classmethod
    def organization_path(cls, organization):
        """Return a fully-qualified organization string."""
        return google.api_core.path_template.expand(
            'organizations/{organization}',
            organization=organization,
        )

    @classmethod
    def organization_location_path(cls, organization, location):
        """Return a fully-qualified organization_location string."""
        return google.api_core.path_template.expand(
            'organizations/{organization}/locations/{location}',
            organization=organization,
            location=location,
        )

    @classmethod
    def project_path(cls, project):
        """Return a fully-qualified project string."""
        return google.api_core.path_template.expand(
            'projects/{project}',
            project=project,
        )

    def __init__(self, transport=None, channel=None, credentials=None,
            client_config=None, client_info=None, client_options=None):
        """Constructor.

        Args:
            transport (Union[~.ConfigServiceV2GrpcTransport,
                    Callable[[~.Credentials, type], ~.ConfigServiceV2GrpcTransport]): A transport
                instance, responsible for actually making the API calls.
                The default transport uses the gRPC protocol.
                This argument may also be a callable which returns a
                transport instance. Callables will be sent the credentials
                as the first argument and the default transport class as
                the second argument.
            channel (grpc.Channel): DEPRECATED. A ``Channel`` instance
                through which to make calls. This argument is mutually exclusive
                with ``credentials``; providing both will raise an exception.
            credentials (google.auth.credentials.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
                This argument is mutually exclusive with providing a
                transport instance to ``transport``; doing so will raise
                an exception.
            client_config (dict): DEPRECATED. A dictionary of call options for
                each method. If not specified, the default configuration is used.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.
            client_options (Union[dict, google.api_core.client_options.ClientOptions]):
                Client options used to set user options on the client. API Endpoint
                should be set through client_options.
        """
        # Raise deprecation warnings for things we want to go away.
        if client_config is not None:
            warnings.warn('The `client_config` argument is deprecated.',
                          PendingDeprecationWarning, stacklevel=2)
        else:
            client_config = config_service_v2_client_config.config

        if channel:
            warnings.warn('The `channel` argument is deprecated; use '
                          '`transport` instead.',
                          PendingDeprecationWarning, stacklevel=2)

        api_endpoint = self.SERVICE_ADDRESS
        if client_options:
            if type(client_options) == dict:
                client_options = google.api_core.client_options.from_dict(client_options)
            if client_options.api_endpoint:
                api_endpoint = client_options.api_endpoint

        # Instantiate the transport.
        # The transport is responsible for handling serialization and
        # deserialization and actually sending data to the service.
        if transport:
            if callable(transport):
                self.transport = transport(
                    credentials=credentials,
                    default_class=config_service_v2_grpc_transport.ConfigServiceV2GrpcTransport,
                    address=api_endpoint,
                )
            else:
                if credentials:
                    raise ValueError(
                        'Received both a transport instance and '
                        'credentials; these are mutually exclusive.'
                    )
                self.transport = transport
        else:
            self.transport = config_service_v2_grpc_transport.ConfigServiceV2GrpcTransport(
                address=api_endpoint,
                channel=channel,
                credentials=credentials,
            )

        if client_info is None:
            client_info = google.api_core.gapic_v1.client_info.ClientInfo(
                gapic_version=_GAPIC_LIBRARY_VERSION,
            )
        else:
            client_info.gapic_version = _GAPIC_LIBRARY_VERSION
        self._client_info = client_info

        # Parse out the default settings for retry and timeout for each RPC
        # from the client configuration.
        # (Ordinarily, these are the defaults specified in the `*_config.py`
        # file next to this one.)
        self._method_configs = google.api_core.gapic_v1.config.parse_method_configs(
            client_config['interfaces'][self._INTERFACE_NAME],
        )

        # Save a dictionary of cached API call functions.
        # These are the actual callables which invoke the proper
        # transport methods, wrapped with `wrap_method` to add retry,
        # timeout, and the like.
        self._inner_api_calls = {}

    # Service calls
    def delete_sink(
            self,
            sink_name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Deletes a sink. If the sink has a unique ``writer_identity``, then
        that service account is also deleted.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `sink_name`:
            >>> sink_name = ''
            >>>
            >>> client.delete_sink(sink_name)

        Args:
            sink_name (str): Required. The full resource name of the sink to delete, including
                the parent resource and the sink identifier:

                ::

                    "projects/[PROJECT_ID]/sinks/[SINK_ID]"
                    "organizations/[ORGANIZATION_ID]/sinks/[SINK_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/sinks/[SINK_ID]"
                    "folders/[FOLDER_ID]/sinks/[SINK_ID]"

                Example: ``"projects/my-project-id/sinks/my-sink-id"``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'delete_sink' not in self._inner_api_calls:
            self._inner_api_calls['delete_sink'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_sink,
                default_retry=self._method_configs['DeleteSink'].retry,
                default_timeout=self._method_configs['DeleteSink'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.DeleteSinkRequest(
            sink_name=sink_name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('sink_name', sink_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        self._inner_api_calls['delete_sink'](request, retry=retry, timeout=timeout, metadata=metadata)

    def update_sink(
            self,
            sink_name,
            sink,
            unique_writer_identity=None,
            update_mask=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Updates a sink. This method replaces the following fields in the
        existing sink with values from the new sink: ``destination``, and
        ``filter``.

        The updated sink might also have a new ``writer_identity``; see the
        ``unique_writer_identity`` field.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `sink_name`:
            >>> sink_name = ''
            >>>
            >>> # TODO: Initialize `sink`:
            >>> sink = {}
            >>>
            >>> response = client.update_sink(sink_name, sink)

        Args:
            sink_name (str): Required. The full resource name of the sink to update, including
                the parent resource and the sink identifier:

                ::

                    "projects/[PROJECT_ID]/sinks/[SINK_ID]"
                    "organizations/[ORGANIZATION_ID]/sinks/[SINK_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/sinks/[SINK_ID]"
                    "folders/[FOLDER_ID]/sinks/[SINK_ID]"

                Example: ``"projects/my-project-id/sinks/my-sink-id"``.
            sink (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogSink]): Required. The updated sink, whose name is the same identifier that
                appears as part of ``sink_name``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink`
            unique_writer_identity (bool): Optional. See ``sinks.create`` for a description of this field. When
                updating a sink, the effect of this field on the value of
                ``writer_identity`` in the updated sink depends on both the old and new
                values of this field:

                -  If the old and new values of this field are both false or both true,
                   then there is no change to the sink's ``writer_identity``.
                -  If the old value is false and the new value is true, then
                   ``writer_identity`` is changed to a unique service account.
                -  It is an error if the old value is true and the new value is set to
                   false or defaulted to false.
            update_mask (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.FieldMask]): Optional. Field mask that specifies the fields in ``sink`` that need
                an update. A sink field will be overwritten if, and only if, it is in
                the update mask. ``name`` and output only fields cannot be updated.

                An empty updateMask is temporarily treated as using the following mask
                for backwards compatibility purposes: destination,filter,includeChildren
                At some point in the future, behavior will be removed and specifying an
                empty updateMask will be an error.

                For a detailed ``FieldMask`` definition, see
                https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask

                Example: ``updateMask=filter``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'update_sink' not in self._inner_api_calls:
            self._inner_api_calls['update_sink'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_sink,
                default_retry=self._method_configs['UpdateSink'].retry,
                default_timeout=self._method_configs['UpdateSink'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.UpdateSinkRequest(
            sink_name=sink_name,
            sink=sink,
            unique_writer_identity=unique_writer_identity,
            update_mask=update_mask,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('sink_name', sink_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['update_sink'](request, retry=retry, timeout=timeout, metadata=metadata)

    def delete_exclusion(
            self,
            name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Deletes an exclusion.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> client.delete_exclusion(name)

        Args:
            name (str): Required. The resource name of an existing exclusion to delete:

                ::

                    "projects/[PROJECT_ID]/exclusions/[EXCLUSION_ID]"
                    "organizations/[ORGANIZATION_ID]/exclusions/[EXCLUSION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/exclusions/[EXCLUSION_ID]"
                    "folders/[FOLDER_ID]/exclusions/[EXCLUSION_ID]"

                Example: ``"projects/my-project-id/exclusions/my-exclusion-id"``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'delete_exclusion' not in self._inner_api_calls:
            self._inner_api_calls['delete_exclusion'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_exclusion,
                default_retry=self._method_configs['DeleteExclusion'].retry,
                default_timeout=self._method_configs['DeleteExclusion'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.DeleteExclusionRequest(
            name=name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        self._inner_api_calls['delete_exclusion'](request, retry=retry, timeout=timeout, metadata=metadata)

    def list_buckets(
            self,
            parent,
            page_size=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Lists buckets (Beta).

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> parent = client.organization_location_path('[ORGANIZATION]', '[LOCATION]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_buckets(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_buckets(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Required. The parent resource whose buckets are to be listed:

                ::

                    "projects/[PROJECT_ID]/locations/[LOCATION_ID]"
                    "organizations/[ORGANIZATION_ID]/locations/[LOCATION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/locations/[LOCATION_ID]"
                    "folders/[FOLDER_ID]/locations/[LOCATION_ID]"

                Note: The locations portion of the resource must be specified, but
                supplying the character ``-`` in place of [LOCATION_ID] will return all
                buckets.
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.page_iterator.PageIterator` instance.
            An iterable of :class:`~googlecloudsdk.third_party.logging_v2.types.LogBucket` instances.
            You can also iterate over the pages of the response
            using its `pages` property.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'list_buckets' not in self._inner_api_calls:
            self._inner_api_calls['list_buckets'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_buckets,
                default_retry=self._method_configs['ListBuckets'].retry,
                default_timeout=self._method_configs['ListBuckets'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.ListBucketsRequest(
            parent=parent,
            page_size=page_size,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('parent', parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(self._inner_api_calls['list_buckets'], retry=retry, timeout=timeout, metadata=metadata),
            request=request,
            items_field='buckets',
            request_token_field='page_token',
            response_token_field='next_page_token',
        )
        return iterator

    def get_bucket(
            self,
            name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Gets a bucket (Beta).

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> response = client.get_bucket(name)

        Args:
            name (str): Required. The resource name of the bucket:

                ::

                    "projects/[PROJECT_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "organizations/[ORGANIZATION_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "folders/[FOLDER_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"

                Example:
                ``"projects/my-project-id/locations/my-location/buckets/my-bucket-id"``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogBucket` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_bucket' not in self._inner_api_calls:
            self._inner_api_calls['get_bucket'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_bucket,
                default_retry=self._method_configs['GetBucket'].retry,
                default_timeout=self._method_configs['GetBucket'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.GetBucketRequest(
            name=name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_bucket'](request, retry=retry, timeout=timeout, metadata=metadata)

    def update_bucket(
            self,
            name,
            bucket,
            update_mask,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Updates a bucket. This method replaces the following fields in the
        existing bucket with values from the new bucket: ``retention_period``

        If the retention period is decreased and the bucket is locked,
        FAILED_PRECONDITION will be returned.

        If the bucket has a LifecycleState of DELETE_REQUESTED,
        FAILED_PRECONDITION will be returned.

        A buckets region may not be modified after it is created. This method is
        in Beta.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> # TODO: Initialize `bucket`:
            >>> bucket = {}
            >>>
            >>> # TODO: Initialize `update_mask`:
            >>> update_mask = {}
            >>>
            >>> response = client.update_bucket(name, bucket, update_mask)

        Args:
            name (str): Required. The full resource name of the bucket to update.

                ::

                    "projects/[PROJECT_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "organizations/[ORGANIZATION_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"
                    "folders/[FOLDER_ID]/locations/[LOCATION_ID]/buckets/[BUCKET_ID]"

                Example:
                ``"projects/my-project-id/locations/my-location/buckets/my-bucket-id"``.
                Also requires permission "resourcemanager.projects.updateLiens" to set
                the locked property
            bucket (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogBucket]): Required. The updated bucket.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogBucket`
            update_mask (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.FieldMask]): Required. Field mask that specifies the fields in ``bucket`` that
                need an update. A bucket field will be overwritten if, and only if, it
                is in the update mask. ``name`` and output only fields cannot be
                updated.

                For a detailed ``FieldMask`` definition, see
                https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask

                Example: ``updateMask=retention_days``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogBucket` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'update_bucket' not in self._inner_api_calls:
            self._inner_api_calls['update_bucket'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_bucket,
                default_retry=self._method_configs['UpdateBucket'].retry,
                default_timeout=self._method_configs['UpdateBucket'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.UpdateBucketRequest(
            name=name,
            bucket=bucket,
            update_mask=update_mask,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['update_bucket'](request, retry=retry, timeout=timeout, metadata=metadata)

    def list_sinks(
            self,
            parent,
            page_size=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Lists sinks.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_sinks(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_sinks(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Required. The parent resource whose sinks are to be listed:

                ::

                    "projects/[PROJECT_ID]"
                    "organizations/[ORGANIZATION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]"
                    "folders/[FOLDER_ID]"
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.page_iterator.PageIterator` instance.
            An iterable of :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink` instances.
            You can also iterate over the pages of the response
            using its `pages` property.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'list_sinks' not in self._inner_api_calls:
            self._inner_api_calls['list_sinks'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_sinks,
                default_retry=self._method_configs['ListSinks'].retry,
                default_timeout=self._method_configs['ListSinks'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.ListSinksRequest(
            parent=parent,
            page_size=page_size,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('parent', parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(self._inner_api_calls['list_sinks'], retry=retry, timeout=timeout, metadata=metadata),
            request=request,
            items_field='sinks',
            request_token_field='page_token',
            response_token_field='next_page_token',
        )
        return iterator

    def get_sink(
            self,
            sink_name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Gets a sink.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `sink_name`:
            >>> sink_name = ''
            >>>
            >>> response = client.get_sink(sink_name)

        Args:
            sink_name (str): Required. The resource name of the sink:

                ::

                    "projects/[PROJECT_ID]/sinks/[SINK_ID]"
                    "organizations/[ORGANIZATION_ID]/sinks/[SINK_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/sinks/[SINK_ID]"
                    "folders/[FOLDER_ID]/sinks/[SINK_ID]"

                Example: ``"projects/my-project-id/sinks/my-sink-id"``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_sink' not in self._inner_api_calls:
            self._inner_api_calls['get_sink'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_sink,
                default_retry=self._method_configs['GetSink'].retry,
                default_timeout=self._method_configs['GetSink'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.GetSinkRequest(
            sink_name=sink_name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('sink_name', sink_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_sink'](request, retry=retry, timeout=timeout, metadata=metadata)

    def create_sink(
            self,
            parent,
            sink,
            unique_writer_identity=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Creates a sink that exports specified log entries to a destination.
        The export of newly-ingested log entries begins immediately, unless the
        sink's ``writer_identity`` is not permitted to write to the destination.
        A sink can export log entries only from the resource owning the sink.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # TODO: Initialize `sink`:
            >>> sink = {}
            >>>
            >>> response = client.create_sink(parent, sink)

        Args:
            parent (str): Required. The resource in which to create the sink:

                ::

                    "projects/[PROJECT_ID]"
                    "organizations/[ORGANIZATION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]"
                    "folders/[FOLDER_ID]"

                Examples: ``"projects/my-logging-project"``,
                ``"organizations/123456789"``.
            sink (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogSink]): Required. The new sink, whose ``name`` parameter is a sink
                identifier that is not already in use.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink`
            unique_writer_identity (bool): Optional. Determines the kind of IAM identity returned as
                ``writer_identity`` in the new sink. If this value is omitted or set to
                false, and if the sink's parent is a project, then the value returned as
                ``writer_identity`` is the same group or service account used by Logging
                before the addition of writer identities to this API. The sink's
                destination must be in the same project as the sink itself.

                If this field is set to true, or if the sink is owned by a non-project
                resource such as an organization, then the value of ``writer_identity``
                will be a unique service account used only for exports from the new
                sink. For more information, see ``writer_identity`` in ``LogSink``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogSink` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'create_sink' not in self._inner_api_calls:
            self._inner_api_calls['create_sink'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_sink,
                default_retry=self._method_configs['CreateSink'].retry,
                default_timeout=self._method_configs['CreateSink'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.CreateSinkRequest(
            parent=parent,
            sink=sink,
            unique_writer_identity=unique_writer_identity,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('parent', parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['create_sink'](request, retry=retry, timeout=timeout, metadata=metadata)

    def list_exclusions(
            self,
            parent,
            page_size=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Lists all the exclusions in a parent resource.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_exclusions(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_exclusions(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Required. The parent resource whose exclusions are to be listed.

                ::

                    "projects/[PROJECT_ID]"
                    "organizations/[ORGANIZATION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]"
                    "folders/[FOLDER_ID]"
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.page_iterator.PageIterator` instance.
            An iterable of :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion` instances.
            You can also iterate over the pages of the response
            using its `pages` property.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'list_exclusions' not in self._inner_api_calls:
            self._inner_api_calls['list_exclusions'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_exclusions,
                default_retry=self._method_configs['ListExclusions'].retry,
                default_timeout=self._method_configs['ListExclusions'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.ListExclusionsRequest(
            parent=parent,
            page_size=page_size,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('parent', parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(self._inner_api_calls['list_exclusions'], retry=retry, timeout=timeout, metadata=metadata),
            request=request,
            items_field='exclusions',
            request_token_field='page_token',
            response_token_field='next_page_token',
        )
        return iterator

    def get_exclusion(
            self,
            name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Gets the description of an exclusion.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> response = client.get_exclusion(name)

        Args:
            name (str): Required. The resource name of an existing exclusion:

                ::

                    "projects/[PROJECT_ID]/exclusions/[EXCLUSION_ID]"
                    "organizations/[ORGANIZATION_ID]/exclusions/[EXCLUSION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/exclusions/[EXCLUSION_ID]"
                    "folders/[FOLDER_ID]/exclusions/[EXCLUSION_ID]"

                Example: ``"projects/my-project-id/exclusions/my-exclusion-id"``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_exclusion' not in self._inner_api_calls:
            self._inner_api_calls['get_exclusion'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_exclusion,
                default_retry=self._method_configs['GetExclusion'].retry,
                default_timeout=self._method_configs['GetExclusion'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.GetExclusionRequest(
            name=name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_exclusion'](request, retry=retry, timeout=timeout, metadata=metadata)

    def create_exclusion(
            self,
            parent,
            exclusion,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Creates a new exclusion in a specified parent resource.
        Only log entries belonging to that resource can be excluded.
        You can have up to 10 exclusions in a resource.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # TODO: Initialize `exclusion`:
            >>> exclusion = {}
            >>>
            >>> response = client.create_exclusion(parent, exclusion)

        Args:
            parent (str): Required. The parent resource in which to create the exclusion:

                ::

                    "projects/[PROJECT_ID]"
                    "organizations/[ORGANIZATION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]"
                    "folders/[FOLDER_ID]"

                Examples: ``"projects/my-logging-project"``,
                ``"organizations/123456789"``.
            exclusion (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogExclusion]): Required. The new exclusion, whose ``name`` parameter is an
                exclusion name that is not already used in the parent resource.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'create_exclusion' not in self._inner_api_calls:
            self._inner_api_calls['create_exclusion'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_exclusion,
                default_retry=self._method_configs['CreateExclusion'].retry,
                default_timeout=self._method_configs['CreateExclusion'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.CreateExclusionRequest(
            parent=parent,
            exclusion=exclusion,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('parent', parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['create_exclusion'](request, retry=retry, timeout=timeout, metadata=metadata)

    def update_exclusion(
            self,
            name,
            exclusion,
            update_mask,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Changes one or more properties of an existing exclusion.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> # TODO: Initialize `exclusion`:
            >>> exclusion = {}
            >>>
            >>> # TODO: Initialize `update_mask`:
            >>> update_mask = {}
            >>>
            >>> response = client.update_exclusion(name, exclusion, update_mask)

        Args:
            name (str): Required. The resource name of the exclusion to update:

                ::

                    "projects/[PROJECT_ID]/exclusions/[EXCLUSION_ID]"
                    "organizations/[ORGANIZATION_ID]/exclusions/[EXCLUSION_ID]"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/exclusions/[EXCLUSION_ID]"
                    "folders/[FOLDER_ID]/exclusions/[EXCLUSION_ID]"

                Example: ``"projects/my-project-id/exclusions/my-exclusion-id"``.
            exclusion (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogExclusion]): Required. New values for the existing exclusion. Only the fields
                specified in ``update_mask`` are relevant.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion`
            update_mask (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.FieldMask]): Required. A non-empty list of fields to change in the existing
                exclusion. New values for the fields are taken from the corresponding
                fields in the ``LogExclusion`` included in this request. Fields not
                mentioned in ``update_mask`` are not changed and are ignored in the
                request.

                For example, to change the filter and description of an exclusion,
                specify an ``update_mask`` of ``"filter,description"``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogExclusion` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'update_exclusion' not in self._inner_api_calls:
            self._inner_api_calls['update_exclusion'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_exclusion,
                default_retry=self._method_configs['UpdateExclusion'].retry,
                default_timeout=self._method_configs['UpdateExclusion'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.UpdateExclusionRequest(
            name=name,
            exclusion=exclusion,
            update_mask=update_mask,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['update_exclusion'](request, retry=retry, timeout=timeout, metadata=metadata)

    def get_cmek_settings(
            self,
            name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Gets the Logs Router CMEK settings for the given resource.

        Note: CMEK for the Logs Router can currently only be configured for GCP
        organizations. Once configured, it applies to all projects and folders
        in the GCP organization.

        See `Enabling CMEK for Logs
        Router <https://cloud.google.com/logging/docs/routing/managed-encryption>`__
        for more information.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> response = client.get_cmek_settings(name)

        Args:
            name (str): Required. The resource for which to retrieve CMEK settings.

                ::

                    "projects/[PROJECT_ID]/cmekSettings"
                    "organizations/[ORGANIZATION_ID]/cmekSettings"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/cmekSettings"
                    "folders/[FOLDER_ID]/cmekSettings"

                Example: ``"organizations/12345/cmekSettings"``.

                Note: CMEK for the Logs Router can currently only be configured for GCP
                organizations. Once configured, it applies to all projects and folders
                in the GCP organization.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.CmekSettings` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_cmek_settings' not in self._inner_api_calls:
            self._inner_api_calls['get_cmek_settings'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_cmek_settings,
                default_retry=self._method_configs['GetCmekSettings'].retry,
                default_timeout=self._method_configs['GetCmekSettings'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.GetCmekSettingsRequest(
            name=name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_cmek_settings'](request, retry=retry, timeout=timeout, metadata=metadata)

    def update_cmek_settings(
            self,
            name,
            cmek_settings,
            update_mask=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Updates the Logs Router CMEK settings for the given resource.

        Note: CMEK for the Logs Router can currently only be configured for GCP
        organizations. Once configured, it applies to all projects and folders
        in the GCP organization.

        ``UpdateCmekSettings`` will fail if 1) ``kms_key_name`` is invalid, or
        2) the associated service account does not have the required
        ``roles/cloudkms.cryptoKeyEncrypterDecrypter`` role assigned for the
        key, or 3) access to the key is disabled.

        See `Enabling CMEK for Logs
        Router <https://cloud.google.com/logging/docs/routing/managed-encryption>`__
        for more information.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.ConfigServiceV2Client()
            >>>
            >>> # TODO: Initialize `name`:
            >>> name = ''
            >>>
            >>> # TODO: Initialize `cmek_settings`:
            >>> cmek_settings = {}
            >>>
            >>> response = client.update_cmek_settings(name, cmek_settings)

        Args:
            name (str): Required. The resource name for the CMEK settings to update.

                ::

                    "projects/[PROJECT_ID]/cmekSettings"
                    "organizations/[ORGANIZATION_ID]/cmekSettings"
                    "billingAccounts/[BILLING_ACCOUNT_ID]/cmekSettings"
                    "folders/[FOLDER_ID]/cmekSettings"

                Example: ``"organizations/12345/cmekSettings"``.

                Note: CMEK for the Logs Router can currently only be configured for GCP
                organizations. Once configured, it applies to all projects and folders
                in the GCP organization.
            cmek_settings (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.CmekSettings]): Required. The CMEK settings to update.

                See `Enabling CMEK for Logs
                Router <https://cloud.google.com/logging/docs/routing/managed-encryption>`__
                for more information.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.CmekSettings`
            update_mask (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.FieldMask]): Optional. Field mask identifying which fields from ``cmek_settings``
                should be updated. A field will be overwritten if and only if it is in
                the update mask. Output only fields cannot be updated.

                See ``FieldMask`` for more information.

                Example: ``"updateMask=kmsKeyName"``

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.CmekSettings` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'update_cmek_settings' not in self._inner_api_calls:
            self._inner_api_calls['update_cmek_settings'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_cmek_settings,
                default_retry=self._method_configs['UpdateCmekSettings'].retry,
                default_timeout=self._method_configs['UpdateCmekSettings'].timeout,
                client_info=self._client_info,
            )

        request = logging_config_pb2.UpdateCmekSettingsRequest(
            name=name,
            cmek_settings=cmek_settings,
            update_mask=update_mask,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('name', name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['update_cmek_settings'](request, retry=retry, timeout=timeout, metadata=metadata)
