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

"""Accesses the google.logging.v2 MetricsServiceV2 API."""

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

from google.api import monitored_resource_pb2
from google.protobuf import duration_pb2
from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2
from googlecloudsdk.third_party.logging_v2.gapic import enums
from googlecloudsdk.third_party.logging_v2.gapic import metrics_service_v2_client_config
from googlecloudsdk.third_party.logging_v2.gapic.transports import metrics_service_v2_grpc_transport
from googlecloudsdk.third_party.logging_v2.proto import log_entry_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_config_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_config_pb2_grpc
from googlecloudsdk.third_party.logging_v2.proto import logging_metrics_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_metrics_pb2_grpc
from googlecloudsdk.third_party.logging_v2.proto import logging_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_pb2_grpc



#_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution(
#    'google-cloud-logging',
#).version
_GAPIC_LIBRARY_VERSION = "52c89b6149379c0d59aff58bf89b47689cdf40b3"


class MetricsServiceV2Client(object):
    """Service for configuring logs-based metrics."""

    SERVICE_ADDRESS = 'logging.googleapis.com:443'
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = 'google.logging.v2.MetricsServiceV2'


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
            MetricsServiceV2Client: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(
            filename)
        kwargs['credentials'] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file


    @classmethod
    def log_metric_path(cls, project, metric):
        """Return a fully-qualified log_metric string."""
        return google.api_core.path_template.expand(
            'projects/{project}/metrics/{metric}',
            project=project,
            metric=metric,
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
            transport (Union[~.MetricsServiceV2GrpcTransport,
                    Callable[[~.Credentials, type], ~.MetricsServiceV2GrpcTransport]): A transport
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
            client_config = metrics_service_v2_client_config.config

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
                    default_class=metrics_service_v2_grpc_transport.MetricsServiceV2GrpcTransport,
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
            self.transport = metrics_service_v2_grpc_transport.MetricsServiceV2GrpcTransport(
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
    def update_log_metric(
            self,
            metric_name,
            metric,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Creates or updates a logs-based metric.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.MetricsServiceV2Client()
            >>>
            >>> metric_name = client.log_metric_path('[PROJECT]', '[METRIC]')
            >>>
            >>> # TODO: Initialize `metric`:
            >>> metric = {}
            >>>
            >>> response = client.update_log_metric(metric_name, metric)

        Args:
            metric_name (str): Required. The resource name of the metric to update:

                ::

                    "projects/[PROJECT_ID]/metrics/[METRIC_ID]"

                The updated metric must be provided in the request and it's ``name``
                field must be the same as ``[METRIC_ID]`` If the metric does not exist
                in ``[PROJECT_ID]``, then a new metric is created.
            metric (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogMetric]): Required. The updated metric.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'update_log_metric' not in self._inner_api_calls:
            self._inner_api_calls['update_log_metric'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_log_metric,
                default_retry=self._method_configs['UpdateLogMetric'].retry,
                default_timeout=self._method_configs['UpdateLogMetric'].timeout,
                client_info=self._client_info,
            )

        request = logging_metrics_pb2.UpdateLogMetricRequest(
            metric_name=metric_name,
            metric=metric,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('metric_name', metric_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['update_log_metric'](request, retry=retry, timeout=timeout, metadata=metadata)

    def delete_log_metric(
            self,
            metric_name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Deletes a logs-based metric.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.MetricsServiceV2Client()
            >>>
            >>> metric_name = client.log_metric_path('[PROJECT]', '[METRIC]')
            >>>
            >>> client.delete_log_metric(metric_name)

        Args:
            metric_name (str): Required. The resource name of the metric to delete:

                ::

                    "projects/[PROJECT_ID]/metrics/[METRIC_ID]"
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
        if 'delete_log_metric' not in self._inner_api_calls:
            self._inner_api_calls['delete_log_metric'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_log_metric,
                default_retry=self._method_configs['DeleteLogMetric'].retry,
                default_timeout=self._method_configs['DeleteLogMetric'].timeout,
                client_info=self._client_info,
            )

        request = logging_metrics_pb2.DeleteLogMetricRequest(
            metric_name=metric_name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('metric_name', metric_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        self._inner_api_calls['delete_log_metric'](request, retry=retry, timeout=timeout, metadata=metadata)

    def list_log_metrics(
            self,
            parent,
            page_size=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Lists logs-based metrics.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.MetricsServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_log_metrics(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_log_metrics(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Required. The name of the project containing the metrics:

                ::

                    "projects/[PROJECT_ID]"
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
            An iterable of :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric` instances.
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
        if 'list_log_metrics' not in self._inner_api_calls:
            self._inner_api_calls['list_log_metrics'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_log_metrics,
                default_retry=self._method_configs['ListLogMetrics'].retry,
                default_timeout=self._method_configs['ListLogMetrics'].timeout,
                client_info=self._client_info,
            )

        request = logging_metrics_pb2.ListLogMetricsRequest(
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
            method=functools.partial(self._inner_api_calls['list_log_metrics'], retry=retry, timeout=timeout, metadata=metadata),
            request=request,
            items_field='metrics',
            request_token_field='page_token',
            response_token_field='next_page_token',
        )
        return iterator

    def get_log_metric(
            self,
            metric_name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Gets a logs-based metric.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.MetricsServiceV2Client()
            >>>
            >>> metric_name = client.log_metric_path('[PROJECT]', '[METRIC]')
            >>>
            >>> response = client.get_log_metric(metric_name)

        Args:
            metric_name (str): Required. The resource name of the desired metric:

                ::

                    "projects/[PROJECT_ID]/metrics/[METRIC_ID]"
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_log_metric' not in self._inner_api_calls:
            self._inner_api_calls['get_log_metric'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_log_metric,
                default_retry=self._method_configs['GetLogMetric'].retry,
                default_timeout=self._method_configs['GetLogMetric'].timeout,
                client_info=self._client_info,
            )

        request = logging_metrics_pb2.GetLogMetricRequest(
            metric_name=metric_name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('metric_name', metric_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_log_metric'](request, retry=retry, timeout=timeout, metadata=metadata)

    def create_log_metric(
            self,
            parent,
            metric,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Creates a logs-based metric.

        Example:
            >>> from googlecloudsdk.third_party import logging_v2
            >>>
            >>> client = logging_v2.MetricsServiceV2Client()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # TODO: Initialize `metric`:
            >>> metric = {}
            >>>
            >>> response = client.create_log_metric(parent, metric)

        Args:
            parent (str): Required. The resource name of the project in which to create the
                metric:

                ::

                    "projects/[PROJECT_ID]"

                The new metric must be provided in the request.
            metric (Union[dict, ~googlecloudsdk.third_party.logging_v2.types.LogMetric]): Required. The new logs-based metric, which must not have an identifier that
                already exists.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~googlecloudsdk.third_party.logging_v2.types.LogMetric` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'create_log_metric' not in self._inner_api_calls:
            self._inner_api_calls['create_log_metric'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_log_metric,
                default_retry=self._method_configs['CreateLogMetric'].retry,
                default_timeout=self._method_configs['CreateLogMetric'].timeout,
                client_info=self._client_info,
            )

        request = logging_metrics_pb2.CreateLogMetricRequest(
            parent=parent,
            metric=metric,
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

        return self._inner_api_calls['create_log_metric'](request, retry=retry, timeout=timeout, metadata=metadata)
