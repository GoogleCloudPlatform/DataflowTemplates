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


import google.api_core.grpc_helpers

from googlecloudsdk.third_party.logging_v2.proto import logging_pb2_grpc


class LoggingServiceV2GrpcTransport(object):
    """gRPC transport class providing stubs for
    google.logging.v2 LoggingServiceV2 API.

    The transport provides access to the raw gRPC stubs,
    which can be used to take advantage of advanced
    features of gRPC.
    """
    # The scopes needed to make gRPC calls to all of the methods defined
    # in this service.
    _OAUTH_SCOPES = (
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/cloud-platform.read-only',
        'https://www.googleapis.com/auth/logging.admin',
        'https://www.googleapis.com/auth/logging.read',
        'https://www.googleapis.com/auth/logging.write',
    )

    def __init__(self, channel=None, credentials=None,
                 address='logging.googleapis.com:443'):
        """Instantiate the transport class.

        Args:
            channel (grpc.Channel): A ``Channel`` instance through
                which to make calls. This argument is mutually exclusive
                with ``credentials``; providing both will raise an exception.
            credentials (google.auth.credentials.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            address (str): The address where the service is hosted.
        """
        # If both `channel` and `credentials` are specified, raise an
        # exception (channels come with credentials baked in already).
        if channel is not None and credentials is not None:
            raise ValueError(
                'The `channel` and `credentials` arguments are mutually '
                'exclusive.',
            )

        # Create the channel.
        if channel is None:
            channel = self.create_channel(
                address=address,
                credentials=credentials,
                options={
                    'grpc.max_send_message_length': -1,
                    'grpc.max_receive_message_length': -1,
                }.items(),
            )

        self._channel = channel

        # gRPC uses objects called "stubs" that are bound to the
        # channel and provide a basic method for each RPC.
        self._stubs = {
            'logging_service_v2_stub': logging_pb2_grpc.LoggingServiceV2Stub(channel),
        }


    @classmethod
    def create_channel(
                cls,
                address='logging.googleapis.com:443',
                credentials=None,
                **kwargs):
        """Create and return a gRPC channel object.

        Args:
            address (str): The host for the channel to use.
            credentials (~.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If
                none are specified, the client will attempt to ascertain
                the credentials from the environment.
            kwargs (dict): Keyword arguments, which are passed to the
                channel creation.

        Returns:
            grpc.Channel: A gRPC channel object.
        """
        return google.api_core.grpc_helpers.create_channel(
            address,
            credentials=credentials,
            scopes=cls._OAUTH_SCOPES,
            **kwargs
        )

    @property
    def channel(self):
        """The gRPC channel used by the transport.

        Returns:
            grpc.Channel: A gRPC channel object.
        """
        return self._channel

    @property
    def delete_log(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.delete_log`.

        Deletes all the log entries in a log. The log reappears if it receives new
        entries. Log entries written shortly before the delete operation might not
        be deleted. Entries received after the delete operation with a timestamp
        before the operation will be deleted.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].DeleteLog

    @property
    def list_log_entries(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.list_log_entries`.

        Lists log entries. Use this method to retrieve log entries that
        originated from a project/folder/organization/billing account. For ways
        to export log entries, see `Exporting
        Logs <https://cloud.google.com/logging/docs/export>`__.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].ListLogEntries

    @property
    def write_log_entries(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.write_log_entries`.

        Writes log entries to Logging. This API method is the
        only way to send log entries to Logging. This method
        is used, directly or indirectly, by the Logging agent
        (fluentd) and all logging libraries configured to use Logging.
        A single request may contain log entries for a maximum of 1000
        different resources (projects, organizations, billing accounts or
        folders)

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].WriteLogEntries

    @property
    def list_monitored_resource_descriptors(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.list_monitored_resource_descriptors`.

        Lists the descriptors for monitored resource types used by Logging.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].ListMonitoredResourceDescriptors

    @property
    def list_logs(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.list_logs`.

        Lists the logs in projects, organizations, folders, or billing accounts.
        Only logs that have entries are listed.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].ListLogs

    @property
    def tail_log_entries(self):
        """Return the gRPC stub for :meth:`LoggingServiceV2Client.tail_log_entries`.

        Streaming read of log entries as they are ingested. Until the stream is
        terminated, it will continue reading logs.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs['logging_service_v2_stub'].TailLogEntries
