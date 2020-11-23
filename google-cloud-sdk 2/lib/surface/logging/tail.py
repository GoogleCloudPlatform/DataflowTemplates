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
"""'logging 'tail' command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import logging

from googlecloudsdk.api_lib.logging import util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import gapic_util
from googlecloudsdk.core import log

from google.api_core import bidi


class NoGRPCInstalledError(exceptions.ToolException):
  """Unable to import grpc-based modules."""

  def __init__(self):
    super(NoGRPCInstalledError, self).__init__(
        'Please ensure the grpc module is installed.  Run:\n'
        'pip install grpcio')


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Tail(base.Command):
  """Tail log entries."""

  @staticmethod
  def Args(parser):
    """Registers flags for this command."""
    parser.add_argument(
        'log_filter',
        help=(
            'Filter expression that specifies the log entries to return. A '
            'detailed guide on the Logging query language can be found at: '
            'https://cloud.google.com/logging/docs/view/logging-query-language.'
            'overview'),
        nargs='?')
    parser.add_argument(
        '--buffer-window',
        required=False,
        type=arg_parsers.Duration(),
        help=(
            'The duration of time for which entries should be buffered for '
            'ordering before being returned. A longer buffer window helps to '
            'return logs in chronological order, but it also increases the '
            'latency from when entries are received by Cloud Logging to when '
            'they are returned. If unset, Cloud Logging will use 2s by '
            'default.'))

    view_group = parser.add_argument_group(
        help='These arguments are used in conjunction with the parent to '
        'construct a view resource.')
    view_group.add_argument(
        '--location',
        required=True,
        metavar='LOCATION',
        help='Location of the bucket. If this argument is provided, then '
        '`--bucket` and `--view` must also be specified.')
    view_group.add_argument(
        '--bucket',
        required=True,
        help='Id of the bucket. If this argument is provided, then '
        '`--location` and `--view` must also be specified.')
    view_group.add_argument(
        '--view',
        required=True,
        help='Id of the view. If this argument is provided, then '
        '`--location` and `--bucket` must also be specified.')

    util.AddParentArgs(parser, 'Tail log entries')

  def _Run(self, args):
    try:
      # pylint: disable=g-import-not-at-top
      from googlecloudsdk.api_lib.logging import tailing
      from googlecloudsdk.third_party.logging_v2.gapic.transports.logging_service_v2_grpc_transport import LoggingServiceV2GrpcTransport
      # pylint: enable=g-import-not-at-top
    except ImportError:
      raise NoGRPCInstalledError()

    log.err.Print('Initializing tail session.')
    parent = util.GetParentFromArgs(args)
    if args.IsSpecified('location'):
      parent = util.CreateResourceName(
          util.CreateResourceName(
              util.CreateResourceName(parent, 'locations', args.location),
              'buckets', args.bucket), 'views', args.view)
    buffer_window_seconds = None
    if args.buffer_window:
      if args.buffer_window < 0 or args.buffer_window > 60:
        log.error('The buffer window must be set between 0s and 1m.')
      buffer_window_seconds = args.buffer_window
    transport = LoggingServiceV2GrpcTransport(
        credentials=gapic_util.StoredCredentials(),
        address='logging.googleapis.com:443')

    # By default and up to the INFO verbosity, all console output is included in
    # the log file. When tailing logs, coarse filters could cause very large
    # files. So, we limit the log file to WARNING logs and above.
    log.SetLogFileVerbosity(logging.WARNING)
    return tailing.TailLogs(
        bidi.BidiRpc(transport.tail_log_entries), [parent],
        args.log_filter or '',
        buffer_window_seconds=buffer_window_seconds)

  def Run(self, args):
    """Gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      An iterator of log entries.
    """
    return self._Run(args)


Tail.detailed_help = {
    'DESCRIPTION': """
         {command} streams newly received log entries. Log entries matching
         *log-filter* are returned in the order that they were received by
         Cloud Logging. If the log entries come from multiple logs, then
         entries from different logs might be intermingled in the results. To
         help return log entries in order, use `--buffer-window`.

         Before you can use {command}, you must complete the installation
         instructions at
         [Live tailing log entries](https://cloud.google.com/logging/docs/reference/tools/gcloud-logging#live-tailing).

         For the quotas and limits associated with {command},
         see [Logging API quotas and limits](https://cloud.google.com/logging/quotas#api-limits).
    """,
    'EXAMPLES':
        """\
        To stream log entries from Google Compute Engine instances, run:

          $ {command} "resource.type=gce_instance"

        To stream log entries with severity ERROR or higher, run:

          $ {command} "severity>=ERROR"

        To stream log entries with severity ERROR but only output the timestamps
        and instance IDs, run:

          $ {command} "severity>=ERROR" --format="default(timestamp,resource[\"labels\"][\"instance_id\"])"

        To stream with minimal latency but potentially incorrect ordering:

          $ {command} "resource.type=gce_instance" --buffer-window=0s

        To stream up to 10 log entries in your project's syslog log from Compute
        Engine instances containing payloads that include the word `SyncAddress`
        and format the output in `JSON` format, run:

          $ {command} "resource.type=gce_instance AND log_id(syslog) AND textPayload:SyncAddress" --limit=10 --format=json

        To stream a log entry from a folder, run:

          $ {command} "resource.type=global" --folder=[FOLDER_ID] --limit=1

        Detailed information about filters can be found at:
        https://cloud.google.com/logging/docs/view/logging-query-language
    """,
}
