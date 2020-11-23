# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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

"""'logging read' command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.logging import common
from googlecloudsdk.api_lib.logging import util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.logs import read as read_logs_lib


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA)
class Read(base.Command):
  """Read log entries."""

  @staticmethod
  def Args(parser):
    """Register flags for this command."""
    read_logs_lib.LogFilterPositionalArgs(parser)
    read_logs_lib.LoggingReadArgs(parser)
    util.AddParentArgs(parser, 'Read log entries')

  def _Run(self, args, is_alpha=False):
    filter_clauses = read_logs_lib.MakeTimestampFilters(args)
    filter_clauses += [args.log_filter] if args.log_filter else []
    parent = util.GetParentFromArgs(args)
    if is_alpha and args.IsSpecified('location'):
      parent = util.CreateResourceName(
          util.CreateResourceName(
              util.CreateResourceName(parent, 'locations', args.location),
              'buckets', args.bucket),
          'views', args.view)
    return common.FetchLogs(
        read_logs_lib.JoinFilters(filter_clauses, operator='AND') or None,
        order_by=args.order,
        limit=args.limit,
        parent=parent)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.
    Returns:
      The list of log entries.
    """
    return self._Run(args)

Read.detailed_help = {
    'DESCRIPTION': """\
        {command} reads log entries.  Log entries matching *log-filter* are
        returned in order of decreasing timestamps, most-recent entries first.
        If the log entries come from multiple logs, then entries from different
        logs might be intermingled in the results.
    """,
    'EXAMPLES': """\
        To read log entries from Google Compute Engine instances, run:

          $ {command} "resource.type=gce_instance"

        To read log entries with severity ERROR or higher, run:

          $ {command} "severity>=ERROR"

        To read log entries written in a specific time window, run:

          $ {command} 'timestamp<="2015-05-31T23:59:59Z" AND timestamp>="2015-05-31T00:00:00Z"'

        To read up to 10 log entries in your project's syslog log from Compute
        Engine instances containing payloads that include the word `SyncAddress`
        and format the output in `JSON` format, run:

          $ {command} "resource.type=gce_instance AND logName=projects/[PROJECT_ID]/logs/syslog AND textPayload:SyncAddress" --limit=10 --format=json

        To read a log entry from a folder, run:

          $ {command} "resource.type=global" --folder=[FOLDER_ID] --limit=1

        Detailed information about filters can be found at:
        [](https://cloud.google.com/logging/docs/view/advanced_filters)
    """,
}


# pylint: disable=missing-docstring
@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class ReadAlpha(Read):
  __doc__ = Read.__doc__

  @staticmethod
  def Args(parser):
    Read.Args(parser)
    view_group = parser.add_argument_group(
        help='These arguments are used in conjunction with the parent to '
        'construct a view resource.')
    view_group.add_argument(
        '--location', required=True, metavar='LOCATION',
        help='Location of the bucket. If this argument is provided then '
        '`--bucket` and `--view` must also be specified.')
    view_group.add_argument(
        '--bucket', required=True,
        help='Id of the bucket. If this argument is provided then '
        '`--location` and `--view` must also be specified.')
    view_group.add_argument(
        '--view', required=True,
        help='Id of the view. If this argument is provided then '
        '`--location` and `--bucket` must also be specified.')

  def Run(self, args):
    return self._Run(args, is_alpha=True)
