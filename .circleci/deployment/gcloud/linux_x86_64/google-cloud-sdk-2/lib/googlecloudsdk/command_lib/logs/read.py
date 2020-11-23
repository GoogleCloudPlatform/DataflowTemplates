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
"""Helper functions for the log read commands."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime

from googlecloudsdk.api_lib.logging import util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base

_LOG_FILTER_HELP_TEXT = ('Filter expression that specifies the '
                         'log entries to return. A detailed guide on '
                         'basic and advanced filters can be found at: '
                         'https://cloud.google.com/logging/docs/view/'
                         'overview')


def LogFilterPositionalArgs(parser):
  """Add a log filter positional arg."""
  parser.add_argument('log_filter', help=_LOG_FILTER_HELP_TEXT, nargs='?')


def LogFilterArgs(parser):
  """Add a log filter arg."""
  parser.add_argument('--log-filter', help=_LOG_FILTER_HELP_TEXT)


def LoggingReadArgs(parser):
  """Arguments common to all log commands."""
  base.LIMIT_FLAG.AddToParser(parser)

  order_arg = base.ChoiceArgument(
      '--order',
      choices=('desc', 'asc'),
      required=False,
      default='desc',
      help_str='Ordering of returned log entries based on timestamp field.')
  order_arg.AddToParser(parser)

  parser.add_argument(
      '--freshness',
      type=arg_parsers.Duration(),
      help=('Return entries that are not older than this value. '
            'Works only with DESC ordering and filters without a timestamp. '
            'See $ gcloud topic datetimes for information on '
            'duration formats.'),
      default='1d')


def MakeTimestampFilters(args):
  """Create filters for the minimum log timestamp.

  This function creates an upper bound on the timestamp of log entries.
  A filter clause is returned if order == 'desc' and timestamp is not in
  the log-filter argument.

  Args:
    args: An argparse namespace object.

  Returns:
    A list of strings that are clauses in a Cloud Logging filter expression.
  """
  if (args.order == 'desc' and
      (not args.log_filter or 'timestamp' not in args.log_filter)):
    # Argparser returns freshness in seconds.
    freshness = datetime.timedelta(seconds=args.freshness)
    # Cloud Logging uses timestamps in UTC timezone.
    last_timestamp = datetime.datetime.utcnow() - freshness
    # Construct timestamp filter.
    return ['timestamp>="%s"' % util.FormatTimestamp(last_timestamp)]
  else:
    return []


def JoinFilters(clauses, operator='AND'):
  """Join the clauses with the operator.

  This function surrounds each clause with a set of parentheses and joins the
  clauses with the operator.

  Args:
    clauses: List of strings. Each string is a clause in the filter.
    operator: Logical operator used to join the clauses

  Returns:
    The clauses joined by the operator.
  """
  return (' ' + operator + ' ').join(clauses)
