# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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

"""'logging views list' command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.logging import util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base

DETAILED_HELP = {
    'DESCRIPTION': """
        Lists the views defined on a bucket.
    """,
    'EXAMPLES': """
     To list the views defined on a bucket, run:

        $ {command}
    """,
}


class List(base.ListCommand):
  """List the defined views."""

  @staticmethod
  def Args(parser):
    """Register flags for this command."""

    util.AddParentArgs(parser, 'List views')
    util.AddBucketLocationArg(
        parser, True, 'Location of the specified bucket')
    parser.add_argument(
        '--bucket', required=True,
        type=arg_parsers.RegexpValidator(r'.+', 'must be non-empty'),
        help='ID of bucket')
    parser.display_info.AddFormat(
        'table(name.segment(-1):label=VIEW_ID, filter, create_time, '
        'update_time)')

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
      command invocation.

    Yields:
      The list of views.
    """
    result = util.GetClient().projects_locations_buckets_views.List(
        util.GetMessages().LoggingProjectsLocationsBucketsViewsListRequest(
            parent=util.CreateResourceName(
                util.GetBucketLocationFromArgs(args), 'buckets', args.bucket)))
    for view in result.views:
      yield view

List.detailed_help = DETAILED_HELP
