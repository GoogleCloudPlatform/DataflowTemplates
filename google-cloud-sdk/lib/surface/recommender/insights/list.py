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
"""recommender API insights list command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager
from googlecloudsdk.api_lib.recommender import flag_utils as api_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.recommender import flags

DETAILED_HELP = {
    'EXAMPLES':
        """
        To list all insights for a billing account:

          $ {command} --project=project-name --location=global --insight-type=google.compute.firewall.Insight
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class List(base.ListCommand):
  r"""List insights for a cloud entity.

  This command will list all insights for a give cloud entity, location and
  insight type. Currently the following cloud_entity_types are supported,
  project, billing_account, folder and organization.
  """

  detailed_help = DETAILED_HELP

  @staticmethod
  def Args(parser):
    """Args is called by calliope to gather arguments for this command.

    Args:
      parser: An argparse parser that you can use to add arguments that go on
        the command line after this command.
    """
    flags.AddParentFlagsToParser(parser)
    parser.add_argument('--location', metavar='LOCATION', help='Location')
    parser.add_argument(
        '--insight-type',
        metavar='INSIGHT_TYPE',
        required=True,
        help='Insight type to list insights for')
    parser.display_info.AddFormat("""
        table(
          name.basename(): label=INSIGHT_ID,
          name.segment(3): label=LOCATION,
          name.segment(5): label=INSIGHT_TYPE,
          category: label=CATEGORY,
          stateInfo.state: label=INSIGHT_STATE,
          lastRefreshTime: label=LAST_REFRESH_TIME
        )
    """)

  def Run(self, args):
    """Run 'gcloud recommender insights list'.

    Args:
      args: argparse.Namespace, The arguments that this command was invoked
        with.

    Returns:
      The list of insights for this project.
    """
    recommender_service = api_utils.GetServiceFromArgs(
        args, is_insight_api=True)
    parent_ref = flags.GetParentFromFlags(
        args, is_list_api=True, is_insight_api=True)
    request = api_utils.GetListRequestFromArgs(
        args, parent_ref, is_insight_api=True)
    return list_pager.YieldFromList(
        recommender_service,
        request,
        batch_size_attribute='pageSize',
        batch_size=args.page_size,
        limit=args.limit,
        field='insights')
