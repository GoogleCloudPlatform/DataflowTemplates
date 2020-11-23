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
"""recommender API recommendations Mark Active command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.recommender import flag_utils as api_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.recommender import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class MarkActive(base.Command):
  r"""Mark Active operations for a recommendation.

     Mark a recommendation's state as ACTIVE. Can be applied to recommendations
     in DISMISSED state. This currently supports the following parent entities:
     project, billing account, folder, and organization.

     ## EXAMPLES
     To mark a recommenation as ACTIVE:

      $ {command} RECOMMENDATION_ID --project=${PROJECT} --location=${LOCATION}
      --recommender=${RECOMMENDER} --etag=etag
  """

  @staticmethod
  def Args(parser):
    """Args is called by calliope to gather arguments for this command.

    Args:
      parser: An argparse parser that you can use to add arguments that go on
        the command line after this command.
    """
    flags.AddParentFlagsToParser(parser)
    parser.add_argument(
        'RECOMMENDATION',
        type=str,
        help='Recommendation id which will be marked as active',
    )
    parser.add_argument('--location', metavar='LOCATION', help='Location')
    parser.add_argument(
        '--recommender',
        metavar='RECOMMENDER',
        help='Recommender of the recommendations')
    parser.add_argument(
        '--etag',
        required=True,
        metavar='etag',
        help='Etag of a recommendation')

  def Run(self, args):
    """Run 'gcloud recommender recommendations mark-active'.

    Args:
      args: argparse.Namespace, The arguments that this command was invoked
        with.

    Returns:
      The result recommendations after being marked as active
    """
    recommender_service = api_utils.GetServiceFromArgs(
        args, is_insight_api=False)
    parent_ref = flags.GetParentFromFlags(
        args, is_list_api=False, is_insight_api=False)
    request = api_utils.GetMarkActiveRequestFromArgs(
        args, parent_ref, is_insight_api=False)
    return recommender_service.MarkActive(request)
