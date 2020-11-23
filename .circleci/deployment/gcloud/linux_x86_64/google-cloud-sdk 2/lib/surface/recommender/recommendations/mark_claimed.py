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
"""recommender API recommendations list command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.recommender import flag_utils as api_utils
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.recommender import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class MarkClaimed(base.Command):
  r"""Mark a recommendation's state as CLAIMED.

      Mark a recommendation's state as CLAIMED. Can be applied to
      recommendations in
      CLAIMED, SUCCEEDED, FAILED, or ACTIVE state. Users can use this method to
      indicate to the Recommender API that they are starting to apply the
      recommendation themselves. This stops the recommendation content from
      being updated.

     ## EXAMPLES
      To mark a recommendation as CLAIMED:

      $ {command} RECOMMENDATION --project=project-name --location=global
      --recommender=google.compute.instance.MachineTypeRecommender --etag=abc123
      --state-metadata=key1=value1,key2=value2
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
        help='Recommendation id which will be marked as claimed')
    parser.add_argument('--location', metavar='LOCATION', help='Location')
    parser.add_argument(
        '--recommender',
        metavar='RECOMMENDER',
        help='Recommender of recommendation')
    parser.add_argument(
        '--etag',
        metavar='etag',
        required=True,
        help='Etag of a recommendation')
    parser.add_argument(
        '--state-metadata',
        type=arg_parsers.ArgDict(min_length=1),
        default={},
        help='State metadata for recommendation, in format of --state-metadata=key1=value1,key2=value2',
        metavar='KEY=VALUE',
        action=arg_parsers.StoreOnceAction)

  def Run(self, args):
    """Run 'gcloud recommender recommendations mark-claimed'.

    Args:
      args: argparse.Namespace, The arguments that this command was invoked
        with.

    Returns:
      The recommendations after being marked as claimed.
    """
    recommender_service = api_utils.GetServiceFromArgs(
        args, is_insight_api=False)
    parent_ref = flags.GetParentFromFlags(
        args, is_list_api=False, is_insight_api=False)
    request = api_utils.GetMarkClaimedRequestFromArgs(args, parent_ref)
    return recommender_service.MarkClaimed(request)
