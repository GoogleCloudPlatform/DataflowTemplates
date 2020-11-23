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
"""parsing flags for Recommender APIs."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.util.args import common_args


def AddParentFlagsToParser(parser):
  """Adding argument mutex group project, billing-account, folder, organization to parser.

  Args:
      parser: An argparse parser that you can use to add arguments that go on
        the command line after this command.
  """
  resource_group = parser.add_mutually_exclusive_group(
      required=True,
      help='Resource that is associated with cloud entity type. Currently four mutually exclusive flags are supported, --project, --billing-account, --folder, --organization.'
  )
  common_args.ProjectArgument(
      help_text_to_prepend='The Google Cloud Platform Project Number to use for this invocation.'
  ).AddToParser(resource_group)
  resource_group.add_argument(
      '--billing-account',
      metavar='BILLING_ACCOUNT',
      help='The Google Cloud Platform Billing Account ID to use for this invocation.'
  )
  resource_group.add_argument(
      '--organization',
      metavar='ORGANIZATION_ID',
      help='The Google Cloud Platform Organization ID to use for this invocation.'
  )
  resource_group.add_argument(
      '--folder',
      metavar='FOLDER_ID',
      help='Folder ID to use for this invocation.')


def GetLocationAndRecommender(location, recommender):
  return '/locations/{0}/recommenders/{1}'.format(location, recommender)


def GetParentFromFlags(args, is_list_api, is_insight_api):
  """Parsing args to get full url string.

  Args:
      args: argparse.Namespace, The arguments that this command was invoked
        with.
      is_list_api: Boolean value specifying whether this is a list api, if not
        append recommendation id or insight id to the resource name.
      is_insight_api: whether this is an insight api, if so, append
        insightTypes/[INSIGHT_TYPE] rather than recommenders/[RECOMMENDER_ID].

  Returns:
      The full url string based on flags given by user.
  """
  url = ''
  if args.project:
    url = 'projects/{0}'.format(args.project)
  elif args.billing_account:
    url = 'billingAccounts/{0}'.format(args.billing_account)
  elif args.folder:
    url = 'folders/{0}'.format(args.folder)
  elif args.organization:
    url = 'organizations/{0}'.format(args.organization)

  url = url + '/locations/{0}'.format(args.location)

  if is_insight_api:
    url = url + '/insightTypes/{0}'.format(args.insight_type)
    if not is_list_api:
      url = url + '/insights/{0}'.format(args.INSIGHT)
  else:
    url = url + '/recommenders/{0}'.format(args.recommender)
    if not is_list_api:
      url = url + '/recommendations/{0}'.format(args.RECOMMENDATION)
  return url
