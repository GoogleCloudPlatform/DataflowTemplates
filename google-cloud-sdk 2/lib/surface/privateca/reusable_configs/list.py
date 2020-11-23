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
"""List reusable configs in a location."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.privateca import constants
from googlecloudsdk.api_lib.privateca import locations
from googlecloudsdk.api_lib.util import common_args
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.privateca import response_utils


# Resource IDs of the currently available reusable configs.
_KnownResourceIds = [
    'leaf-client-tls',
    'leaf-code-signing',
    'leaf-mtls',
    'leaf-server-tls',
    'leaf-smime',
    'root-unconstrained',
    'subordinate-client-tls-pathlen-0',
    'subordinate-code-signing-pathlen-0',
    'subordinate-mtls-pathlen-0',
    'subordinate-server-tls-pathlen-0',
    'subordinate-smime-pathlen-0',
    'subordinate-unconstrained-pathlen-0',
]


def _GetLocation(args):
  if args.IsSpecified('location'):
    return args.location

  # During alpha, we replicate the same set of resources across all locations.
  return locations.GetSupportedLocations()[0]


class List(base.ListCommand):
  """List reusable configs in a location."""

  @staticmethod
  def Args(parser):
    base.Argument(
        '--location',
        help=('Location of the reusable configs. If this is not specified, it '
              'defaults to the first location supported by the service.'
             )).AddToParser(parser)
    base.PAGE_SIZE_FLAG.SetDefault(parser, 100)
    base.SORT_BY_FLAG.SetDefault(parser, 'name')

    parser.display_info.AddFormat("""
        table(
          name.scope("reusableConfigs"):label=NAME,
          name.scope("locations").segment(0):label=LOCATION,
          description)
        """)

  def ListLatestReusableConfigs(self, args, project, location):
    """Makes one or more List requests for the latest reusable config resources."""
    parent = 'projects/{}/locations/{}'.format(project, location)
    request = self.messages.PrivatecaProjectsLocationsReusableConfigsListRequest(
        parent=parent,
        orderBy=common_args.ParseSortByArg(args.sort_by),
        filter=args.filter)

    return list_pager.YieldFromList(
        self.client.projects_locations_reusableConfigs,
        request,
        field='reusableConfigs',
        limit=args.limit,
        batch_size_attribute='pageSize',
        batch_size=args.page_size,
        get_field_func=response_utils.GetFieldAndLogUnreachable)

  def ListKnownReusableConfigs(self, project, location):
    """Make a series of Get requests for the known reusable config resources."""
    parent = 'projects/{}/locations/{}'.format(project, location)

    for resource_id in _KnownResourceIds:
      resource_name = '{}/reusableConfigs/{}'.format(parent, resource_id)
      yield self.client.projects_locations_reusableConfigs.Get(
          self.messages.PrivatecaProjectsLocationsReusableConfigsGetRequest(
              name=resource_name))

  def Run(self, args):
    """Runs the command."""
    self.client = privateca_base.GetClientInstance()
    self.messages = privateca_base.GetMessagesModule()

    project = constants.PREDEFINED_REUSABLE_CONFIG_PROJECT
    location = _GetLocation(args)

    # TODO(b/170409946): Revert to ListLatestReusableConfigs after IAM issue.
    return self.ListKnownReusableConfigs(project, location)

