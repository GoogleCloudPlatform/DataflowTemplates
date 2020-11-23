# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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
"""Command for listing network peerings."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager
from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import filter_rewrite
from googlecloudsdk.calliope import base
from googlecloudsdk.core import properties
from googlecloudsdk.core.resource import resource_projector


@base.ReleaseTracks(base.ReleaseTrack.GA,
                    base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class List(base.ListCommand):
  """List Compute Engine network peerings."""

  @staticmethod
  def Args(parser):
    parser.display_info.AddFormat("""
        table(peerings:format="table(
            name,
            source_network.basename():label=NETWORK,
            network.map().scope(projects).segment(0):label=PEER_PROJECT,
            network.basename():label=PEER_NETWORK,
            peerMtu,
            importCustomRoutes,
            exportCustomRoutes,
            state,
            stateDetails
        )")
    """)

    parser.add_argument(
        '--network',
        help='Only show peerings of a specific network.')

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())

    client = holder.client.apitools_client
    messages = client.MESSAGES_MODULE

    project = properties.VALUES.core.project.GetOrFail()

    args.filter, filter_expr = filter_rewrite.Rewriter().Rewrite(args.filter)
    request = messages.ComputeNetworksListRequest(
        project=project, filter=filter_expr)

    for network in list_pager.YieldFromList(
        client.networks,
        request,
        field='items',
        limit=args.limit,
        batch_size=None):
      if network.peerings and (args.network is None
                               or args.network == network.name):
        # Network is synthesized for legacy reasons to maintain prior format.
        # In general, synthesized output should not be done.
        synthesized_network = resource_projector.MakeSerializable(network)
        for peering in synthesized_network['peerings']:
          peering['source_network'] = network.selfLink
        yield synthesized_network


List.detailed_help = base_classes.GetGlobalListerHelp('peerings')
