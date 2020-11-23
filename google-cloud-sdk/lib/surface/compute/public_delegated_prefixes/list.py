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
"""List public delegated prefixes command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import lister
from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class List(base.ListCommand):
  """Lists Compute Engine public delegated prefixes."""

  _return_partial_success = True

  @staticmethod
  def Args(parser):
    # TODO(b/131250985): add status once enum is visible
    # TODO(b/131250985): add delegated sub-prefixes list
    parser.display_info.AddFormat("""\
      table(
        name,
        selfLink.scope().segment(-3).yesno(no="global"):label=LOCATION,
        parentPrefix.basename():label=PARENT_PREFIX,
        ipCidrRange:label=RANGE
      )""")

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    request_data = lister.ParseMultiScopeFlags(args, holder.resources)

    list_implementation = lister.MultiScopeLister(
        client,
        regional_service=client.apitools_client.publicDelegatedPrefixes,
        global_service=client.apitools_client.globalPublicDelegatedPrefixes,
        aggregation_service=client.apitools_client.publicDelegatedPrefixes,
        return_partial_success=self._return_partial_success
    )

    return lister.Invoke(request_data, list_implementation)


List.detailed_help = base_classes.GetGlobalListerHelp(
    'public delegated prefixes')
