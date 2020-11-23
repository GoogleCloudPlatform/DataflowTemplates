# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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
"""Command for listing URL maps."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import lister
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute.url_maps import flags


def _DetailedHelp(include_l7_internal_load_balancing):
  if include_l7_internal_load_balancing:
    return base_classes.GetMultiScopeListerHelp(
        'URL maps',
        scopes=[
            base_classes.ScopeType.global_scope,
            base_classes.ScopeType.regional_scope
        ])
  else:
    return base_classes.GetGlobalListerHelp('URL maps')


def _Run(args, holder, include_l7_internal_load_balancing,
         return_partial_success):
  """Issues requests necessary to list URL maps."""
  client = holder.client

  if include_l7_internal_load_balancing:
    request_data = lister.ParseMultiScopeFlags(args, holder.resources)
    list_implementation = lister.MultiScopeLister(
        client,
        regional_service=client.apitools_client.regionUrlMaps,
        global_service=client.apitools_client.urlMaps,
        aggregation_service=client.apitools_client.urlMaps,
        return_partial_success=return_partial_success)
  else:
    request_data = lister.ParseNamesAndRegexpFlags(args, holder.resources)
    list_implementation = lister.GlobalLister(client,
                                              client.apitools_client.urlMaps)
  return lister.Invoke(request_data, list_implementation)


@base.ReleaseTracks(base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class List(base.ListCommand):
  """List URL maps."""

  # TODO(b/144022508): Remove _include_l7_internal_load_balancing
  _include_l7_internal_load_balancing = True
  _return_partial_success = False

  detailed_help = _DetailedHelp(_include_l7_internal_load_balancing)

  @classmethod
  def Args(cls, parser):
    parser.display_info.AddFormat(flags.DEFAULT_LIST_FORMAT)
    if cls._include_l7_internal_load_balancing:
      lister.AddMultiScopeListerFlags(parser, regional=True, global_=True)
      parser.display_info.AddCacheUpdater(flags.UrlMapsCompleterAlpha)
    else:
      lister.AddBaseListerArgs(parser)
      parser.display_info.AddCacheUpdater(flags.UrlMapsCompleter)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return _Run(args, holder, self._include_l7_internal_load_balancing,
                self._return_partial_success)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class ListAlpha(List):
  """List URL maps."""

  _return_partial_success = True
