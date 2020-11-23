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
"""Command for listing SSL certificates."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import lister
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute.ssl_certificates import flags


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.GA)
class List(base.ListCommand):
  """List Compute Engine SSL certificates."""

  _return_partial_success = False

  @staticmethod
  def Args(parser):
    parser.display_info.AddFormat(flags.DEFAULT_LIST_FORMAT)
    lister.AddMultiScopeListerFlags(parser, regional=True, global_=True)
    parser.display_info.AddCacheUpdater(flags.SslCertificatesCompleterBeta)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    request_data = lister.ParseMultiScopeFlags(args, holder.resources)
    list_implementation = lister.MultiScopeLister(
        client,
        regional_service=client.apitools_client.regionSslCertificates,
        global_service=client.apitools_client.sslCertificates,
        aggregation_service=client.apitools_client.sslCertificates,
        return_partial_success=self._return_partial_success)

    return lister.Invoke(request_data, list_implementation)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class ListAlpha(List):
  """List Google Compute Engine SSL certificates."""

  _return_partial_success = True


List.detailed_help = base_classes.GetMultiScopeListerHelp(
    'SSL certificates',
    scopes=[
        base_classes.ScopeType.global_scope,
        base_classes.ScopeType.regional_scope
    ])
