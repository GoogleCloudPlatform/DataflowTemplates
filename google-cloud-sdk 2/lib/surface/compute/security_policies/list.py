# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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

"""Command for listing security policies."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager
from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.core import properties


class List(base.ListCommand):
  """List security policies."""

  @staticmethod
  def Args(parser):
    parser.display_info.AddFormat('table(name)')

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())

    client = holder.client.apitools_client
    messages = client.MESSAGES_MODULE

    project = properties.VALUES.core.project.Get(required=True)

    request = messages.ComputeSecurityPoliciesListRequest(
        project=project, filter=args.filter)

    return list_pager.YieldFromList(
        client.securityPolicies,
        request,
        field='items',
        limit=args.limit,
        batch_size=None)


List.detailed_help = base_classes.GetGlobalListerHelp('security policies')
