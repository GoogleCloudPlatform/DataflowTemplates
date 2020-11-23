# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""Command for deleting subnetworks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.networks.subnets import flags


class Delete(base.DeleteCommand):
  """Delete Compute Engine subnetworks.

  *{command}* deletes one or more Compute Engine
  subnetworks. Subnetworks can only be deleted when no other resources
  (e.g., virtual machine instances) refer to them.
  """

  SUBNET_ARG = None

  @staticmethod
  def Args(parser):
    Delete.SUBNET_ARG = flags.SubnetworkArgument(plural=True)
    Delete.SUBNET_ARG.AddArgument(parser, operation_type='delete')
    parser.display_info.AddCacheUpdater(flags.SubnetworksCompleter)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    subnet_refs = Delete.SUBNET_ARG.ResolveAsResource(
        args,
        holder.resources,
        scope_lister=compute_flags.GetDefaultScopeLister(client))

    utils.PromptForDeletion(subnet_refs, 'region')

    requests = []
    for subnet_ref in subnet_refs:
      requests.append((client.apitools_client.subnetworks, 'Delete',
                       client.messages.ComputeSubnetworksDeleteRequest(
                           **subnet_ref.AsDict())))

    return client.MakeRequests(requests)
