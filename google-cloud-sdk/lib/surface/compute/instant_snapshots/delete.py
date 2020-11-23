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
"""Delete instant snapshot command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.instant_snapshots import flags as ips_flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Delete(base.DeleteCommand):
  """Delete Compute Engine instant snapshots."""

  def _GetCommonScopeNameForRefs(self, refs):
    """Gets common scope for references."""
    has_zone = any(hasattr(ref, 'zone') for ref in refs)
    has_region = any(hasattr(ref, 'region') for ref in refs)

    if has_zone and not has_region:
      return 'zone'
    elif has_region and not has_zone:
      return 'region'
    else:
      return None

  def _CreateDeleteRequests(self, client, ips_refs):
    """Returns a list of delete messages for instant snapshots."""

    messages = client.MESSAGES_MODULE
    requests = []
    for ips_ref in ips_refs:
      if ips_ref.Collection() == 'compute.zoneInstantSnapshots':
        service = client.zoneInstantSnapshots
        request = messages.ComputeZoneInstantSnapshotsDeleteRequest(
            instantSnapshot=ips_ref.Name(),
            project=ips_ref.project,
            zone=ips_ref.zone)
      elif ips_ref.Collection() == 'compute.regionInstantSnapshots':
        service = client.regionInstantSnapshots
        request = messages.ComputeRegionInstantSnapshotsDeleteRequest(
            instantSnapshot=ips_ref.Name(),
            project=ips_ref.project,
            region=ips_ref.region)
      else:
        raise ValueError('Unknown reference type {0}'.format(
            ips_ref.Collection()))

      requests.append((service, 'Delete', request))
    return requests

  @staticmethod
  def Args(parser):
    Delete.ips_arg = ips_flags.MakeInstantSnapshotArg(plural=True)
    Delete.ips_arg.AddArgument(parser, operation_type='delete')

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())

    ips_refs = Delete.ips_arg.ResolveAsResource(
        args,
        holder.resources,
        scope_lister=compute_flags.GetDefaultScopeLister(holder.client))

    scope_name = self._GetCommonScopeNameForRefs(ips_refs)

    utils.PromptForDeletion(ips_refs, scope_name=scope_name, prompt_title=None)

    requests = list(
        self._CreateDeleteRequests(holder.client.apitools_client, ips_refs))

    return holder.client.MakeRequests(requests)
