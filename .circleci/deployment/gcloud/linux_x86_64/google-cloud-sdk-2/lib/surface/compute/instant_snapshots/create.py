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
"""Create instant snapshot command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute.instant_snapshots import flags as ips_flags
from googlecloudsdk.command_lib.util.args import labels_util

import six


def _SourceArgs(parser):
  source_disk = parser.add_group('Source disk options')
  ips_flags.SOURCE_DISK_ARG.AddArgument(source_disk)


def _CommonArgs(parser):
  """A helper function to build args based on different API version."""
  Create.IPS_ARG = ips_flags.MakeInstantSnapshotArg()
  Create.IPS_ARG.AddArgument(parser, operation_type='create')
  labels_util.AddCreateLabelsFlags(parser)
  _SourceArgs(parser)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Create(base.Command):
  """Create Compute Engine instant snapshots."""

  @classmethod
  def Args(cls, parser):
    _CommonArgs(parser)

  @classmethod
  def _GetApiHolder(cls, no_http=False):
    return base_classes.ComputeApiHolder(cls.ReleaseTrack())

  def _GetSourceDiskUri(self, args, compute_holder, default_scope):
    source_disk_ref = ips_flags.SOURCE_DISK_ARG.ResolveAsResource(
        args, compute_holder.resources)
    if source_disk_ref:
      return source_disk_ref.SelfLink()
    return None

  def _Run(self, args):
    compute_holder = self._GetApiHolder()
    client = compute_holder.client
    messages = client.messages

    ips_ref = Create.IPS_ARG.ResolveAsResource(args, compute_holder.resources)
    requests = []
    if ips_ref.Collection() == 'compute.zoneInstantSnapshots':
      instant_snapshot = messages.InstantSnapshot(
          name=ips_ref.Name(),
          sourceDisk=self._GetSourceDiskUri(args, compute_holder,
                                            compute_scope.ScopeEnum.ZONE))
      request = messages.ComputeZoneInstantSnapshotsInsertRequest(
          instantSnapshot=instant_snapshot,
          project=ips_ref.project,
          zone=ips_ref.zone)
      request = (client.apitools_client.zoneInstantSnapshots, 'Insert', request)
    elif ips_ref.Collection() == 'compute.regionInstantSnapshots':
      instant_snapshot = messages.InstantSnapshot(
          name=ips_ref.Name(),
          sourceDisk=self._GetSourceDiskUri(args, compute_holder,
                                            compute_scope.ScopeEnum.REGION))
      request = messages.ComputeRegionInstantSnapshotsInsertRequest(
          instantSnapshot=instant_snapshot,
          project=ips_ref.project,
          region=ips_ref.region)
      request = (client.apitools_client.regionInstantSnapshots, 'Insert',
                 request)

    args_labels = getattr(args, 'labels', None)
    if args_labels:
      labels = messages.InstantSnapshot.LabelsValue(additionalProperties=[
          messages.InstantSnapshot.LabelsValue.AdditionalProperty(
              key=key, value=value)
          for key, value in sorted(six.iteritems(args_labels))
      ])
      instant_snapshot.labels = labels

    requests.append(request)
    return client.MakeRequests(requests)

  def Run(self, args):
    return self._Run(args)
