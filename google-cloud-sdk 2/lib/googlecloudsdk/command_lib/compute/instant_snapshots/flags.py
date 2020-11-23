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
"""Flags and helpers for the compute instant snapshots commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import completers as compute_completers
from googlecloudsdk.command_lib.compute import flags as compute_flags

_SOURCE_DISK_DETAILED_HELP = """\
      Source disk used to create the instant snapshot.
"""

MULTISCOPE_LIST_FORMAT = """
    table(
      name,
      location(),
      location_scope(),
      status
      )"""


def MakeInstantSnapshotArg(plural=False):
  return compute_flags.ResourceArgument(
      resource_name='instant snapshot',
      completer=compute_completers.InstantSnapshotsCompleter,
      plural=plural,
      name='INSTANT_SNAPSHOT_NAME',
      zonal_collection='compute.zoneInstantSnapshots',
      regional_collection='compute.regionInstantSnapshots',
      zone_explanation=compute_flags.ZONE_PROPERTY_EXPLANATION,
      region_explanation=compute_flags.REGION_PROPERTY_EXPLANATION)


SOURCE_DISK_ARG = compute_flags.ResourceArgument(
    resource_name='source disk',
    name='--source-disk',
    completer=compute_completers.DisksCompleter,
    short_help='Source disk used to create the instant snapshot.',
    detailed_help=_SOURCE_DISK_DETAILED_HELP,
    zonal_collection='compute.disks',
    regional_collection='compute.regionDisks',
    zone_explanation=compute_flags.ZONE_PROPERTY_EXPLANATION,
    region_explanation=compute_flags.REGION_PROPERTY_EXPLANATION,
    use_existing_default_scope=True,
    required=True)
