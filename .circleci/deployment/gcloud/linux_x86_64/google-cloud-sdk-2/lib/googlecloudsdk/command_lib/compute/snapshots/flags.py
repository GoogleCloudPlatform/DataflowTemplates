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
"""Flags and helpers for the compute snapshots commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import completers as compute_completers
from googlecloudsdk.command_lib.compute import flags as compute_flags


def MakeSnapshotArg(plural=False):
  return compute_flags.ResourceArgument(
      resource_name='snapshot',
      name='snapshot_name',
      completer=compute_completers.RoutesCompleter,
      plural=plural,
      global_collection='compute.snapshots')


def AddChainArg(parser):
  parser.add_argument(
      '--chain-name',
      help=(
          """Create the new snapshot in the snapshot chain labeled with the specified name.
          The chain name must be 1-63 characters long and comply with RFC1035.
          Use this flag only if you are an advanced service owner who needs
          to create separate snapshot chains, for example, for chargeback tracking.
          When you describe your snapshot resource, this field is visible only
          if it has a non-empty value."""))


def AddSourceDiskCsekKey(parser):
  parser.add_argument(
      '--source-disk-key-file',
      metavar='FILE',
      help="""
      Path to the customer-supplied encryption key of the source disk.
      Required if the source disk is protected by a customer-supplied
      encryption key.
      """)


SOURCE_DISK_ARG = compute_flags.ResourceArgument(
    resource_name='source disk',
    name='--source-disk',
    completer=compute_completers.DisksCompleter,
    short_help="""
    Source disk used to create the snapshot. To create a snapshot from a source
    disk in a different project, specify the full path to the source disk.
    For example:
    https://www.googleapis.com/compute/v1/projects/MY-PROJECT/zones/MY-ZONE/disks/MY-DISK
    """,
    zonal_collection='compute.disks',
    regional_collection='compute.regionDisks',
    required=False)
