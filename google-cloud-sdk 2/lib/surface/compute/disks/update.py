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
"""Command for labels update to disks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import disks_util as api_util
from googlecloudsdk.api_lib.compute.operations import poller
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute.disks import flags as disks_flags
from googlecloudsdk.command_lib.util.args import labels_util


def _CommonArgs(cls, parser, support_user_licenses=False):
  """Add arguments used for parsing in all command tracks."""
  cls.DISK_ARG = disks_flags.MakeDiskArg(plural=False)
  cls.DISK_ARG.AddArgument(parser, operation_type='update')
  labels_util.AddUpdateLabelsFlags(parser)

  if support_user_licenses:
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        '--update-user-licenses',
        type=arg_parsers.ArgList(),
        metavar='LICENSE',
        action=arg_parsers.UpdateAction,
        help=(
            'List of user licenses to be updated on a disk. These user licenses'
            ' will replace all existing user licenses. If this flag is not '
            'provided, all existing user licenses will remain unchanged.'))
    scope.add_argument(
        '--clear-user-licenses',
        action='store_true',
        help=('Remove all existing user licenses on a disk.'))


def _LabelsFlagsIncluded(args):
  return args.IsSpecified('update_labels') or args.IsSpecified(
      'clear_labels') or args.IsSpecified('remove_labels')


def _UserLicensesFlagsIncluded(args):
  return args.IsSpecified('update_user_licenses') or args.IsSpecified(
      'clear_user_licenses')


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA)
class Update(base.UpdateCommand):
  r"""Update a Compute Engine persistent disk."""

  DISK_ARG = None

  @classmethod
  def Args(cls, parser):
    _CommonArgs(cls, parser, support_user_licenses=False)

  def Run(self, args):
    return self._Run(args, support_user_licenses=False)

  def _Run(self, args, support_user_licenses=False):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client.apitools_client
    messages = holder.client.messages

    disk_ref = self.DISK_ARG.ResolveAsResource(
        args, holder.resources,
        scope_lister=flags.GetDefaultScopeLister(holder.client))
    disk_info = api_util.GetDiskInfo(disk_ref, client, messages)
    service = disk_info.GetService()

    if support_user_licenses and args.IsSpecified(
        'zone') and _UserLicensesFlagsIncluded(args):
      disk_res = messages.Disk(name=disk_ref.Name())
      if args.IsSpecified('update_user_licenses'):
        disk_res.userLicenses = args.update_user_licenses
      disk_update_request = messages.ComputeDisksUpdateRequest(
          project=disk_ref.project,
          disk=disk_ref.Name(),
          diskResource=disk_res,
          zone=disk_ref.zone,
          paths=['userLicenses'])

      update_operation = service.Update(disk_update_request)
      update_operation_ref = holder.resources.Parse(
          update_operation.selfLink,
          collection=disk_info.GetOperationCollection())
      update_operation_poller = poller.Poller(service)
      result = waiter.WaitFor(
          update_operation_poller, update_operation_ref,
          'Updating user licenses of disk [{0}]'.format(disk_ref.Name()))
      if not _LabelsFlagsIncluded(args):
        return result

    labels_diff = labels_util.GetAndValidateOpsFromArgs(args)

    disk = disk_info.GetDiskResource()

    set_label_req = disk_info.GetSetLabelsRequestMessage()
    labels_update = labels_diff.Apply(set_label_req.LabelsValue, disk.labels)
    request = disk_info.GetSetDiskLabelsRequestMessage(
        disk, labels_update.GetOrNone())

    if not labels_update.needs_update:
      return disk

    operation = service.SetLabels(request)
    operation_ref = holder.resources.Parse(
        operation.selfLink, collection=disk_info.GetOperationCollection())

    operation_poller = poller.Poller(service)
    return waiter.WaitFor(
        operation_poller, operation_ref,
        'Updating labels of disk [{0}]'.format(
            disk_ref.Name()))


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class UpdateAlpha(Update):
  r"""Update a Compute Engine persistent disk."""

  DISK_ARG = None

  @classmethod
  def Args(cls, parser):
    _CommonArgs(cls, parser, support_user_licenses=True)

  def Run(self, args):
    return self._Run(args, support_user_licenses=True)


Update.detailed_help = {
    'DESCRIPTION':
        '*{command}* updates a Compute Engine persistent disk.',
    'EXAMPLES':
        """\
        To update labels 'k0' and 'k1' and remove label 'k3' of a disk, run:

            $ {command} example-disk --zone=us-central1-a --update-labels=k0=value1,k1=value2 --remove-labels=k3

            ``k0'' and ``k1'' are added as new labels if not already present.

        Labels can be used to identify the disk. To list disks with the 'k1:value2' label, run:

            $ {parent_command} list --filter='labels.k1:value2'

        To list only the labels when describing a resource, use --format to filter the result:

            $ {parent_command} describe example-disk --format="default(labels)"
        """,
}
