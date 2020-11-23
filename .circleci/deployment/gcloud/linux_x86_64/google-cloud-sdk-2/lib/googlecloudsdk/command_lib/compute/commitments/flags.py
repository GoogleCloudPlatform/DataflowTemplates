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

"""Flags and helpers for the compute commitments commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.command_lib.compute import completers as compute_completers
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.reservations import flags as reservation_flags
from googlecloudsdk.command_lib.compute.reservations import resource_args
from googlecloudsdk.command_lib.util.apis import arg_utils


VALID_PLANS = ['12-month', '36-month']
_REQUIRED_RESOURCES = sorted(['vcpu', 'memory'])


class RegionCommitmentsCompleter(compute_completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(RegionCommitmentsCompleter, self).__init__(
        collection='compute.regionCommitments',
        list_command='alpha compute commitments list --uri',
        **kwargs)


def _GetFlagToPlanMap(messages):
  return {
      '12-month': messages.Commitment.PlanValueValuesEnum.TWELVE_MONTH,
      '36-month': messages.Commitment.PlanValueValuesEnum.THIRTY_SIX_MONTH,
  }


def TranslatePlanArg(messages, plan_arg):
  return _GetFlagToPlanMap(messages)[plan_arg]


def TranslateResourcesArg(messages, resources_arg):
  return [
      messages.ResourceCommitment(
          amount=resources_arg['vcpu'],
          type=messages.ResourceCommitment.TypeValueValuesEnum.VCPU,
      ),
      # Arg is in B API accepts values in MB.
      messages.ResourceCommitment(
          amount=resources_arg['memory'] // (1024 * 1024),
          type=messages.ResourceCommitment.TypeValueValuesEnum.MEMORY,
      ),
  ]


def TranslateResourcesArgGroup(messages, args):
  """Util functions to parse ResourceCommitments."""
  resources_arg = args.resources
  resources = TranslateResourcesArg(messages, resources_arg)

  if 'local-ssd' in resources_arg.keys():
    resources.append(
        messages.ResourceCommitment(
            amount=resources_arg['local-ssd'],
            type=messages.ResourceCommitment.TypeValueValuesEnum.LOCAL_SSD))

  if args.IsSpecified('resources_accelerator'):
    accelerator_arg = args.resources_accelerator
    resources.append(
        messages.ResourceCommitment(
            amount=accelerator_arg['count'],
            acceleratorType=accelerator_arg['type'],
            type=messages.ResourceCommitment.TypeValueValuesEnum.ACCELERATOR))

  return resources


def MakeCommitmentArg(plural):
  return compute_flags.ResourceArgument(
      resource_name='commitment',
      completer=RegionCommitmentsCompleter,
      plural=plural,
      name='commitment',
      regional_collection='compute.regionCommitments',
      region_explanation=compute_flags.REGION_PROPERTY_EXPLANATION)


def AddCreateFlags(parser):
  """Add general arguments for `commitments create` flag."""
  AddPlan(parser)
  AddReservationArgGroup(parser)
  AddResourcesArgGroup(parser)


def AddPlan(parser):
  return parser.add_argument(
      '--plan',
      required=True,
      choices=VALID_PLANS,
      help='Duration of the commitment.')


def AddLicenceBasedFlags(parser):
  parser.add_argument('--license', required=True,
                      help='Applicable license URI. For example: '
                           '`https://www.googleapis.com/compute/v1/projects/suse-sap-cloud/global/licenses/sles-sap-12`')  #  pylint:disable=line-too-long
  parser.add_argument('--cores-per-license', required=False, type=str,
                      help='Core range of the instance. Must be one of: `1-2`,'
                           ' `3-4`, `5+`. Required for SAP licenses.')
  parser.add_argument('--amount', required=True, type=int,
                      help='Number of licenses purchased.')
  AddPlan(parser)


# TODO(b/129054682): fix the format in text render.
def AddResourcesArgGroup(parser):
  """Add the argument group for ResourceCommitment support in commitment."""
  resources_group = parser.add_group(
      'Manage the commitment for particular resources.', required=True)

  resources_help = """\
Resources to be included in the commitment. The ratio between the number of vCPU cores and memory must conform to limits described at:
https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type#specifications.
*memory*::: The size of the memory, should include units (e.g. 3072MB or 9GB). If no units are specified, GB is assumed.
*vcpu*::: The number of the vCPU cores.
*local-ssd*::: The size of local SSD.
"""

  resources_group.add_argument(
      '--resources',
      help=resources_help,
      type=arg_parsers.ArgDict(
          spec={
              'vcpu': int,
              'local-ssd': int,
              'memory': arg_parsers.BinarySize()
          }))
  accelerator_help = """\
Manage the configuration of the type and number of accelerator cards to include in the commitment.
*count*::: The number of accelerators to include.
*type*::: The specific type (e.g. nvidia-tesla-k80 for NVIDIA Tesla K80) of the accelerator. Use `gcloud compute accelerator-types list` to learn about all available accelerator types.
"""
  resources_group.add_argument(
      '--resources-accelerator',
      help=accelerator_help,
      type=arg_parsers.ArgDict(spec={
          'count': int,
          'type': str
      }))


def GetTypeMapperFlag(messages):
  """Helper to get a choice flag from the commitment type enum."""
  return arg_utils.ChoiceEnumMapper(
      '--type',
      messages.Commitment.TypeValueValuesEnum,
      help_str=(
          'Type of commitment. `memory-optimized` indicates that the '
          'commitment is for memory-optimized VMs.'),
      default='general-purpose',
      include_filter=lambda x: x != 'TYPE_UNSPECIFIED')


def AddUpdateReservationGroup(parser):
  """Add reservation arguments to the update-reservations command."""
  parent_reservations_group = parser.add_group(
      'Manage reservations that are attached to the commitment.',
      mutex=True)
  AddReservationsFromFileFlag(
      parent_reservations_group,
      custom_text='Path to a YAML file of two reservations\' configuration.')
  reservations_group = parent_reservations_group.add_group(
      'Specify source and destination reservations configuration.')
  AddReservationArguments(reservations_group)
  reservation_flags.GetAcceleratorFlag(
      '--source-accelerator').AddToParser(reservations_group)
  reservation_flags.GetAcceleratorFlag(
      '--dest-accelerator').AddToParser(reservations_group)
  reservation_flags.GetLocalSsdFlag(
      '--source-local-ssd').AddToParser(reservations_group)
  reservation_flags.GetLocalSsdFlag(
      '--dest-local-ssd').AddToParser(reservations_group)
  return parser


def AddReservationArguments(parser):
  """Add --source-reservation and --dest-reservation arguments to parser."""
  help_text = """
{0} reservation configuration.
*reservation*::: Name of the {0} reservation to operate on.
*reservation-zone*:::  Zone of the {0} reservation to operate on.
*vm-count*::: The number of VM instances that are allocated to this reservation.
The value of this field must be an int in the range [1, 1000].
*machine-type*:::  The type of machine (name only) which has a fixed number of
vCPUs and a fixed amount of memory. This also includes specifying custom machine
type following `custom-number_of_CPUs-amount_of_memory` pattern, e.g. `custom-32-29440`.
*min-cpu-platform*::: Optional minimum CPU platform of the reservation to create.
*require-specific-reservation*::: Indicates whether the reservation can be consumed by VMs with "any reservation"
defined. If enabled, then only VMs that target this reservation by name using
`--reservation-affinity=specific` can consume from this reservation.
"""
  reservation_spec = {
      'reservation': str,
      'reservation-zone': str,
      'vm-count': int,
      'machine-type': str,
      'min-cpu-platform': str,
      'require-specific-reservation': bool,
  }

  parser.add_argument('--source-reservation',
                      type=arg_parsers.ArgDict(spec=reservation_spec),
                      help=help_text.format('source'),
                      required=True)
  parser.add_argument('--dest-reservation',
                      type=arg_parsers.ArgDict(spec=reservation_spec),
                      help=help_text.format('destination'),
                      required=True)
  return parser


def AddReservationsFromFileFlag(parser, custom_text=None):
  help_text = (custom_text if custom_text else
               'Path to a YAML file of multiple reservations\' configuration.')
  return parser.add_argument(
      '--reservations-from-file',
      type=arg_parsers.FileContents(),
      help=help_text)


def AddReservationArgGroup(parser):
  """Adds all flags needed for reservations creation."""
  reservations_manage_group = parser.add_group(
      'Manage the reservations to be created with the commitment.', mutex=True)

  AddReservationsFromFileFlag(reservations_manage_group)

  single_reservation_group = reservations_manage_group.add_argument_group(
      help='Manage the reservation to be created with the commitment.')
  resource_args.GetReservationResourceArg(
      positional=False).AddArgument(single_reservation_group)
  single_reservation_group.add_argument(
      '--reservation-type',
      hidden=True,
      choices=['specific'],
      default='specific',
      help='The type of the reservation to be created.')

  specific_sku_reservation_group = single_reservation_group.add_argument_group(
      help='Manage the specific SKU reservation properties to create.')
  AddFlagsToSpecificSkuGroup(specific_sku_reservation_group)


def AddFlagsToSpecificSkuGroup(group):
  """Adds flags needed for a specific sku zonal allocation."""
  args = [
      reservation_flags.GetRequireSpecificAllocation(),
      reservation_flags.GetVmCountFlag(required=False),
      reservation_flags.GetMinCpuPlatform(),
      reservation_flags.GetMachineType(required=False),
      reservation_flags.GetLocalSsdFlag(),
      reservation_flags.GetAcceleratorFlag(),
  ]

  for arg in args:
    arg.AddToParser(group)
