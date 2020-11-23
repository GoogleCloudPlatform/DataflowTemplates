# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""Flags and helpers for the compute routers commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap
import enum
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.command_lib.compute import flags as compute_flags

IP_ADDRESSES_ARG = compute_flags.ResourceArgument(
    name='--nat-external-ip-pool',
    short_help='External IP Addresses to use for NAT',
    resource_name='address',
    regional_collection='compute.addresses',
    region_hidden=True,
    plural=True,
    required=False)

DRAIN_NAT_IP_ADDRESSES_ARG = compute_flags.ResourceArgument(
    name='--nat-external-drain-ip-pool',
    detailed_help=textwrap.dedent("""\
       External IP Addresses to be drained

       These IPs must be valid external IPs that have been used as NAT IPs
       """),
    resource_name='address',
    regional_collection='compute.addresses',
    region_hidden=True,
    plural=True,
    required=False)


class IpAllocationOption(enum.Enum):
  AUTO = 0
  MANUAL = 1


class SubnetOption(enum.Enum):
  ALL_RANGES = 0
  PRIMARY_RANGES = 1
  CUSTOM_RANGES = 2


DEFAULT_LIST_FORMAT = """\
    table(
      name,
      natIpAllocateOption,
      sourceSubnetworkIpRangesToNat
    )"""


def AddNatNameArg(parser, operation_type='operate on', plural=False):
  """Adds a positional argument for the NAT name."""
  help_text = 'Name of the NAT{} to {}'.format('s' if plural else '',
                                               operation_type)
  params = {'help': help_text}
  if plural:
    params['nargs'] = '+'

  parser.add_argument('name', **params)


def AddCommonNatArgs(parser,
                     for_create=False,
                     with_endpoint_independent_mapping=False):
  """Adds common arguments for creating and updating NATs."""
  _AddIpAllocationArgs(parser, for_create)
  _AddSubnetworkArgs(parser, for_create)
  _AddTimeoutsArgs(parser, for_create)
  _AddMinPortsPerVmArg(parser, for_create)
  _AddLoggingArgs(parser)
  if with_endpoint_independent_mapping:
    _AddEndpointIndependentMappingArg(parser)
  if not for_create:
    _AddDrainNatIpsArgument(parser)


def _AddIpAllocationArgs(parser, for_create=False):
  """Adds a mutually exclusive group to specify IP allocation options."""

  ip_allocation = parser.add_mutually_exclusive_group(required=for_create)
  ip_allocation.add_argument(
      '--auto-allocate-nat-external-ips',
      help='Automatically allocate external IP addresses for Cloud NAT',
      action='store_const',
      dest='ip_allocation_option',
      const=IpAllocationOption.AUTO,
      default=IpAllocationOption.MANUAL)
  IP_ADDRESSES_ARG.AddArgument(
      parser, mutex_group=ip_allocation, cust_metavar='IP_ADDRESS')


def _AddSubnetworkArgs(parser, for_create=False):
  """Adds a mutually exclusive group to specify subnet options."""
  subnetwork = parser.add_mutually_exclusive_group(required=for_create)
  subnetwork.add_argument(
      '--nat-all-subnet-ip-ranges',
      help=textwrap.dedent("""\
          Allow all IP ranges of all subnetworks in the region, including
          primary and secondary ranges, to use NAT."""),
      action='store_const',
      dest='subnet_option',
      const=SubnetOption.ALL_RANGES,
      default=SubnetOption.CUSTOM_RANGES)
  subnetwork.add_argument(
      '--nat-primary-subnet-ip-ranges',
      help=textwrap.dedent("""\
          Allow only primary IP ranges of all subnetworks in the region to use
          NAT."""),
      action='store_const',
      dest='subnet_option',
      const=SubnetOption.PRIMARY_RANGES,
      default=SubnetOption.CUSTOM_RANGES)
  subnetwork.add_argument(
      '--nat-custom-subnet-ip-ranges',
      metavar='SUBNETWORK[:RANGE_NAME]',
      help=textwrap.dedent("""\
          List of subnetwork primary and secondary IP ranges to be allowed to
          use NAT.
          [SUBNETWORK]:
          including a subnetwork name includes only the primary
          subnet range of the subnetwork.
          [SUBNETWORK]:[RANGE_NAME]:
          specifying a subnetwork and secondary range
          name includes only that secondary range.It does not include the
          primary range of the subnet."""),
      type=arg_parsers.ArgList(min_length=1))


def _AddTimeoutsArgs(parser, for_create=False):
  """Adds arguments to specify connection timeouts."""
  _AddClearableArgument(
      parser, for_create, 'udp-idle-timeout', arg_parsers.Duration(),
      textwrap.dedent("""\
         Timeout for UDP connections. See $ gcloud topic datetimes for
         information on duration formats."""),
      'Clear timeout for UDP connections')
  _AddClearableArgument(
      parser, for_create, 'icmp-idle-timeout', arg_parsers.Duration(),
      textwrap.dedent("""\
         Timeout for ICMP connections. See $ gcloud topic datetimes for
         information on duration formats."""),
      'Clear timeout for ICMP connections')
  _AddClearableArgument(
      parser, for_create, 'tcp-established-idle-timeout',
      arg_parsers.Duration(),
      textwrap.dedent("""\
         Timeout for TCP established connections. See $ gcloud topic datetimes
         for information on duration formats."""),
      'Clear timeout for TCP established connections')
  _AddClearableArgument(
      parser, for_create, 'tcp-transitory-idle-timeout', arg_parsers.Duration(),
      textwrap.dedent("""\
         Timeout for TCP transitory connections. See $ gcloud topic datetimes
         for information on duration formats."""),
      'Clear timeout for TCP transitory connections')


def _AddMinPortsPerVmArg(parser, for_create=False):
  """Adds an argument to specify the minimum number of ports per VM for NAT."""
  _AddClearableArgument(
      parser,
      for_create,
      'min-ports-per-vm',
      arg_parsers.BoundedInt(lower_bound=2),
      'Minimum ports to be allocated to a VM',
      'Clear minimum ports to be allocated to a VM')


def _AddLoggingArgs(parser):
  """Adds arguments to configure NAT logging."""
  enable_logging_help_text = textwrap.dedent("""\
    Enable logging for the NAT. Logs will be exported to Stackdriver. NAT
    logging is disabled by default.
    To disable logging for the NAT, use
    $ {parent_command} update MY-NAT --no-enable-logging --router ROUTER
      --region REGION""")
  log_filter_help_text = textwrap.dedent("""\
    Filter for logs exported to stackdriver.

    The default is ALL.

    If logging is not enabled, filter settings will be persisted but will have
    no effect.

    Use --[no-]enable-logging to enable and disable logging.
""")
  filter_choices = {
      'ALL': 'Export logs for all connections handled by this NAT.',
      'ERRORS_ONLY': 'Export logs for connection failures only.',
      'TRANSLATIONS_ONLY': 'Export logs for successful connections only.',
  }
  parser.add_argument(
      '--enable-logging',
      action='store_true',
      default=None,
      help=enable_logging_help_text)
  parser.add_argument(
      '--log-filter', help=log_filter_help_text, choices=filter_choices)


def _AddDrainNatIpsArgument(parser):
  drain_ips_group = parser.add_mutually_exclusive_group(required=False)
  DRAIN_NAT_IP_ADDRESSES_ARG.AddArgument(parser, mutex_group=drain_ips_group)
  drain_ips_group.add_argument(
      '--clear-nat-external-drain-ip-pool',
      action='store_true',
      default=False,
      help='Clear the drained NAT IPs')


def _AddEndpointIndependentMappingArg(parser):
  help_text = textwrap.dedent("""\
  Enable endpoint-independent mapping for the NAT (as defined in RFC 5128).

  If not specified, NATs have endpoint-independent mapping enabled by default.

  Use `--no-enable-endpoint-independent-mapping` to disable endpoint-independent
  mapping.
  """)
  parser.add_argument('--enable-endpoint-independent-mapping',
                      action='store_true',
                      default=None,
                      help=help_text)


def _AddClearableArgument(parser,
                          for_create,
                          arg_name,
                          arg_type,
                          arg_help,
                          clear_help,
                          choices=None):
  """Adds an argument for a field that can be cleared in an update."""
  if for_create:
    parser.add_argument(
        '--{}'.format(arg_name), type=arg_type, help=arg_help, choices=choices)
  else:
    mutex = parser.add_mutually_exclusive_group(required=False)
    mutex.add_argument(
        '--{}'.format(arg_name), type=arg_type, help=arg_help, choices=choices)
    mutex.add_argument(
        '--clear-{}'.format(arg_name),
        action='store_true',
        default=False,
        help=clear_help)
