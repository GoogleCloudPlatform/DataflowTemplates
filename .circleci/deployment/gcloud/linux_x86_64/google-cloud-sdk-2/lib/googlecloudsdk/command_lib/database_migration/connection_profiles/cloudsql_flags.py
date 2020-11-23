# -*- coding: utf-8 -*- #
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Flags and helpers for the connection profiles cloudsql related commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import arg_parsers


_IP_ADDRESS_PART = r'(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})'  # Match decimal 0-255
_CIDR_PREFIX_PART = r'([0-9]|[1-2][0-9]|3[0-2])'  # Match decimal 0-32
# Matches either IPv4 range in CIDR notation or a naked IPv4 address.
_CIDR_REGEX = r'{addr_part}(\.{addr_part}){{3}}(\/{prefix_part})?$'.format(
    addr_part=_IP_ADDRESS_PART, prefix_part=_CIDR_PREFIX_PART)


def AddDatabaseVersionFlag(parser):
  """Adds a --database-version flag to the given parser."""
  help_text = """\
    Database engine type and version (MYSQL_5_7, MYSQL_5_6, POSTGRES_9_6,
    POSTGRES_11).
    """
  choices = ['MYSQL_5_7', 'MYSQL_5_6', 'POSTGRES_9_6', 'POSTGRES_11']
  parser.add_argument(
      '--database-version', help=help_text, choices=choices, required=True)


def AddUserLabelsFlag(parser):
  """Adds a --user-labels flag to the given parser."""
  help_text = """\
    The resource labels for a Cloud SQL instance to use to annotate any related
    underlying resources such as Compute Engine VMs. An object containing a list
    of "key": "value" pairs.
    """
  parser.add_argument('--user-labels',
                      metavar='KEY=VALUE',
                      type=arg_parsers.ArgDict(),
                      help=help_text)


def AddTierFlag(parser):
  """Adds a --tier flag to the given parser."""
  help_text = """\
    Tier (or machine type) for this instance, for example: ``db-n1-standard-1''
    (MySQL instances) of ``db-custom-1-3840'' (PostgreSQL instances). For more
    information, see
    [Cloud SQL Instance Settings](https://cloud.google.com/sql/docs/mysql/instance-settings).
    """
  parser.add_argument('--tier', help=help_text, required=True)


def AddStorageAutoResizeLimitFlag(parser):
  """Adds a --storage-auto-resize-limit flag to the given parser."""
  help_text = """\
    Maximum size to which storage capacity can be automatically increased. The
    default value is 0, which specifies that there is no limit.
    """
  parser.add_argument('--storage-auto-resize-limit', type=int, help=help_text)


def AddActivationPolicylag(parser):
  """Adds a --activation-policy flag to the given parser."""
  help_text = """\
    Activation policy specifies when the instance is activated; it is
    applicable only when the instance state is 'RUNNABLE'. Valid values:

    ALWAYS: The instance is on, and remains so even in the absence of
    connection requests.

    NEVER: The instance is off; it is not activated, even if a connection
    request arrives.
    """
  choices = ['ALWAYS', 'NEVER']
  parser.add_argument('--activation-policy', help=help_text, choices=choices)


def AddEnableIpv4Flag(parser):
  """Adds a --enable-ip-v4 flag to the given parser."""
  help_text = 'Whether the instance should be assigned an IPv4 address or not.'
  parser.add_argument('--enable-ip-v4', type=bool, help=help_text)


def AddPrivateNetworkFlag(parser):
  """Adds a --private-network flag to the given parser."""
  help_text = """\
    Resource link for the VPC network from which the Cloud SQL instance is
    accessible for private IP. For example,
    /projects/myProject/global/networks/default. This setting can be updated,
    but it cannot be removed after it is set.
    """
  parser.add_argument('--private-network', help=help_text)


def AddRequireSslFlag(parser):
  """Adds a --require-ssl flag to the given parser."""
  help_text = 'Whether SSL connections over IP should be enforced or not.'
  parser.add_argument('--require-ssl', type=bool, help=help_text)


def AddAuthorizedNetworksFlag(parser):
  """Adds a `--authorized-networks` flag."""
  cidr_validator = arg_parsers.RegexpValidator(
      _CIDR_REGEX, ('Must be specified in CIDR notation, also known as '
                    '\'slash\' notation (e.g. 192.168.100.0/24).'))
  help_text = """\
    List of external networks that are allowed to connect to the instance using
    the IP. See https://en.wikipedia.org/wiki/CIDR_notation#CIDR_notation, also
    known as 'slash' notation (e.g.192.168.100.0/24).
    """
  parser.add_argument(
      '--authorized-networks',
      type=arg_parsers.ArgList(min_length=1, element_type=cidr_validator),
      metavar='NETWORK',
      default=[],
      help=help_text)


def AddAutoStorageIncreaseFlag(parser):
  """Adds a --auto-storage-increase flag to the given parser."""
  help_text = """\
    If you enable this setting, Cloud SQL checks your available storage every
    30 seconds. If the available storage falls below a threshold size, Cloud
    SQL automatically adds additional storage capacity. If the available
    storage repeatedly falls below the threshold size, Cloud SQL continues to
    add storage until it reaches the maximum of 30 TB. Default: ON.
    """
  parser.add_argument('--auto-storage-increase', type=bool, help=help_text)


def AddDatabaseFlagsFlag(parser):
  """Adds a --database-flags flag to the given parser."""
  help_text = """\
    Database flags passed to the Cloud SQL instance at startup. An object
    containing a list of "key": value pairs. Example: { "name": "wrench",
    "mass": "1.3kg", "count": "3" }.
  """
  parser.add_argument('--database-flags',
                      type=arg_parsers.ArgDict(),
                      metavar='KEY=VALUE',
                      help=help_text)


def AddDataDiskTypeFlag(parser):
  """Adds a --data-disk-type flag to the given parser."""
  help_text = 'Type of storage: PD_SSD (default) or PD_HDD.'
  choices = ['PD_SSD', 'PD_HDD']
  parser.add_argument('--data-disk-type', help=help_text, choices=choices)


def AddDataDiskSizeFlag(parser):
  """Adds a --data-disk-size flag to the given parser."""
  help_text = """\
    Storage capacity available to the database, in GB. The minimum (and
    default) size is 10GB.
  """
  parser.add_argument('--data-disk-size', type=int, help=help_text)


def AddZoneFlag(parser):
  """Adds a --zone flag to the given parser."""
  help_text = """\
    Google Cloud Platform zone where your Cloud SQL datdabse instance is
    located.
  """
  parser.add_argument('--zone', help=help_text)
