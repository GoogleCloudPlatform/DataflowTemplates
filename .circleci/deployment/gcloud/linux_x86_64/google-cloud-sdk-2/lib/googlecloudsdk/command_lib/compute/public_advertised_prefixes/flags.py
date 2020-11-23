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
"""Flags for the `compute public-advertised-prefixes` commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags


def MakePublicAdvertisedPrefixesArg():
  return compute_flags.ResourceArgument(
      resource_name='public advertised prefix',
      global_collection='compute.publicAdvertisedPrefixes')


def AddCreatePapArgsToParser(parser):
  """Adds public advertised prefixes create related flags to parser."""

  parser.add_argument(
      '--range',
      required=True,
      help='IPv4 range allocated to this public advertised prefix, in CIDR format.'
  )
  parser.add_argument(
      '--dns-verification-ip',
      required=True,
      help='IPv4 address to use for verification. It must be within the IPv4 range specified in --range.'
  )
  parser.add_argument(
      '--description',
      help='Description of this public advertised prefix.'
  )


def AddUpdatePapArgsToParser(parser):
  base.ChoiceArgument(
      '--status',
      required=True,
      choices=['ptr-configured'],
      help_str='The status of public advertised prefix.'
  ).AddToParser(parser)
