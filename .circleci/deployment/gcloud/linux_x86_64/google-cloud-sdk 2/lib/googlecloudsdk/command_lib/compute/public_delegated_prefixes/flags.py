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
"""Flags for the `compute public-delegated-prefixes` commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import flags as compute_flags


PUBLIC_DELEGATED_PREFIX_FLAG_ARG = compute_flags.ResourceArgument(
    name='--public-delegated-prefix',
    resource_name='public delegated prefix',
    global_collection='compute.globalPublicDelegatedPrefixes',
    regional_collection='compute.publicDelegatedPrefixes',
    region_explanation=compute_flags.REGION_PROPERTY_EXPLANATION)


def MakePublicDelegatedPrefixesArg():
  return compute_flags.ResourceArgument(
      resource_name='public delegated prefix',
      regional_collection='compute.publicDelegatedPrefixes',
      global_collection='compute.globalPublicDelegatedPrefixes')


def AddCreatePdpArgsToParser(parser):
  """Adds flags for public delegated prefixes create command."""
  parser.add_argument(
      '--public-advertised-prefix',
      required=True,
      help='Public advertised prefix that this delegated prefix is created from.'
  )
  parser.add_argument(
      '--range',
      help='IPv4 range from this public delegated prefix that should be '
           'delegated, in CIDR format. If not specified, the entire range of'
           'the public advertised prefix is delegated.'
  )
  parser.add_argument(
      '--description',
      help='Description of this public delegated prefix.'
  )


def _AddCommonSubPrefixArgs(parser, verb):
  """Adds common flags for delegate sub prefixes create/delete commands."""
  parser.add_argument(
      'name',
      help='Name of the delegated sub prefix to {}.'.format(verb)
  )
  PUBLIC_DELEGATED_PREFIX_FLAG_ARG.AddArgument(
      parser, operation_type='{} the delegate sub prefix for'.format(verb))


def AddCreateSubPrefixArgs(parser):
  """Adds flags for delegate sub prefixes create command."""
  _AddCommonSubPrefixArgs(parser, 'create')
  parser.add_argument(
      '--range',
      help='IPv4 range from this public delegated prefix that should be '
           'delegated, in CIDR format. If not specified, the entire range of'
           'the public advertised prefix is delegated.'
  )
  parser.add_argument(
      '--description',
      help='Description of the delegated sub prefix to create.'
  )
  parser.add_argument(
      '--delegatee-project',
      help='Project to delegate the IPv4 range specified in `--range` to. '
      'If empty, the sub-range is delegated to the same/existing project.'
  )
  parser.add_argument(
      '--create-addresses',
      action='store_true',
      help='Specify if the sub prefix is delegated to create address '
      'resources in the delegatee project. Default is false.'
  )


def AddDeleteSubPrefixArgs(parser):
  """Adds flags for delegate sub prefixes delete command."""
  _AddCommonSubPrefixArgs(parser, 'delete')
