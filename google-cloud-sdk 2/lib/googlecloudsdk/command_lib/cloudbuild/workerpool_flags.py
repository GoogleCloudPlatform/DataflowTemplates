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
"""Set up flags for creating or updating a workerpool."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base

_CREATE_FILE_DESC = ('A file that contains the configuration for the '
                     'WorkerPool to be created.')
_UPDATE_FILE_DESC = ('A file that contains updates to the configuration for '
                     'the WorkerPool.')


def AddWorkerpoolArgs(parser, release_track, update=False):
  """Set up all the argparse flags for creating or updating a workerpool.

  Args:
    parser: An argparse.ArgumentParser-like object.
    release_track: The desired value of the enum
      googlecloudsdk.calliope.base.ReleaseTrack.
    update: If true, use the version of the flags for updating a workerpool.
      Otherwise, use the version for creating a workerpool.

  Returns:
    The parser argument with workerpool flags added in.
  """
  verb = 'update' if update else 'create'
  if release_track != base.ReleaseTrack.ALPHA:
    parser.add_argument(
        '--region',
        required=True,
        help='The Cloud region where the WorkerPool is %sd.' % verb)
  file_or_flags = parser.add_mutually_exclusive_group(required=True)
  file_or_flags.add_argument(
      '--config-from-file',
      help=(_UPDATE_FILE_DESC if update else _CREATE_FILE_DESC),
  )
  flags = file_or_flags.add_argument_group(
      'Command-line flags to configure the WorkerPool:')
  flags.add_argument(
      'WORKER_POOL',
      help='The unique identifier for the custom worker pool to %s. This value should be 1-63 characters, and valid characters are [a-z][0-9]-'
      % verb,
  )
  if release_track == base.ReleaseTrack.ALPHA:
    flags.add_argument(
        '--region', help='The Cloud region where the WorkerPool is.')
  flags.add_argument(
      '--peered-network',
      help="""\
VPC network to which workers are peered.

If not specified, the workers are not peered to any network.
""")
  worker_flags = flags.add_argument_group(
      'Configuration to be used for creating workers in the WorkerPool:')
  worker_flags.add_argument(
      '--worker-machine-type',
      help="""\
Compute Engine machine type for a worker pool.

For information on supported machine types, see https://cloud.google.com/cloud-build/docs/custom-workers/worker-pool-config-file.

If unspecified, Cloud Build uses a standard machine type.
""")
  worker_flags.add_argument(
      '--worker-disk-size',
      type=arg_parsers.BinarySize(lower_bound='100GB'),
      help="""\
Size of the disk attached to the worker.

If not given, Cloud Build will use a standard disk size.
""")
  return parser


def AddWorkerpoolCreateArgs(parser, release_track):
  """Set up all the argparse flags for creating a workerpool.

  Args:
    parser: An argparse.ArgumentParser-like object.
    release_track: The desired value of the enum
      googlecloudsdk.calliope.base.ReleaseTrack.

  Returns:
    The parser argument with workerpool flags added in.
  """
  return AddWorkerpoolArgs(parser, release_track, update=False)


def AddWorkerpoolUpdateArgs(parser, release_track):
  """Set up all the argparse flags for updating a workerpool.

  Args:
    parser: An argparse.ArgumentParser-like object.
    release_track: The desired value of the enum
      googlecloudsdk.calliope.base.ReleaseTrack.

  Returns:
    The parser argument with workerpool flags added in.
  """
  return AddWorkerpoolArgs(parser, release_track, update=True)
