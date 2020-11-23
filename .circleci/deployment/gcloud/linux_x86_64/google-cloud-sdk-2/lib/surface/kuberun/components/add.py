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
"""Command to create a new Kuberun Component."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.kuberun import flags
from googlecloudsdk.command_lib.kuberun import kuberun_command

_DETAILED_HELP = {
    'EXAMPLES':
        """
        To create a new Component, run:

            $ {command} COMPONENT --type TYPE --devkit DEVKIT
        """,
}


def _DevKitFlag():
  return flags.StringFlag(
      '--devkit',
      help='Name of the Development Kit to use for this Component.',
      required=True)


def _TypeFlag():
  return flags.StringFlag(
      '--type',
      help='Type of Component to create. See `kuberun devkits describe` for available Component Types.',
      required=True)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Add(kuberun_command.KubeRunStreamingCommand, base.CreateCommand):
  """Create a new Component."""

  detailed_help = _DETAILED_HELP
  flags = [_DevKitFlag(), _TypeFlag()]

  @classmethod
  def Args(cls, parser):
    super(Add, cls).Args(parser)
    parser.add_argument('component', help='Name of the component.')

  def Command(self):
    return ['components', 'add']

  def BuildKubeRunArgs(self, args):
    return [args.component] + super(Add, self).BuildKubeRunArgs(args)
