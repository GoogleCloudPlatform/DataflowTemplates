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
"""Describe a Knative revision."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

from googlecloudsdk.api_lib.kuberun import revision
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.kuberun import flags
from googlecloudsdk.command_lib.kuberun import kuberun_command
from googlecloudsdk.command_lib.kuberun import revision_printer
from googlecloudsdk.core import exceptions
from googlecloudsdk.core.resource import resource_printer

_DETAILED_HELP = {
    'EXAMPLES':
        """
        To show all the data about a Knative revision, run

            $ {command}
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Describe(kuberun_command.KubeRunCommandWithOutput, base.DescribeCommand):
  """Describes a Knative revision."""

  detailed_help = _DETAILED_HELP
  flags = [flags.ClusterConnectionFlags(), flags.NamespaceFlag()]

  @classmethod
  def Args(cls, parser):
    super(Describe, cls).Args(parser)
    parser.add_argument(
        'revision', help='The Knative revision to show details for.')
    resource_printer.RegisterFormatter(
        revision_printer.REVISION_PRINTER_FORMAT,
        revision_printer.RevisionPrinter,
        hidden=True)
    parser.display_info.AddFormat(revision_printer.REVISION_PRINTER_FORMAT)

  def BuildKubeRunArgs(self, args):
    return [args.revision] + super(Describe, self).BuildKubeRunArgs(args)

  def Command(self):
    return ['core', 'revisions', 'describe']

  def FormatOutput(self, out, args):
    if out:
      return revision.Revision(json.loads(out))
    else:
      raise exceptions.Error('Cannot find revision [{}]'.format(args.revision))
