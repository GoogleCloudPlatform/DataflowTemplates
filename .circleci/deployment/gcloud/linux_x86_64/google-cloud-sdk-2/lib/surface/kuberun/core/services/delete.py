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
"""Delete a Knative service."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.kuberun import flags
from googlecloudsdk.command_lib.kuberun import kuberun_command
from googlecloudsdk.core import log
from googlecloudsdk.core.console import console_io

_DETAILED_HELP = {
    'EXAMPLES': """
        To delete a Knative service, run:

            $ {command} <service-name>
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Delete(kuberun_command.KubeRunCommand, base.DeleteCommand):
  """Deletes a Knative service."""

  detailed_help = _DETAILED_HELP
  flags = [
      flags.NamespaceFlag(),
      flags.ClusterConnectionFlags(),
      flags.AsyncFlag()
  ]

  @classmethod
  def Args(cls, parser):
    super(Delete, cls).Args(parser)
    parser.add_argument('service',
                        help='The Knative service to delete.')

  def OperationResponseHandler(self, response, args):
    if response.failed:
      log.error(response.stderr)
      return None

    if response.stderr:
      log.status.Print(response.stderr)

    log.status.Print('Service is successfully deleted.')
    return None

  def BuildKubeRunArgs(self, args):
    return [args.service] + super(Delete, self).BuildKubeRunArgs(args)

  def Run(self, args):
    """Delete a service."""
    console_io.PromptContinue(
        message='Service [{service}] will be deleted.'.format(
            service=args.service),
        throw_if_unattended=True,
        cancel_on_no=True)
    return super(Delete, self).Run(args)

  def Command(self):
    return ['core', 'services', 'delete']
