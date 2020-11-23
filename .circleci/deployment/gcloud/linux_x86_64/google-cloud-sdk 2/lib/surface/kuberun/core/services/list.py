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
"""Command to list Knative services in a Kubernetes cluster."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

from googlecloudsdk.api_lib.kuberun import service
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.kuberun import flags
from googlecloudsdk.command_lib.kuberun import kuberun_command
from googlecloudsdk.command_lib.kuberun import pretty_print
from googlecloudsdk.core import exceptions

_DETAILED_HELP = {
    'EXAMPLES':
        """
        To show all Knative services in the default namespace, run

            $ {command}

        To show all Knative services in a namespace, run

            $ {command} --namespace=my-namespace

        To show all Knative services from all namespaces, run

            $ {command} --all-namespaces
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class List(kuberun_command.KubeRunCommandWithOutput, base.ListCommand):
  """Lists services in a Knative cluster."""

  detailed_help = _DETAILED_HELP
  flags = [flags.NamespaceFlagGroup(), flags.ClusterConnectionFlags()]

  @classmethod
  def Args(cls, parser):
    super(List, cls).Args(parser)
    base.ListCommand._Flags(parser)
    base.URI_FLAG.RemoveFromParser(parser)
    columns = [
        pretty_print.READY_COLUMN,
        'name:label=SERVICE',
        'namespace:label=NAMESPACE',
        'url',
        'last_modifier:label="LAST DEPLOYED BY"',
        'last_transition_time:label="LAST DEPLOYED AT"',
    ]
    parser.display_info.AddFormat('table({})'.format(','.join(columns)))

  def Command(self):
    return ['core', 'services', 'list']

  def FormatOutput(self, out, args):
    if out:
      json_object = json.loads(out)
      return [service.Service(x) for x in json_object]
    else:
      raise exceptions.Error('Cannot list services')
