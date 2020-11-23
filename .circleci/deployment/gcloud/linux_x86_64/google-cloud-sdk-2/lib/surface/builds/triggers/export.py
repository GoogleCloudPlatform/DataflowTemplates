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
"""Export Cloud Build trigger to file command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from googlecloudsdk.api_lib.cloudbuild import cloudbuild_util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.cloudbuild import resource_args
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.core import properties
from googlecloudsdk.core import yaml
from googlecloudsdk.core.util import files


class Export(base.Command):
  """Export a build trigger."""

  detailed_help = {
      'EXAMPLES':
          """\
          To export a trigger to a file called trigger.yaml:

            $ {command} my-trigger --destination=trigger.yaml
          """,
  }

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """

    concept_parsers.ConceptParser.ForResource(
        'TRIGGER',
        resource_args.GetTriggerResourceSpec(),
        'Build Trigger.',
        required=True).AddToParser(parser)

    parser.add_argument(
        '--destination',
        metavar='PATH',
        required=True,
        help="""\
File path where trigger should be exported.
        """)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.
    """
    client = cloudbuild_util.GetClientInstance()
    messages = cloudbuild_util.GetMessagesModule()

    project = properties.VALUES.core.project.Get(required=True)
    trigger = client.projects_triggers.Get(
        messages.CloudbuildProjectsTriggersGetRequest(
            projectId=project, triggerId=args.TRIGGER))
    with files.FileWriter(args.destination) as out:
      yaml.dump(encoding.MessageToDict(trigger), stream=out)
