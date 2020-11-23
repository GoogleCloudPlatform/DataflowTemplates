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
"""Command for initializing a cloud run events namespaces."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.events import anthosevents_operations
from googlecloudsdk.command_lib.events import eventflow_operations
from googlecloudsdk.command_lib.events import exceptions
from googlecloudsdk.command_lib.events import flags
from googlecloudsdk.command_lib.events import resource_args
from googlecloudsdk.command_lib.run import connection_context
from googlecloudsdk.command_lib.run import flags as serverless_flags
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import log


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class Init(base.Command):
  """Initialize a namespace for eventing."""

  detailed_help = {
      'DESCRIPTION':
          """
      {description}
      Copies the necessary google-cloud-sources-key secret to the specified namespace, which is necessary when using secrets for authentication.

      This command is only available with Cloud Run for Anthos.
      """,
      'EXAMPLES':
          """
      To initialize a namespace:

          $ {command}
      """,
  }

  @staticmethod
  def Args(parser):
    flags.AddCopyDefaultSecret(parser)
    namespace_presentation = presentation_specs.ResourcePresentationSpec(
        'namespace',
        resource_args.GetCoreNamespaceResourceSpec(),
        'Namespace to copy secret to.',
        required=True,
        prefixes=False)
    concept_parsers.ConceptParser([namespace_presentation]).AddToParser(parser)

  def Run(self, args):
    """Executes when user runs the init command."""
    if serverless_flags.GetPlatform() == serverless_flags.PLATFORM_MANAGED:
      raise exceptions.UnsupportedArgumentError(
          'This command is only available with Cloud Run for Anthos.')

    conn_context = connection_context.GetConnectionContext(
        args, serverless_flags.Product.EVENTS, self.ReleaseTrack())

    namespace_ref = args.CONCEPTS.namespace.Parse()

    with eventflow_operations.Connect(conn_context) as client:
      client.CreateOrReplaceSourcesSecret(namespace_ref)

    log.status.Print('Initialized namespace [{}] for Cloud Run eventing with '
                     'secret {}'.format(namespace_ref.Name(),
                                        anthosevents_operations.SOURCES_KEY))
