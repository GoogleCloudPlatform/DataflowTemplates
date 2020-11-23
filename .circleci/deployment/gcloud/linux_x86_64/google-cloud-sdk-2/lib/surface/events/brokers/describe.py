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
"""Command for describing a broker."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.events import eventflow_operations
from googlecloudsdk.command_lib.events import exceptions
from googlecloudsdk.command_lib.events import flags
from googlecloudsdk.command_lib.events import resource_args
from googlecloudsdk.command_lib.run import connection_context
from googlecloudsdk.command_lib.run import flags as serverless_flags
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs

SerializedTriggerAndSource = collections.namedtuple(
    'SerializedTriggerAndSource', 'serialized_trigger serialized_source')


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class Describe(base.Command):
  """Get details about a given broker."""

  detailed_help = {
      'DESCRIPTION':
          """\
          {description}
          """,
      'EXAMPLES':
          """\
          To get details about a given broker:

              $ {command} BROKER
          """,
  }

  @staticmethod
  def Args(parser):
    flags.AddBrokerArg(parser)
    namespace_presentation = presentation_specs.ResourcePresentationSpec(
        '--namespace',
        resource_args.GetCoreNamespaceResourceSpec(),
        'Namespace to create the Broker in.',
        required=True,
        prefixes=False)
    concept_parsers.ConceptParser([namespace_presentation]).AddToParser(parser)

  def Run(self, args):
    """Executes when the user runs the describe command."""
    if serverless_flags.GetPlatform() == serverless_flags.PLATFORM_MANAGED:
      raise exceptions.UnsupportedArgumentError(
          'This command is only available with Cloud Run for Anthos.')

    conn_context = connection_context.GetConnectionContext(
        args, serverless_flags.Product.EVENTS, self.ReleaseTrack())

    namespace_ref = args.CONCEPTS.namespace.Parse()
    broker_name = args.BROKER
    broker_full_name = namespace_ref.RelativeName() + '/brokers/' + broker_name

    with eventflow_operations.Connect(conn_context) as client:
      broker_obj = client.GetBroker(broker_full_name)

    if not broker_obj:
      raise exceptions.BrokerNotFound(
          'Broker [{}] not found.'.format(broker_name))
    return broker_obj
