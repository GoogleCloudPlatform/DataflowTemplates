# Lint as: python3
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
"""Describe a reusable config."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.command_lib.util.concepts import concept_parsers


class Describe(base.DescribeCommand):
  """Show details about a reusable config."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
        To show details about a reusable config, run:

          $ {command} leaf-server-tls
        """,
  }

  @staticmethod
  def Args(parser):
    concept_parsers.ConceptParser.ForResource(
        'REUSABLE_CONFIG',
        resource_args.CreateReusableConfigResourceSpec(
            location_fallthroughs=[
                resource_args.LOCATION_PROPERTY_FALLTHROUGH]),
        'The reusable config to describe.',
        required=True).AddToParser(parser)

  def Run(self, args):
    """Runs the command."""

    reusable_config = args.CONCEPTS.reusable_config.Parse()

    client = privateca_base.GetClientInstance()
    messages = privateca_base.GetMessagesModule()

    return client.projects_locations_reusableConfigs.Get(
        messages.PrivatecaProjectsLocationsReusableConfigsGetRequest(
            name=reusable_config.RelativeName()))
