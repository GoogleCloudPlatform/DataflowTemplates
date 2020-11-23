# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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

"""Command to list all registries in a project and location."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.cloudiot import registries
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iot import resource_args
from googlecloudsdk.command_lib.iot import util


_FORMAT = """\
table(
    name.scope("registries"):label=ID,
    name.scope("locations").segment(0):label=LOCATION,
    mqttConfig.mqttEnabledState:label=MQTT_ENABLED
)
"""


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.GA)
class List(base.ListCommand):
  """List device registries."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
        To list all device registries in a project and region 'us-central1', run:

          $ {command} --region=us-central1
        """,
  }

  @staticmethod
  def Args(parser):
    resource_args.AddRegionResourceArg(parser, 'to list registries for')
    parser.display_info.AddFormat(_FORMAT)
    parser.display_info.AddUriFunc(util.RegistriesUriFunc)

  def Run(self, args):
    """Run the list command."""
    client = registries.RegistriesClient()

    location_ref = args.CONCEPTS.region.Parse()

    return client.List(location_ref, args.limit, args.page_size)
