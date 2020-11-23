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
"""Command to list all triggers in a project and location."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.eventarc import triggers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.eventarc import flags
from googlecloudsdk.command_lib.eventarc import types

_DETAILED_HELP = {
    'DESCRIPTION':
        '{description}',
    'EXAMPLES':
        """ \
        To list all triggers in location ``us-central1'', run:

          $ {command} --location=us-central1
        """,
}

_FORMAT = """ \
table(
    name.scope("triggers"):label=NAME,
    matchingCriteria.type():label=TYPE,
    destination.cloudRunService.service:label=DESTINATION_RUN_SERVICE,
    destination.cloudRunService.path:label=DESTINATION_RUN_PATH,
    active_status():label=ACTIVE
)
"""


def _ActiveStatus(trigger):
  event_type = types.MatchingCriteriaDictToType(trigger['matchingCriteria'])
  active_time = triggers.TriggerActiveTime(event_type, trigger['updateTime'])
  return 'By {}'.format(active_time) if active_time else 'Yes'


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class List(base.ListCommand):
  """List Eventarc triggers."""

  detailed_help = _DETAILED_HELP

  @staticmethod
  def Args(parser):
    flags.AddLocationResourceArg(
        parser,
        "The location for which to list triggers. This should be either "
        "``global'' or one of the supported regions.",
        required=True)
    parser.display_info.AddFormat(_FORMAT)
    parser.display_info.AddUriFunc(triggers.GetTriggerURI)
    parser.display_info.AddTransforms({
        'active_status': _ActiveStatus,
        'type': types.MatchingCriteriaDictToType
    })

  def Run(self, args):
    """Run the list command."""
    client = triggers.TriggersClient()
    location_ref = args.CONCEPTS.location.Parse()
    return client.List(location_ref, args.limit, args.page_size)
