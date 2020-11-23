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
"""Command to create a trigger."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from googlecloudsdk.api_lib.eventarc import triggers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.eventarc import flags
from googlecloudsdk.command_lib.eventarc import types
from googlecloudsdk.core import log

_DETAILED_HELP = {
    'DESCRIPTION':
        '{description}',
    'EXAMPLES':
        """ \
        To create a new trigger ``my-trigger'' for events of type ``google.cloud.pubsub.topic.v1.messagePublished'' with destination Cloud Run service ``my-service'', run:

          $ {command} my-trigger --matching-criteria="type=google.cloud.pubsub.topic.v1.messagePublished" --destination-run-service=my-service
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class Create(base.CreateCommand):
  """Create an Eventarc trigger."""

  detailed_help = _DETAILED_HELP

  @staticmethod
  def Args(parser):
    flags.AddTriggerResourceArg(parser, 'The trigger to create.', required=True)
    flags.AddMatchingCriteriaArg(parser, required=True)
    flags.AddServiceAccountArg(parser)
    flags.AddDestinationRunServiceArg(parser, required=True)
    flags.AddDestinationRunPathArg(parser)
    flags.AddDestinationRunRegionArg(parser)
    base.ASYNC_FLAG.AddToParser(parser)

  def Run(self, args):
    """Run the create command."""
    client = triggers.TriggersClient()
    trigger_ref = args.CONCEPTS.trigger.Parse()
    operation = client.Create(trigger_ref, args.matching_criteria,
                              args.service_account,
                              args.destination_run_service,
                              args.destination_run_path,
                              args.destination_run_region)
    self._event_type = args.matching_criteria['type']
    if args.async_:
      return operation
    response = client.WaitFor(operation)
    trigger_dict = encoding.MessageToPyValue(response)
    if types.IsPubsubType(self._event_type):
      log.status.Print('Created Pub/Sub topic [{}].'.format(
          trigger_dict['transport']['pubsub']['topic']))
      log.status.Print(
          'Publish to this topic to receive events in Cloud Run service [{}].'
          .format(args.destination_run_service))
    return response

  def Epilog(self, resources_were_displayed):
    if resources_were_displayed and types.IsAuditLogType(self._event_type):
      log.warning(
          'It may take up to {} minutes for the new trigger to become active.'
          .format(triggers.MAX_ACTIVE_DELAY_MINUTES))
