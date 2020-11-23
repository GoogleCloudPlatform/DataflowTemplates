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
"""Command to list values for the serviceName CloudEvents attribute."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.eventarc import flags
from googlecloudsdk.command_lib.eventarc import service_catalog
from googlecloudsdk.command_lib.eventarc import types

_DETAILED_HELP = {
    'DESCRIPTION':
        '{description}',
    'EXAMPLES':
        """ \
        To list serviceName values for event type ``google.cloud.audit.log.v1.written'', run:

          $ {command} --type=google.cloud.audit.log.v1.written
        """,
}

_FORMAT = 'table(service_name, display_name)'


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class List(base.ListCommand):
  """List values for the serviceName CloudEvents attribute."""

  detailed_help = _DETAILED_HELP

  @staticmethod
  def Args(parser):
    flags.AddTypeArg(parser, required=True)
    base.PAGE_SIZE_FLAG.RemoveFromParser(parser)
    base.URI_FLAG.RemoveFromParser(parser)
    parser.display_info.AddFormat(_FORMAT)

  def Run(self, args):
    """Run the list command."""
    types.ValidateAuditLogEventType(args.type)
    return service_catalog.GetServices()
