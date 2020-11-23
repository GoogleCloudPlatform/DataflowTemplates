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
"""Flags and helpers for the compute service-attachment commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import completers as compute_completers
from googlecloudsdk.command_lib.compute import flags as compute_flags

DEFAULT_LIST_FORMAT = """\
    table(
      name,
      region.basename(),
      producerForwardingRule.basename()
    )"""


class ServiceAttachmentsCompleter(compute_completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(ServiceAttachmentsCompleter, self).__init__(
        collection='compute.serviceAttachments',
        list_command='compute service-attachments list --uri',
        **kwargs)


def AddDescription(parser):
  parser.add_argument(
      '--description',
      help='An optional, textual description for the service attachment.')


def AddConnectionPreference(parser):
  connection_preference_choices = {
      'ACCEPT_AUTOMATIC':
          'Always accept connection requests from consumers automatically.',
  }

  parser.add_argument(
      '--connection-preference',
      choices=connection_preference_choices,
      type=lambda x: x.replace('-', '_').upper(),
      default='ACCEPT_AUTOMATIC',
      help="This defines the service attachment's connection preference.")


def ServiceAttachmentArgument(required=True, plural=False):
  return compute_flags.ResourceArgument(
      resource_name='service attachment',
      completer=ServiceAttachmentsCompleter,
      plural=plural,
      required=required,
      regional_collection='compute.serviceAttachments',
      region_explanation=compute_flags.REGION_PROPERTY_EXPLANATION)
