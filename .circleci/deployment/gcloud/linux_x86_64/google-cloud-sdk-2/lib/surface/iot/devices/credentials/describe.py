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

"""`gcloud iot devices credentials describe` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.cloudiot import devices
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iot import flags
from googlecloudsdk.command_lib.iot import resource_args
from googlecloudsdk.command_lib.iot import util


class Describe(base.DescribeCommand):
  """Show details about a specific device credential."""

  detailed_help = {
      'EXAMPLES':
          """\
          To describe the first credential of a device in region 'us-central1', run:

            $ {command} --region=us-central1 --registry=my-registry --device=my-device 0
          """,
  }

  @staticmethod
  def Args(parser):
    resource_args.AddDeviceResourceArg(parser,
                                       'to which the credential belongs',
                                       positional=False)
    flags.GetIndexFlag('credential', 'to describe').AddToParser(parser)

  def Run(self, args):
    client = devices.DevicesClient()

    device_ref = args.CONCEPTS.device.Parse()

    credentials = client.Get(device_ref).credentials
    try:
      return credentials[args.index]
    except IndexError:
      raise util.BadCredentialIndexError(device_ref.Name(), credentials,
                                         args.index)
