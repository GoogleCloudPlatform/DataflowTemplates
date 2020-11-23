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
"""'notebooks instances is-upgradeable' command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.notebooks import instances as instance_util
from googlecloudsdk.api_lib.notebooks import util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.notebooks import flags

DETAILED_HELP = {
    'DESCRIPTION':
        """
        Request for checking if a notebook instance is upgradeable.
    """,
    'EXAMPLES':
        """
    To check if an instance can be upgraded, run:

        $ {command} example-instance --location=us-central1-a
    """,
}


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class IsUpgradeable(base.DescribeCommand):
  """Request for checking if a notebook instance is upgradeable."""

  @staticmethod
  def Args(parser):
    """Register flags for this command."""
    flags.AddIsUpgradeableInstanceFlags(parser)

  def Run(self, args):
    instance_service = util.GetClient().projects_locations_instances
    result = instance_service.IsUpgradeable(
        instance_util.CreateInstanceIsUpgradeableRequest(args))
    return result


IsUpgradeable.detailed_help = DETAILED_HELP
