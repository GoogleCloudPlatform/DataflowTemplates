# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""The command to enable Config Management Feature."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.services import services_util
from googlecloudsdk.api_lib.services import serviceusage
from googlecloudsdk.command_lib.container.hub.features import base
from googlecloudsdk.core import log
from googlecloudsdk.core import properties


class Enable(base.EnableCommand):
  """Enable Config Management Feature.

  This command enables Config Management Feature in Hub.

  ## Examples

  Enable Config Management Feature:

    $ {command}
  """

  FEATURE_NAME = 'configmanagement'
  FEATURE_DISPLAY_NAME = 'Config Management'

  @classmethod
  def Args(cls, parser):
    pass

  def Run(self, args):
    enable_service()
    return self.RunCommand(args, configmanagementFeatureSpec=(
        base.CreateConfigManagementFeatureSpec()))


def enable_service():
  project = properties.VALUES.core.project.Get(required=True)
  service_name = 'anthosconfigmanagement.googleapis.com'
  op = serviceusage.EnableApiCall(project, service_name)
  log.status.Print('Enabling service {0}'.format(service_name))
  if op.done:
    return
  op = services_util.WaitOperation(op.name, serviceusage.GetOperation)
  services_util.PrintOperation(op)
