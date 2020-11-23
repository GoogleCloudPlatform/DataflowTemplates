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
"""`gcloud domains registrations configure management` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.domains import registrations
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.domains import flags
from googlecloudsdk.command_lib.domains import resource_args
from googlecloudsdk.command_lib.domains import util
from googlecloudsdk.core import log


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class ConfigureManagement(base.UpdateCommand):
  """Configure management settings of a Cloud Domains registration.

  Configure management settings of a registration. This includes settings
  related to transfers, billing and renewals of a registration.

  ## EXAMPLES

  To unlock a transfer lock of a registration for ``example.com'', run:

    $ {command} example.com --transfer-lock-state=unlocked
  """

  @staticmethod
  def Args(parser):
    resource_args.AddRegistrationResourceArg(
        parser, 'to configure management settings for')
    flags.AddManagementSettingsFlagsToParser(parser)
    flags.AddAsyncFlagToParser(parser)

  def Run(self, args):
    api_version = registrations.GetApiVersionFromArgs(args)
    client = registrations.RegistrationsClient(api_version)
    args.registration = util.NormalizeResourceName(args.registration)
    registration_ref = args.CONCEPTS.registration.Parse()

    registration = client.Get(registration_ref)
    util.AssertRegistrationOperational(api_version, registration)

    transfer_lock_state = util.ParseTransferLockState(api_version,
                                                      args.transfer_lock_state)
    if transfer_lock_state is None:
      transfer_lock_state = util.PromptForTransferLockState(
          api_version, registration.managementSettings.transferLockState)
      if transfer_lock_state is None:
        return None

    response = client.ConfigureManagement(registration_ref, transfer_lock_state)

    response = util.WaitForOperation(api_version, response, args.async_)
    log.UpdatedResource(registration_ref.Name(), 'registration', args.async_)
    return response
