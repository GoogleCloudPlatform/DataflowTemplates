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
"""Delete a root certificate authority."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from dateutil import tz

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.privateca import request_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.privateca import operations
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.core import log
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.util import times


class Delete(base.DeleteCommand):
  r"""Schedule a root certificate authority for deletion.

    Schedule a root certificate authority for deletion in 30 days.

    Note that any user-managed KMS keys or Google Cloud Storage buckets
    will not be affected by this operation. You will need to delete the user-
    managed resources separately once the CA is deleted. Any Google-managed
    resources will be cleaned up.

    The CA specified in this command MUST:

      1) be disabled.
      2) have no un-revoked or un-expired certificates. Use the revoke command
         to revoke any active certificates.

    You can use the restore command to halt this process.

    ## EXAMPLES

    To schedule a root CA for deletion:

      $ {command} prod-root --location='us-west1'

    To schedule a root CA for deletion while skipping the confirmation
    input:

      $ {command} prod-root --location='us-west1' --quiet

    To un-do the scheduled deletion for a root CA:

      $ {parent_command} restore prod-root --location='us-west1'

  """

  @staticmethod
  def Args(parser):
    resource_args.AddCertificateAuthorityPositionalResourceArg(
        parser, 'to schedule deletion for')

  def Run(self, args):
    client = privateca_base.GetClientInstance()
    messages = privateca_base.GetMessagesModule()

    ca_ref = args.CONCEPTS.certificate_authority.Parse()

    if not console_io.PromptContinue(
        message='You are about to schedule Certificate Authority [{}] for deletion in 30 days'
        .format(ca_ref.RelativeName()),
        default=True):
      log.status.Print('Aborted by user.')
      return

    current_ca = client.projects_locations_certificateAuthorities.Get(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesGetRequest(
            name=ca_ref.RelativeName()))

    resource_args.CheckExpectedCAType(
        messages.CertificateAuthority.TypeValueValuesEnum.SELF_SIGNED,
        current_ca)

    operation = client.projects_locations_certificateAuthorities.ScheduleDelete(
        messages
        .PrivatecaProjectsLocationsCertificateAuthoritiesScheduleDeleteRequest(
            name=ca_ref.RelativeName(),
            scheduleDeleteCertificateAuthorityRequest=messages
            .ScheduleDeleteCertificateAuthorityRequest(
                requestId=request_utils.GenerateRequestId())))

    ca_response = operations.Await(operation, 'Scheduling Root CA for deletion')
    ca = operations.GetMessageFromResponse(ca_response,
                                           messages.CertificateAuthority)

    formatted_deletion_time = times.ParseDateTime(ca.deleteTime).astimezone(
        tz.tzutc()).strftime('%Y-%m-%dT%H:%MZ')

    log.status.Print('Scheduled Root CA [{}] for deletion at {}.'.format(
        ca_ref.RelativeName(), formatted_deletion_time))
