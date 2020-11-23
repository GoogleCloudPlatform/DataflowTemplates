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
"""Restore a subordinate Certificate Authority."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.privateca import request_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.privateca import operations
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.core import log


class Restore(base.SilentCommand):
  r"""Restore a subordinate Certificate Authority.

    Restores a subordinate Certificate Authority that has been marked for
    deletion. A Certificate Authority can be restored within 30 days of being
    scheduled for deletion. Use this command to halt the deletion process. A
    restored CA will move to DISABLED state.

    ## EXAMPLES

    To restore a subordinate CA:

        $ {command} server-tls-1 --location=us-west1
  """

  @staticmethod
  def Args(parser):
    resource_args.AddCertificateAuthorityPositionalResourceArg(
        parser, 'to restore')

  def Run(self, args):
    client = privateca_base.GetClientInstance()
    messages = privateca_base.GetMessagesModule()

    ca_ref = args.CONCEPTS.certificate_authority.Parse()

    current_ca = client.projects_locations_certificateAuthorities.Get(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesGetRequest(
            name=ca_ref.RelativeName()))

    resource_args.CheckExpectedCAType(
        messages.CertificateAuthority.TypeValueValuesEnum.SUBORDINATE,
        current_ca)

    operation = client.projects_locations_certificateAuthorities.Restore(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesRestoreRequest(
            name=ca_ref.RelativeName(),
            restoreCertificateAuthorityRequest=messages
            .RestoreCertificateAuthorityRequest(
                requestId=request_utils.GenerateRequestId())))

    operations.Await(operation, 'Restoring Subordinate CA')

    log.status.Print('Restored Subordinate CA [{}].'.format(
        ca_ref.RelativeName()))
