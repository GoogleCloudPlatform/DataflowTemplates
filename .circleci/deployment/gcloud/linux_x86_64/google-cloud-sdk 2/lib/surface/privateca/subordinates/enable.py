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
"""Enable a subordinate certificate authority."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.privateca import request_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.privateca import operations
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.core import log


class Enable(base.SilentCommand):
  r"""Enable a subordinate certificate authority.

    Enables a subordinate certificate authority. The
    subordinate certificate authority will be allowed to issue certificates once
    enabled.

    ## EXAMPLES

    To enable a subordinate CA:

        $ {command} server-tls1 --location=us-west1
  """

  @staticmethod
  def Args(parser):
    resource_args.AddCertificateAuthorityPositionalResourceArg(
        parser, 'to enable')

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

    operation = client.projects_locations_certificateAuthorities.Enable(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesEnableRequest(
            name=ca_ref.RelativeName(),
            enableCertificateAuthorityRequest=messages
            .EnableCertificateAuthorityRequest(
                requestId=request_utils.GenerateRequestId())))

    operations.Await(operation, 'Enabling Subordinate CA')

    log.status.Print('Enabled Subordinate CA [{}].'.format(
        ca_ref.RelativeName()))
