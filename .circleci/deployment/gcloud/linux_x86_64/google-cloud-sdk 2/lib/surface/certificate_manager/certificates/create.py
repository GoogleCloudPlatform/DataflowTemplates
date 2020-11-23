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
"""`gcloud certificate-manager certificates create` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.certificate_manager import certificates
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.certificate_manager import flags
from googlecloudsdk.command_lib.certificate_manager import resource_args
from googlecloudsdk.command_lib.certificate_manager import util
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import log


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Create(base.CreateCommand):
  """Create a certificate.

  This command creates a certificate.

  ## EXAMPLES

  To create a certificate with name simple-cert, run:

    $ {command} simple-cert --certificate-file=cert.pem
        --private-key-file=key.pem
  """

  @staticmethod
  def Args(parser):
    resource_args.AddCertificateResourceArg(parser, 'to create')
    labels_util.AddCreateLabelsFlags(parser)
    flags.AddDescriptionFlagToParser(parser, 'certificate')
    flags.AddSelfManagedCertificateDataFlagsToParser(parser, is_required=True)
    flags.AddAsyncFlagToParser(parser)

  def Run(self, args):
    client = certificates.CertificateClient()
    cert_ref = args.CONCEPTS.certificate.Parse()
    location_ref = cert_ref.Parent()
    labels = labels_util.ParseCreateArgs(
        args, client.messages.Certificate.LabelsValue)

    response = client.Create(
        location_ref,
        cert_ref.certificatesId,
        self_managed_cert_data=client.messages.SelfManagedCertData(
            certificatePem=args.certificate_file.encode('utf-8'),
            privateKeyPem=args.private_key_file.encode('utf-8'),
        ),
        description=args.description,
        labels=labels)
    operation_response = util.WaitForOperation(response, is_async=args.async_)
    log.CreatedResource(cert_ref.Name(), 'certificate', is_async=args.async_)
    return operation_response
