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
"""Create a new root certificate authority."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.privateca import request_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.command_lib.privateca import create_utils
from googlecloudsdk.command_lib.privateca import flags
from googlecloudsdk.command_lib.privateca import iam
from googlecloudsdk.command_lib.privateca import operations
from googlecloudsdk.command_lib.privateca import p4sa
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.command_lib.privateca import storage
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import log


# pylint: disable=line-too-long
class Create(base.CreateCommand):
  r"""Create a new root certificate authority.

  ## EXAMPLES

  To create a root CA that supports one layer of subordinates:

      $ {command} prod-root \
        --kms-key-version="projects/joonix-pki/locations/us-west1/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1" \
        --subject="CN=Joonix Production Root CA" \
        --max-chain-length=1

  To create a root CA and restrict what it can issue:

      $ {command} prod-root \
        --kms-key-version="projects/joonix-pki/locations/us-west1/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1" \
        --subject="CN=Joonix Production Root CA" \
        --issuance-policy=policy.yaml

  To create a root CA that doesn't publicly publish CA certificate and CRLs:

      $ {command} root-2 \
        --kms-key-version="projects/joonix-pki/locations/us-west1/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1" \
        --subject="CN=Joonix Production Root CA" \
        --issuance-policy=policy.yaml \
        --no-publish-ca-cert \
        --no-publish-crl

  To create a root CA that is based on an existing CA:

      $ {command} prod-root \
        --kms-key-version="projects/joonix-pki/locations/us-west1/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1" \
        --from-ca=source-root --from-ca-location=us-central1
  """

  def __init__(self, *args, **kwargs):
    super(Create, self).__init__(*args, **kwargs)
    self.client = privateca_base.GetClientInstance()
    self.messages = privateca_base.GetMessagesModule()

  @staticmethod
  def Args(parser):
    key_spec_group = parser.add_group(
        mutex=True,
        help='The key configuration used for the CA certificate. Defaults to a '
             'managed key if not specified.')
    reusable_config_group = parser.add_group(
        mutex=True,
        required=False,
        help='The X.509 configuration used for the CA certificate.')

    concept_parsers.ConceptParser([
        presentation_specs.ResourcePresentationSpec(
            'CERTIFICATE_AUTHORITY',
            resource_args.CreateCertificateAuthorityResourceSpec(
                'Certificate Authority'),
            'The name of the root CA to create.',
            required=True),
        presentation_specs.ResourcePresentationSpec(
            '--kms-key-version',
            resource_args.CreateKmsKeyVersionResourceSpec(),
            'An existing KMS key version to back this CA.',
            group=key_spec_group),
        presentation_specs.ResourcePresentationSpec(
            '--reusable-config',
            resource_args.CreateReusableConfigResourceSpec(
                location_fallthroughs=[deps.Fallthrough(
                    function=lambda: '',
                    hint=('location will default to the same location as '
                          'the CA'),
                    active=False,
                    plural=False)]),
            'The Reusable Config containing X.509 values for this CA.',
            flag_name_overrides={
                'location': '',
                'project': '',
            },
            group=reusable_config_group),
        presentation_specs.ResourcePresentationSpec(
            '--from-ca',
            resource_args.CreateCertificateAuthorityResourceSpec(
                'source CA'),
            'An existing CA from which to copy configuration values for the new CA. '
            'You can still override any of those values by explicitly providing '
            'the appropriate flags.',
            flag_name_overrides={'project': '--from-ca-project'},
            prefixes=True)
    ]).AddToParser(parser)
    flags.AddTierFlag(parser)
    flags.AddSubjectFlags(parser, subject_required=False)
    flags.AddPublishCaCertFlag(parser, use_update_help_text=False)
    flags.AddPublishCrlFlag(parser, use_update_help_text=False)
    flags.AddKeyAlgorithmFlag(key_spec_group, default='rsa-pkcs1-4096-sha256')
    flags.AddInlineReusableConfigFlags(reusable_config_group, is_ca=True)
    flags.AddValidityFlag(
        parser,
        resource_name='CA',
        default_value='P10Y',
        default_value_text='10 years')
    flags.AddCertificateAuthorityIssuancePolicyFlag(parser)
    labels_util.AddCreateLabelsFlags(parser)
    flags.AddBucketFlag(parser)

  def Run(self, args):
    new_ca, ca_ref, _ = create_utils.CreateCAFromArgs(
        args, is_subordinate=False)
    project_ref = ca_ref.Parent().Parent()
    key_version_ref = args.CONCEPTS.kms_key_version.Parse()
    kms_key_ref = key_version_ref.Parent() if key_version_ref else None

    iam.CheckCreateCertificateAuthorityPermissions(project_ref, kms_key_ref)

    bucket_ref = None
    if args.IsSpecified('bucket'):
      bucket_ref = storage.ValidateBucketForCertificateAuthority(args.bucket)
      new_ca.gcsBucket = bucket_ref.bucket

    p4sa_email = p4sa.GetOrCreate(project_ref)
    p4sa.AddResourceRoleBindings(p4sa_email, kms_key_ref, bucket_ref)

    create_utils.PrintBetaResourceDeletionDisclaimer('certificate authorities')
    operation = self.client.projects_locations_certificateAuthorities.Create(
        self.messages
        .PrivatecaProjectsLocationsCertificateAuthoritiesCreateRequest(
            certificateAuthority=new_ca,
            certificateAuthorityId=ca_ref.Name(),
            parent=ca_ref.Parent().RelativeName(),
            requestId=request_utils.GenerateRequestId()))

    ca_response = operations.Await(operation, 'Creating Certificate Authority.')
    ca = operations.GetMessageFromResponse(ca_response,
                                           self.messages.CertificateAuthority)

    log.status.Print('Created Certificate Authority [{}].'.format(ca.name))
