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
"""Helpers for create commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions
from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.privateca import flags
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import log


def _ParseCAResourceArgs(args):
  """Parses, validates and returns the resource args from the CLI.

  Args:
    args: The parsed arguments from the command-line.

  Returns:
    Tuple containing the Resource objects for (CA, source CA, issuer).
  """
  resource_args.ValidateResourceIsCompleteIfSpecified(args, 'kms_key_version')
  resource_args.ValidateResourceIsCompleteIfSpecified(args, 'issuer')
  resource_args.ValidateResourceIsCompleteIfSpecified(args, 'from_ca')

  ca_ref = args.CONCEPTS.certificate_authority.Parse()
  resource_args.ValidateResourceLocation(ca_ref, 'CERTIFICATE_AUTHORITY')

  kms_key_version_ref = args.CONCEPTS.kms_key_version.Parse()
  if kms_key_version_ref and ca_ref.locationsId != kms_key_version_ref.locationsId:
    raise exceptions.InvalidArgumentException(
        '--kms-key-version',
        'KMS key must be in the same location as the Certificate Authority '
        '({}).'.format(ca_ref.locationsId))

  issuer_ref = args.CONCEPTS.issuer.Parse() if hasattr(args, 'issuer') else None
  source_ca_ref = args.CONCEPTS.from_ca.Parse()

  return (ca_ref, source_ca_ref, issuer_ref)


def CreateCAFromArgs(args, is_subordinate):
  """Creates a CA object from CA create flags.

  Args:
    args: The parser that contains the flag values.
    is_subordinate: If True, a subordinate CA is returned, otherwise a root CA.

  Returns:
    A tuple for the CA to create with (CA object, CA ref, issuer).
  """

  client = privateca_base.GetClientInstance()
  messages = privateca_base.GetMessagesModule()

  ca_ref, source_ca_ref, issuer_ref = _ParseCAResourceArgs(
      args)
  source_ca = None

  if source_ca_ref:
    source_ca = client.projects_locations_certificateAuthorities.Get(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesGetRequest(
            name=source_ca_ref.RelativeName()))
    if not source_ca:
      raise exceptions.InvalidArgumentException(
          '--from-ca', 'The provided source CA could not be retrieved.')

  tier = flags.ParseTierFlag(args)
  keyspec = flags.ParseKeySpec(args)
  if tier == messages.CertificateAuthority.TierValueValuesEnum.DEVOPS and keyspec.cloudKmsKeyVersion:
    raise exceptions.InvalidArgumentException(
        '--kms-key-version',
        'The DevOps tier does not support user-specified KMS keys.')

  subject_config = messages.SubjectConfig(
      subject=messages.Subject(), subjectAltName=messages.SubjectAltNames())
  if args.IsSpecified('subject'):
    subject_config.commonName, subject_config.subject = flags.ParseSubject(args)
  elif source_ca:
    subject_config.commonName = source_ca.config.subjectConfig.commonName
    subject_config.subject = source_ca.config.subjectConfig.subject

  if flags.SanFlagsAreSpecified(args):
    subject_config.subjectAltName = flags.ParseSanFlags(args)
  elif source_ca:
    subject_config.subjectAltName = source_ca.config.subjectConfig.subjectAltName
  flags.ValidateSubjectConfig(subject_config, is_ca=True)

  issuing_options = flags.ParseIssuingOptions(args)
  if source_ca and not args.IsSpecified('publish_ca_cert'):
    issuing_options.includeCaCertUrl = source_ca.issuingOptions.includeCaCertUrl
  if source_ca and not args.IsSpecified('publish_crl'):
    issuing_options.includeCrlAccessUrl = source_ca.issuingOptions.includeCrlAccessUrl

  issuance_policy = flags.ParseIssuancePolicy(args)
  if source_ca and not issuance_policy:
    issuance_policy = source_ca.certificatePolicy

  reusable_config_wrapper = flags.ParseReusableConfig(
      args, ca_ref.locationsId, is_ca=True)
  if source_ca and not flags.ReusableConfigFlagsAreSpecified(args):
    reusable_config_wrapper = source_ca.config.reusableConfig

  lifetime = flags.ParseValidityFlag(args)
  if source_ca and not args.IsSpecified('validity'):
    lifetime = source_ca.lifetime

  labels = labels_util.ParseCreateArgs(
      args, messages.CertificateAuthority.LabelsValue)

  new_ca = messages.CertificateAuthority(
      tier=tier,
      type=messages.CertificateAuthority.TypeValueValuesEnum.SUBORDINATE
      if is_subordinate else
      messages.CertificateAuthority.TypeValueValuesEnum.SELF_SIGNED,
      lifetime=lifetime,
      config=messages.CertificateConfig(
          reusableConfig=reusable_config_wrapper, subjectConfig=subject_config),
      keySpec=keyspec,
      certificatePolicy=issuance_policy,
      issuingOptions=issuing_options,
      gcsBucket=None,
      labels=labels)

  return (new_ca, ca_ref, issuer_ref)


_BETA_RESOURCE_DELETION_DISCLAIMER = """CA Service is currently in preview.

Please remember that all resources created during preview will be deleted
when CA service transitions to General Availability (GA). Relying on these
{resource_type} for production traffic is discouraged."""


def PrintBetaResourceDeletionDisclaimer(resource_type_plural):
  log.warning(_BETA_RESOURCE_DELETION_DISCLAIMER.format(
      resource_type=resource_type_plural))


def ValidateIssuingCA(ca_name):
  """Checks that a CA is valid to be issuing Certificate Authority for a subordinate.

  Args:
    ca_name: The resource name of the issuing Certificate Authority.

  Raises:
    InvalidArgumentException if the CA does not exist or is not enabled.
  """
  try:
    client = privateca_base.GetClientInstance()
    messages = privateca_base.GetMessagesModule()
    ca = client.projects_locations_certificateAuthorities.Get(
        messages.PrivatecaProjectsLocationsCertificateAuthoritiesGetRequest(
            name=ca_name))
    if ca.state != messages.CertificateAuthority.StateValueValuesEnum.ENABLED:
      raise exceptions.InvalidArgumentException(
          '--issuer',
          'The issuing Certificate Authority [{}] is not in ENABLED state. Please enable it and try again.'
          .format(ca_name))
  except apitools_exceptions.HttpNotFoundError:
    raise exceptions.InvalidArgumentException(
        '--issuer',
        'The issuing Certificate Authority [{}] was not found. Please verify this information is correct and try again.'
        .format(ca_name))
