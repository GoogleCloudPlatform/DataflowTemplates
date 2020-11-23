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
"""Helpers for parsing resource arguments."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base
from googlecloudsdk.api_lib.privateca import constants as api_constants
from googlecloudsdk.api_lib.privateca import locations
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.calliope.concepts import handlers
from googlecloudsdk.calliope.concepts import util
from googlecloudsdk.command_lib.kms import resource_args as kms_args
from googlecloudsdk.command_lib.privateca import exceptions as privateca_exceptions
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.core import properties
import six

# Flag fallthroughs that rely on properties.
LOCATION_PROPERTY_FALLTHROUGH = deps.PropertyFallthrough(
    properties.VALUES.privateca.location)
PROJECT_PROPERTY_FALLTHROUGH = deps.PropertyFallthrough(
    properties.VALUES.core.project)


def _GetFallthroughsSummary(fallthroughs):
  if not fallthroughs:
    return ''

  if len(fallthroughs) == 1:
    return ' Alternatively, you can {}.'.format(fallthroughs[0].hint)

  return ' Alternatively, you can {}, or {}.'.format(
      ', '.join([f.hint for f in fallthroughs[:-1]]), fallthroughs[-1].hint)


def ReusableConfigAttributeConfig():
  # ReusableConfig is always an anchor attribute so help_text is unused.
  return concepts.ResourceParameterAttributeConfig(name='reusable_config')


def CertificateAttributeConfig(fallthroughs=None):
  # Certificate is always an anchor attribute so help_text is unused.
  return concepts.ResourceParameterAttributeConfig(
      name='certificate', fallthroughs=fallthroughs or [])


def CertificateAuthorityAttributeConfig(arg_name='certificate_authority',
                                        fallthroughs=None):
  fallthroughs = fallthroughs or []
  return concepts.ResourceParameterAttributeConfig(
      name=arg_name,
      help_text='The issuing certificate authority of the {resource}.' +
      _GetFallthroughsSummary(fallthroughs),
      fallthroughs=fallthroughs)


def LocationAttributeConfig(arg_name='location', fallthroughs=None):
  fallthroughs = fallthroughs or [LOCATION_PROPERTY_FALLTHROUGH]
  return concepts.ResourceParameterAttributeConfig(
      name=arg_name,
      help_text='The location of the {resource}.' +
      _GetFallthroughsSummary(fallthroughs),
      fallthroughs=fallthroughs)


def ProjectAttributeConfig(arg_name='project', fallthroughs=None):
  """DO NOT USE THIS for most flags.

  This config is only useful when you want to provide an explicit project
  fallthrough. For most cases, prefer concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG.

  Args:
    arg_name: Name of the flag used to specify this attribute. Defaults to
              'project'.
    fallthroughs: List of deps.Fallthrough objects to provide project values.

  Returns:
    A concepts.ResourceParameterAttributeConfig for a project.
  """
  return concepts.ResourceParameterAttributeConfig(
      name=arg_name,
      help_text='The project containing the {resource}.' +
      _GetFallthroughsSummary(fallthroughs),
      fallthroughs=fallthroughs or [])


def CreateKmsKeyVersionResourceSpec():
  """Creates a resource spec for a KMS CryptoKeyVersion.

  Defaults to the location and project of the CA, specified through flags or
  properties.

  Returns:
    A concepts.ResourceSpec for a CryptoKeyVersion.
  """
  return concepts.ResourceSpec(
      'cloudkms.projects.locations.keyRings.cryptoKeys.cryptoKeyVersions',
      resource_name='key version',
      cryptoKeyVersionsId=kms_args.KeyVersionAttributeConfig(kms_prefix=True),
      cryptoKeysId=kms_args.KeyAttributeConfig(kms_prefix=True),
      keyRingsId=kms_args.KeyringAttributeConfig(kms_prefix=True),
      locationsId=LocationAttributeConfig(
          'kms-location',
          [deps.ArgFallthrough('location'), LOCATION_PROPERTY_FALLTHROUGH]),
      projectsId=ProjectAttributeConfig(
          'kms-project',
          [deps.ArgFallthrough('project'), PROJECT_PROPERTY_FALLTHROUGH]))


def CreateReusableConfigResourceSpec(location_fallthroughs=None):
  """Create a resource spec for a ReusableConfig.

  Defaults to the predefined project for reusable configs.

  Args:
    location_fallthroughs: List of fallthroughs to use for the location of the
      reusable config, if not explicitly provided.

  Returns:
    A concepts.ResourceSpec for a reusable config.
  """
  # For now, reusable configs exist in a single project.
  project_fallthrough = deps.Fallthrough(
      function=lambda: api_constants.PREDEFINED_REUSABLE_CONFIG_PROJECT,
      hint='project will default to {}'.format(
          api_constants.PREDEFINED_REUSABLE_CONFIG_PROJECT),
      active=False,
      plural=False)
  return concepts.ResourceSpec(
      'privateca.projects.locations.reusableConfigs',
      resource_name='reusable config',
      reusableConfigsId=ReusableConfigAttributeConfig(),
      locationsId=LocationAttributeConfig(fallthroughs=location_fallthroughs),
      projectsId=ProjectAttributeConfig(fallthroughs=[project_fallthrough]))


def CreateCertificateAuthorityResourceSpec(
    display_name,
    certificate_authority_attribute='certificate_authority',
    location_attribute='location',
    ca_id_fallthroughs=None):
  return concepts.ResourceSpec(
      'privateca.projects.locations.certificateAuthorities',
      # This will be formatted and used as {resource} in the help text.
      resource_name=display_name,
      certificateAuthoritiesId=CertificateAuthorityAttributeConfig(
          certificate_authority_attribute, fallthroughs=ca_id_fallthroughs),
      locationsId=LocationAttributeConfig(location_attribute),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def CreateCertificateResourceSpec(display_name, id_fallthroughs=None):
  return concepts.ResourceSpec(
      'privateca.projects.locations.certificateAuthorities.certificates',
      # This will be formatted and used as {resource} in the help text.
      resource_name=display_name,
      certificatesId=CertificateAttributeConfig(
          fallthroughs=id_fallthroughs or []),
      certificateAuthoritiesId=CertificateAuthorityAttributeConfig('issuer'),
      locationsId=LocationAttributeConfig('issuer-location'),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def AddCertificateAuthorityPositionalResourceArg(parser, verb):
  """Add a positional resource argument for a Certificate Authority.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  arg_name = 'CERTIFICATE_AUTHORITY'
  concept_parsers.ConceptParser.ForResource(
      arg_name,
      CreateCertificateAuthorityResourceSpec(arg_name),
      'The certificate authority {}.'.format(verb),
      required=True).AddToParser(parser)


def AddCertificatePositionalResourceArg(parser, verb):
  """Add a positional resource argument for a Certificate.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  arg_name = 'CERTIFICATE'
  concept_parsers.ConceptParser.ForResource(
      arg_name,
      CreateCertificateResourceSpec(arg_name),
      'The certificate {}.'.format(verb),
      required=True).AddToParser(parser)

# Resource validation.


def ValidateResourceLocation(resource_ref, arg_name):
  """Raises an exception if the given resource is in an unsupported location."""
  supported_locations = locations.GetSupportedLocations()
  if resource_ref.locationsId not in supported_locations:
    raise exceptions.InvalidArgumentException(
        arg_name,
        'Resource is in an unsupported location. Supported locations are: {}.'
        .format(', '.join(sorted(supported_locations))))


def CheckExpectedCAType(expected_type, ca):
  """Raises an exception if the Certificate Authority type is not expected_type.

  Args:
    expected_type: The expected type.
    ca: The ca object to check.
  """
  ca_type_enum = base.GetMessagesModule(
  ).CertificateAuthority.TypeValueValuesEnum
  if expected_type == ca_type_enum.SUBORDINATE and ca.type != expected_type:
    raise privateca_exceptions.InvalidCertificateAuthorityTypeError(
        'Cannot perform subordinates command on Root CA. Please use the `privateca roots` command group instead.'
    )
  elif expected_type == ca_type_enum.SELF_SIGNED and ca.type != expected_type:
    raise privateca_exceptions.InvalidCertificateAuthorityTypeError(
        'Cannot perform roots command on Subordinate CA. Please use the `privateca subordinates` command group instead.'
    )


def ValidateResourceIsCompleteIfSpecified(args, resource_arg_name):
  """Raises a ParseError if the given resource_arg_name is partially specified."""
  if not hasattr(args.CONCEPTS, resource_arg_name):
    return

  concept_info = args.CONCEPTS.ArgNameToConceptInfo(resource_arg_name)
  associated_args = [
      util.NamespaceFormat(arg)
      for arg in concept_info.attribute_to_args_map.values()
  ]

  # If none of the relevant args are specified, we're good.
  if not [arg for arg in associated_args if args.IsSpecified(arg)]:
    return

  try:
    # Re-parse this concept, but treat it as required even if it originally
    # wasn't. This will trigger a meaningful user error if it's underspecified.
    concept_info.ClearCache()
    concept_info.allow_empty = False
    concept_info.Parse(args)
  except concepts.InitializationError as e:
    raise handlers.ParseError(resource_arg_name, six.text_type(e))
