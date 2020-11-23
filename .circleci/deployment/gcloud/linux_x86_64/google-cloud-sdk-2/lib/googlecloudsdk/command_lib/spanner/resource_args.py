# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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
"""Shared resource flags for Cloud Spanner commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import properties

_PROJECT = properties.VALUES.core.project
_INSTANCE = properties.VALUES.spanner.instance


def InstanceAttributeConfig():
  """Get instance resource attribute with default value."""
  return concepts.ResourceParameterAttributeConfig(
      name='instance',
      help_text='The Cloud Spanner instance for the {resource}.',
      fallthroughs=[deps.PropertyFallthrough(_INSTANCE)])


def DatabaseAttributeConfig():
  """Get database resource attribute."""
  return concepts.ResourceParameterAttributeConfig(
      name='database',
      help_text='The Cloud Spanner database for the {resource}.')


def BackupAttributeConfig():
  """Get backup resource attribute."""
  return concepts.ResourceParameterAttributeConfig(
      name='backup',
      help_text='The Cloud Spanner backup for the {resource}.')


def SessionAttributeConfig():
  """Get session resource attribute."""
  return concepts.ResourceParameterAttributeConfig(
      name='session', help_text='The Cloud Spanner session for the {resource}.')

def KmsKeyAttributeConfig():
  # For anchor attribute, help text is generated automatically.
  return concepts.ResourceParameterAttributeConfig(name='kms-key')


def KmsKeyringAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='kms-keyring', help_text='KMS Keyring id of the {resource}.')


def KmsLocationAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='kms-location', help_text='Cloud Location for the {resource}.')


def KmsProjectAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='kms-project', help_text='Cloud Project id for the {resource}.')

def GetInstanceResourceSpec():
  return concepts.ResourceSpec(
      'spanner.projects.instances',
      resource_name='instance',
      instancesId=InstanceAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def GetDatabaseResourceSpec():
  return concepts.ResourceSpec(
      'spanner.projects.instances.databases',
      resource_name='database',
      databasesId=DatabaseAttributeConfig(),
      instancesId=InstanceAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)

def GetKmsKeyResourceSpec():
  return concepts.ResourceSpec(
      'cloudkms.projects.locations.keyRings.cryptoKeys',
      resource_name='key',
      cryptoKeysId=KmsKeyAttributeConfig(),
      keyRingsId=KmsKeyringAttributeConfig(),
      locationsId=KmsLocationAttributeConfig(),
      projectsId=KmsProjectAttributeConfig())

def GetBackupResourceSpec():
  return concepts.ResourceSpec(
      'spanner.projects.instances.backups',
      resource_name='backup',
      backupsId=BackupAttributeConfig(),
      instancesId=InstanceAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def GetSessionResourceSpec():
  return concepts.ResourceSpec(
      'spanner.projects.instances.databases.sessions',
      resource_name='session',
      sessionsId=SessionAttributeConfig(),
      databasesId=DatabaseAttributeConfig(),
      instancesId=InstanceAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def AddInstanceResourceArg(parser, verb, positional=True):
  """Add a resource argument for a Cloud Spanner instance.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the argparse parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    positional: bool, if True, means that the instance ID is a positional rather
      than a flag.
  """
  name = 'instance' if positional else '--instance'
  concept_parsers.ConceptParser.ForResource(
      name,
      GetInstanceResourceSpec(),
      'The Cloud Spanner instance {}.'.format(verb),
      required=True).AddToParser(parser)


def AddDatabaseResourceArg(parser, verb, positional=True):
  """Add a resource argument for a Cloud Spanner database.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the argparse parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    positional: bool, if True, means that the database ID is a positional rather
      than a flag.
  """
  name = 'database' if positional else '--database'
  concept_parsers.ConceptParser.ForResource(
      name,
      GetDatabaseResourceSpec(),
      'The Cloud Spanner database {}.'.format(verb),
      required=True).AddToParser(parser)

def AddKmsKeyResourceArg(parser, verb, positional=False):
  """Add a resource argument for a KMS Key used to create a CMEK database.

  Args:
    parser: argparser, the parser for the command.
    verb: str, the verb used to describe the resource, such as 'to create'.
    positional: bool, optional. True if the resource arg is postional rather
      than a flag.
  """
  # TODO(b/154755597): Adding this resource arg to its own group is currently
  # the only way to hide a resource arg. When ready to publish this arg to
  # public, remove this group and add this resource arg directly (and generate
  # the help text).
  group_parser = parser.add_argument_group(hidden=True)
  name = 'kms-key' if positional else '--kms-key'
  concept_parsers.ConceptParser.ForResource(
      name,
      GetKmsKeyResourceSpec(),
      'Cloud KMS Key to be used {} the Cloud Spanner database.'.format(
          verb),
      required=False).AddToParser(group_parser)


def AddSessionResourceArg(parser, verb, positional=True):
  """Add a resource argument for a Cloud Spanner session.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    positional: bool, if True, means that the session ID is a positional rather
      than a flag.
  """
  name = 'session' if positional else '--session'
  concept_parsers.ConceptParser.ForResource(
      name,
      GetSessionResourceSpec(),
      'The Cloud Spanner session {}.'.format(verb),
      required=True).AddToParser(parser)


def AddBackupResourceArg(parser, verb, positional=True):
  """Add a resource argument for a Cloud Spanner backup.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the argparse parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    positional: bool, if True, means that the backup ID is a positional rather
      than a flag.
  """
  name = 'backup' if positional else '--backup'

  concept_parsers.ConceptParser.ForResource(
      name,
      GetBackupResourceSpec(),
      'The Cloud Spanner backup {}.'.format(verb),
      required=True).AddToParser(parser)


def AddRestoreResourceArgs(parser):
  """Add backup resource args (source, destination) for restore command."""
  arg_specs = [
      presentation_specs.ResourcePresentationSpec(
          '--source',
          GetBackupResourceSpec(),
          'TEXT',
          required=True,
          flag_name_overrides={
              'instance': '--source-instance',
              'backup': '--source-backup'
          }),
      presentation_specs.ResourcePresentationSpec(
          '--destination',
          GetDatabaseResourceSpec(),
          'TEXT',
          required=True,
          flag_name_overrides={
              'instance': '--destination-instance',
              'database': '--destination-database',
          }),
  ]

  concept_parsers.ConceptParser(arg_specs).AddToParser(parser)


def GetAndValidateKmsKeyName(args):
  """Parse the KMS key resource arg, make sure the key format is correct."""
  kms_ref = args.CONCEPTS.kms_key.Parse()
  if kms_ref:
    return kms_ref.RelativeName()
  else:
    # If parsing failed but args were specified, raise error
    for keyword in ['kms-key', 'kms-keyring', 'kms-location', 'kms-project']:
      if getattr(args, keyword.replace('-', '_'), None):
        raise exceptions.InvalidArgumentException(
            '--kms-project --kms-location --kms-keyring --kms-key',
            'Specify fully qualified KMS key ID with --kms-key, or use ' +
            'combination of --kms-project, --kms-location, --kms-keyring and ' +
            '--kms-key to specify the key ID in pieces.')
    return None  # User didn't specify KMS key
