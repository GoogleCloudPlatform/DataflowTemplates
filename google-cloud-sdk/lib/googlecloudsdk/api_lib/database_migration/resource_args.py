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

"""Shared resource flags for Database Migration Service commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs


def ConnectionProfileAttributeConfig(name='connection_profile'):
  return concepts.ResourceParameterAttributeConfig(
      name=name,
      help_text='The connection profile of the {resource}.',
      completion_request_params={'fieldMask': 'name'},
      completion_id_field='id')


def MigrationJobAttributeConfig(name='migration_job'):
  return concepts.ResourceParameterAttributeConfig(
      name=name,
      help_text='The migration job of the {resource}.',
      completion_request_params={'fieldMask': 'name'},
      completion_id_field='id')


def RegionAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='region',
      help_text='The Cloud region for the {resource}.')


def GetConnectionProfileResourceSpec(resource_name='connection_profile'):
  return concepts.ResourceSpec(
      'datamigration.projects.locations.connectionProfiles',
      resource_name=resource_name,
      connectionProfilesId=ConnectionProfileAttributeConfig(name=resource_name),
      locationsId=RegionAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      disable_auto_completers=False)


def GetMigrationJobResourceSpec(resource_name='migration_job'):
  return concepts.ResourceSpec(
      'datamigration.projects.locations.migrationJobs',
      resource_name=resource_name,
      migrationJobsId=MigrationJobAttributeConfig(name=resource_name),
      locationsId=RegionAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      disable_auto_completers=False)


def AddConnectionProfileResourceArg(parser, verb, positional=True):
  """Add a resource argument for a database migration connection profile.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    positional: bool, if True, means that the resource is a positional rather
      than a flag.
  """
  if positional:
    name = 'connection_profile'
  else:
    name = '--connection-profile'
  concept_parsers.ConceptParser.ForResource(
      name,
      GetConnectionProfileResourceSpec(),
      'The connection profile {}.'.format(verb),
      required=True).AddToParser(parser)


def AddCloudSqlConnectionProfileResouceArgs(parser, verb):
  """Add resource arguments for a database migration CloudSQL connection profile.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  resource_specs = [
      presentation_specs.ResourcePresentationSpec(
          'connection_profile',
          GetConnectionProfileResourceSpec(),
          'The connection profile {}.'.format(verb),
          required=True
      ),
      presentation_specs.ResourcePresentationSpec(
          '--source-id',
          GetConnectionProfileResourceSpec(),
          'Database Migration Service source connection profile ID.',
          required=True,
          flag_name_overrides={'region': ''}
      ),
  ]
  concept_parsers.ConceptParser(
      resource_specs,
      command_level_fallthroughs={'--source-id.region': ['--region']}
  ).AddToParser(parser)


def AddMigrationJobResourceArgs(parser, verb, required=False):
  """Add resource arguments for creating/updating a database migration job.

  Args:
    parser: argparse.ArgumentParser, the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
    required: boolean, whether source/dest resource args are required.
  """
  resource_specs = [
      presentation_specs.ResourcePresentationSpec(
          'migration_job',
          GetMigrationJobResourceSpec(),
          'The migration job {}.'.format(verb),
          required=True
      ),
      presentation_specs.ResourcePresentationSpec(
          '--source',
          GetConnectionProfileResourceSpec(),
          'Resource ID of the source connection profile.',
          required=required,
          flag_name_overrides={'region': ''}
      ),
      presentation_specs.ResourcePresentationSpec(
          '--destination',
          GetConnectionProfileResourceSpec(),
          'Resource ID of the destination connection profile.',
          required=required,
          flag_name_overrides={'region': ''}
      )
  ]
  concept_parsers.ConceptParser(
      resource_specs,
      command_level_fallthroughs={
          '--source.region': ['--region'],
          '--destination.region': ['--region']
      }).AddToParser(parser)
