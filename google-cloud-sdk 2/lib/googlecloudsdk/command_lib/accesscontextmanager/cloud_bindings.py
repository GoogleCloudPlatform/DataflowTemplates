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
"""Command line processing utilities for cloud access bindings."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.command_lib.accesscontextmanager import levels
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources


def GetBindingAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='binding', help_text='The ID of the cloud access binding.')


def GetOrganizationAttributeConfig():
  property_ = properties.VALUES.access_context_manager.organization
  return concepts.ResourceParameterAttributeConfig(
      name='organization',
      help_text='The ID of the organization.',
      fallthroughs=[deps.PropertyFallthrough(property_)])


def GetResourceSpec():
  return concepts.ResourceSpec(
      'accesscontextmanager.organizations.gcpUserAccessBindings',
      resource_name='cloud-access-binding',
      gcpUserAccessBindingsId=GetBindingAttributeConfig(),
      organizationsId=GetOrganizationAttributeConfig())


def AddResourceArg(parser, verb):
  """Add a resource argument for a gcpUserAccessBinding.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  concept_parsers.ConceptParser([
      presentation_specs.ResourcePresentationSpec(
          '--binding',
          GetResourceSpec(),
          'The access binding {}.'.format(verb),
          required=True),
      presentation_specs.ResourcePresentationSpec(
          '--level',
          levels.GetResourceSpec(),
          'The access level {}.'.format(verb),
          required=True)
  ]).AddToParser(parser)


def GetOrgArg():
  return base.Argument(
      '--organization',
      help='The parent organization for the cloud access binding.')


def GetGroupKeyArg():
  return base.Argument(
      '--group-key',
      required=True,
      help='Google Group id whose members are subject to the restrictions of this binding.'
  )


def GetDefaultOrganization():
  """Gets the reference of the default organization from property access_context_manager/organization."""
  organization = properties.VALUES.access_context_manager.organization.Get()
  if not organization:
    return None

  organization_ref = resources.REGISTRY.Parse(
      organization, collection='accesscontextmanager.organizations')
  return organization_ref
