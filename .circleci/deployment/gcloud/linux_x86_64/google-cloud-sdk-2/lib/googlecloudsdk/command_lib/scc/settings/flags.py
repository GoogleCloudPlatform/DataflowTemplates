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
"""A library for Security Command Center(SCC) settings commands arguments."""

from googlecloudsdk.calliope import base


def AddOrganizationFlag(parser, help_text):
  parser.add_argument(
      '--organization', metavar='ORGANIZATION_ID', help=help_text)


def AddFolderFlag(parser, help_text):
  parser.add_argument('--folder', metavar='FOLDER_ID', help=help_text)


def AddProjectFlag(parser, help_text):
  parser.add_argument('--project', metavar='PROJECT_ID', help=help_text)


def ExtractRequiredFlags(parser):
  parent_group = parser.add_mutually_exclusive_group()
  AddOrganizationFlag(parent_group, 'Organization ID')
  AddFolderFlag(parent_group, 'Folder ID')
  AddProjectFlag(parent_group, 'Project ID')


def AddServiceArgument(parser):
  base.ChoiceArgument(
      '--service',
      required=True,
      metavar='SERVICE_NAME',
      choices=[
          'container-threat-detection',
          'event-threat-detection',
          'security-health-analytics',
          'web-security-scanner',
      ],
      default='none',
      help_str='Service name in Security Command Center').AddToParser(parser)


def AddModuleArgument(parser):
  parser.add_argument(
      '--module',
      required=True,
      metavar='MODULE_NAME',
      help='Module name in Security Command Center')
