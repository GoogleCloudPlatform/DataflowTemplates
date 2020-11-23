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

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import properties


def Args(parser):
  """Add args for the parent resource of a request to the parser."""
  parent_group = parser.add_mutually_exclusive_group(required=False)
  parent_group.add_argument(
      '--project',
      help='project number or id. only one of --project, --folder, or --organization can be provided. If none are provided then it uses config property [core/project].'
  )
  parent_group.add_argument(
      '--folder',
      help='folder number. only one of --project, --folder, or --organization can be provided. If none are provided then it uses config property [core/project].'
  )
  parent_group.add_argument(
      '--organization',
      help='organization number. either --project, --folder, or --organization must be provided. If none are provided then it uses config property [core/project].'
  )


def GetParent(args):
  """Returns the parent resource from args or the active gcloud project."""
  if 0 == sum(bool(x) for x in (args.project, args.folder, args.organization)):
    # if neither project, folder, org was specified default to the
    # current project if available.
    args.project = properties.VALUES.core.project.GetOrFail()

  parent = None
  if args.project:
    _ValidateProject(args.project)
    parent = 'projects/%s' % args.project
  elif args.folder:
    _ValidateFolder(args.folder)
    parent = 'folders/%s' % args.folder
  else:
    _ValidateOrganization(args.organization)
    parent = 'organizations/%s' % args.organization

  return parent


def _ValidateProject(flag_value):
  if not re.match('^[a-z0-9-]+$', flag_value):
    raise exceptions.InvalidArgumentException('project', flag_value)


def _ValidateFolder(flag_value):
  if not re.match('^[0-9]+$', flag_value):
    raise exceptions.InvalidArgumentException('folder', flag_value)


def _ValidateOrganization(flag_value):
  if not re.match('^[0-9]+$', flag_value):
    raise exceptions.InvalidArgumentException('organization', flag_value)
