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
"""`gcloud access-context-manager cloud-bindings create` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.accesscontextmanager import cloud_bindings as cloud_bindings_api
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.accesscontextmanager import cloud_bindings
from googlecloudsdk.command_lib.accesscontextmanager import levels
from googlecloudsdk.command_lib.util.hooks import types


@base.ReleaseTracks(base.ReleaseTrack.GA)
class CreateBindingGA(base.Command):
  """Create a new cloud access binding."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
          To create a new cloud access binding, run:

            $ {command} --group-key=my-group-key --level=my-access-level
          """,
  }

  _API_VERSION = 'v1'

  @staticmethod
  def Args(parser):
    levels.AddResourceFlagArg(parser, 'to create')
    cloud_bindings.GetOrgArg().AddToParser(parser)
    cloud_bindings.GetGroupKeyArg().AddToParser(parser)

  def Run(self, args):
    client = cloud_bindings_api.Client(version=self._API_VERSION)
    org_ref = cloud_bindings.GetDefaultOrganization()
    if args.IsSpecified('organization'):
      org_ref = types.Resource('accesscontextmanager.organizations')(
          args.organization)

    level_ref = args.CONCEPTS.level.Parse()

    return client.Create(org_ref, args.group_key, level_ref)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateBindingAlpha(CreateBindingGA):
  """Create a new cloud access binding."""
  _API_VERSION = 'v1alpha'
