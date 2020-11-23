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
"""`gcloud access-context-manager cloud-bindings update` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.accesscontextmanager import cloud_bindings as cloud_bindings_api
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.accesscontextmanager import cloud_bindings


@base.ReleaseTracks(base.ReleaseTrack.GA)
class UpdateBindingGA(base.Command):
  """Update an existing cloud access binding."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
          To update an existing cloud access binding, run:

            $ {command} --binding=my-binding-id --level=new-access-level
          """,
  }

  _API_VERSION = 'v1'

  @staticmethod
  def Args(parser):
    cloud_bindings.AddResourceArg(parser, 'to update')

  def Run(self, args):
    client = cloud_bindings_api.Client(version=self._API_VERSION)

    binding_ref = args.CONCEPTS.binding.Parse()
    level_ref = args.CONCEPTS.level.Parse()

    return client.Patch(binding_ref, level_ref)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class UpdateBindingALPHA(UpdateBindingGA):
  """Update an existing cloud access binding."""
  _API_VERSION = 'v1alpha'
