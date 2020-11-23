# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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
"""Command for deleting instance templates."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.instance_templates import flags


class Delete(base.DeleteCommand):
  """Delete Compute Engine virtual machine instance templates.

  *{command}* deletes one or more Compute Engine virtual machine
  instance templates.

  ## EXAMPLES
  To delete the instance template named 'INSTANCE-TEMPLATE', run:

    $ {command} INSTANCE-TEMPLATE
  """

  @staticmethod
  def Args(parser):
    Delete.InstanceTemplateArg = flags.MakeInstanceTemplateArg(plural=True)
    Delete.InstanceTemplateArg.AddArgument(parser, operation_type='delete')
    parser.display_info.AddCacheUpdater(completers.InstanceTemplatesCompleter)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    instance_template_refs = Delete.InstanceTemplateArg.ResolveAsResource(
        args,
        holder.resources,
        scope_lister=compute_flags.GetDefaultScopeLister(client))

    utils.PromptForDeletion(instance_template_refs)

    requests = []
    for instance_template_ref in instance_template_refs:
      requests.append((client.apitools_client.instanceTemplates, 'Delete',
                       client.messages.ComputeInstanceTemplatesDeleteRequest(
                           **instance_template_ref.AsDict())))

    return client.MakeRequests(requests)
