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
"""Command for describing instance templates."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.instance_templates import flags


class Describe(base.DescribeCommand):
  """Describe a virtual machine instance template."""

  @staticmethod
  def Args(parser):
    Describe.InstanceTemplateArg = flags.MakeInstanceTemplateArg()
    Describe.InstanceTemplateArg.AddArgument(parser, operation_type='describe')

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    instance_template_ref = Describe.InstanceTemplateArg.ResolveAsResource(
        args,
        holder.resources,
        scope_lister=compute_flags.GetDefaultScopeLister(client))

    request = client.messages.ComputeInstanceTemplatesGetRequest(
        **instance_template_ref.AsDict())

    return client.MakeRequests([(client.apitools_client.instanceTemplates,
                                 'Get', request)])[0]


Describe.detailed_help = {
    'brief':
        'Describe a virtual machine instance template',
    'DESCRIPTION':
        """\
        *{command}* displays all data associated with a Google Compute
        Engine virtual machine instance template.
        """,
    'EXAMPLES':
        """\
        To describe the instance template named 'INSTANCE-TEMPLATE', run:

          $ {command} INSTANCE-TEMPLATE
        """
}
