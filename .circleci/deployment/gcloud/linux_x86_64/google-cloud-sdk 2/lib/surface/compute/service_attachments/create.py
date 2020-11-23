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
"""Command for creating service attachments."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute.forwarding_rules import flags as forwarding_rule_flags
from googlecloudsdk.command_lib.compute.networks.subnets import flags as subnetwork_flags
from googlecloudsdk.command_lib.compute.service_attachments import flags
from googlecloudsdk.command_lib.compute.service_attachments import service_attachments_utils


def _DetailedHelp():
  return {
      'brief':
          'Create a service attachment.',
      'DESCRIPTION':
          """\
      *{command}* is used to create service attachments. A service attachment is
      a component used by a service producer to setup a Private Service Connect
      (PSC) which a service consumer could then forward traffic privately to.
      """,
      'EXAMPLES':
          """\
      If there is an already-created internal load balancer (ILB) with the name
      MY_ILB in region us-central1 and there is an already-created PSC subnets
      MY_SUBNET1 and MY_SUBNET2, create a service attachment pointing to the ILB
      by running:

        $ {command} SERVICE_ATTACHMENT_NAME --region=us-central1 --producer-forwarding-rule=MY_ILB --connection-preference=ACCEPT_AUTOMATIC --nat-subnets=MY_SUBNET1,MY_SUBNET2

      To create a service attachment with a textual description, run:

        $ {command} SERVICE_ATTACHMENT_NAME --region=us-central1 --producer-forwarding-rule=MY_ILB --connection-preference=ACCEPT_AUTOMATIC --nat-subnets=MY_SUBNET1,MY_SUBNET2 --description='default service attachment'

      """,
  }


class Create(base.CreateCommand):
  """Create a service attachment."""

  SERVICE_ATTACHMENT_ARG = None
  PRODUCER_FORWARDING_RULE_ARG = None
  NAT_SUBNETWORK_ARG = None
  detailed_help = _DetailedHelp()

  @classmethod
  def Args(cls, parser):
    cls.SERVICE_ATTACHMENT_ARG = flags.ServiceAttachmentArgument()
    cls.SERVICE_ATTACHMENT_ARG.AddArgument(parser, operation_type='create')
    cls.PRODUCER_FORWARDING_RULE_ARG = \
        forwarding_rule_flags.ForwardingRuleArgumentForServiceAttachment()
    cls.PRODUCER_FORWARDING_RULE_ARG.AddArgument(parser)
    cls.NAT_SUBNETWORK_ARG = \
        subnetwork_flags.SubnetworkArgumentForServiceAttachment()
    cls.NAT_SUBNETWORK_ARG.AddArgument(parser)

    parser.display_info.AddFormat(flags.DEFAULT_LIST_FORMAT)
    parser.display_info.AddCacheUpdater(flags.ServiceAttachmentsCompleter)

    flags.AddDescription(parser)
    flags.AddConnectionPreference(parser)

  def Run(self, args):
    """Issue a service attachment INSERT request."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client
    service_attachment_ref = self.SERVICE_ATTACHMENT_ARG.ResolveAsResource(
        args, holder.resources, default_scope=compute_scope.ScopeEnum.REGION)
    producer_forwarding_rule_ref = \
        self.PRODUCER_FORWARDING_RULE_ARG.ResolveAsResource(args,
                                                            holder.resources)
    nat_subnetwork_refs = self.NAT_SUBNETWORK_ARG.ResolveAsResource(
        args,
        holder.resources,
        default_scope=compute_scope.ScopeEnum.REGION,
        scope_lister=compute_flags.GetDefaultScopeLister(client))
    nat_subnetworks = [
        nat_subnetwork_ref.SelfLink()
        for nat_subnetwork_ref in nat_subnetwork_refs
    ]
    connection_preference = service_attachments_utils.GetConnectionPreference(
        args, client.messages)

    service_attachment = client.messages.ServiceAttachment(
        description=args.description,
        name=service_attachment_ref.Name(),
        natSubnets=nat_subnetworks,
        connectionPreference=connection_preference,
        producerForwardingRule=producer_forwarding_rule_ref.SelfLink())

    request = client.messages.ComputeServiceAttachmentsInsertRequest(
        project=service_attachment_ref.project,
        region=service_attachment_ref.region,
        serviceAttachment=service_attachment)
    collection = client.apitools_client.serviceAttachments
    return client.MakeRequests([(collection, 'Insert', request)])
