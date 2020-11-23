# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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
"""Command for updating an interface on a Compute Engine router."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import parser_errors
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.interconnects.attachments import (
    flags as attachment_flags)
from googlecloudsdk.command_lib.compute.routers import flags as router_flags
from googlecloudsdk.command_lib.compute.routers import router_utils
from googlecloudsdk.command_lib.compute.vpn_tunnels import (flags as
                                                            vpn_tunnel_flags)
from googlecloudsdk.core import log


class UpdateInterface(base.UpdateCommand):
  """Update an interface on a Compute Engine router.

  *{command}* is used to update an interface on a Compute Engine
  router.
  """

  ROUTER_ARG = None
  VPN_TUNNEL_ARG = None
  INTERCONNECT_ATTACHMENT_ARG = None

  @classmethod
  def Args(cls, parser):
    cls.ROUTER_ARG = router_flags.RouterArgument()
    cls.ROUTER_ARG.AddArgument(parser, operation_type='update')

    link_parser = parser.add_mutually_exclusive_group(required=False)

    cls.VPN_TUNNEL_ARG = vpn_tunnel_flags.VpnTunnelArgumentForRouter(
        required=False, operation_type='updated')
    cls.VPN_TUNNEL_ARG.AddArgument(link_parser)

    cls.INTERCONNECT_ATTACHMENT_ARG = (
        attachment_flags.InterconnectAttachmentArgumentForRouter(
            required=False, operation_type='updated'))
    cls.INTERCONNECT_ATTACHMENT_ARG.AddArgument(link_parser)

    router_flags.AddInterfaceArgs(parser, for_update=True)

  def GetGetRequest(self, client, router_ref):
    return (client.apitools_client.routers, 'Get',
            client.messages.ComputeRoutersGetRequest(
                router=router_ref.Name(),
                region=router_ref.region,
                project=router_ref.project))

  def GetSetRequest(self, client, router_ref, replacement):
    return (client.apitools_client.routers, 'Patch',
            client.messages.ComputeRoutersPatchRequest(
                router=router_ref.Name(),
                routerResource=replacement,
                region=router_ref.region,
                project=router_ref.project))

  def Modify(self, client, resources, args, existing):
    replacement = encoding.CopyProtoMessage(existing)

    iface = None
    for i in replacement.interfaces:
      if i.name == args.interface_name:
        iface = i
        break

    if iface is None:
      raise router_utils.InterfaceNotFoundError(args.interface_name)

    # Flags --ip-address and --mask-length must be specified together.
    # TODO(b/65850105): Use an argument group for these flags.
    if (args.ip_address is not None) and (args.mask_length is not None):
      iface.ipRange = '{0}/{1}'.format(args.ip_address, args.mask_length)
    elif (args.ip_address is not None) or (args.mask_length is not None):
      raise router_utils.RequireIpAddressAndMaskLengthError()

    if not args.vpn_tunnel_region:
      args.vpn_tunnel_region = replacement.region

    if args.vpn_tunnel is not None:
      vpn_ref = self.VPN_TUNNEL_ARG.ResolveAsResource(
          args,
          resources,
          scope_lister=compute_flags.GetDefaultScopeLister(client))
      iface.linkedVpnTunnel = vpn_ref.SelfLink()

    if not args.interconnect_attachment_region:
      args.interconnect_attachment_region = replacement.region

    if args.interconnect_attachment is not None:
      attachment_ref = self.INTERCONNECT_ATTACHMENT_ARG.ResolveAsResource(
          args, resources)
      iface.linkedInterconnectAttachment = attachment_ref.SelfLink()

    if (iface.linkedVpnTunnel is not None and
        iface.linkedInterconnectAttachment is not None):
      raise parser_errors.ArgumentException(
          'cannot have both vpn-tunnel and interconnect-attachment for the '
          'interface.')

    return replacement

  def Run(self, args):
    """Issues requests necessary to update interfaces of the Router."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    router_ref = self.ROUTER_ARG.ResolveAsResource(args, holder.resources)
    get_request = self.GetGetRequest(client, router_ref)

    objects = client.MakeRequests([get_request])

    new_object = self.Modify(client, holder.resources, args, objects[0])

    # If existing object is equal to the proposed object or if
    # Modify() returns None, then there is no work to be done, so we
    # print the resource and return.
    if objects[0] == new_object:
      log.status.Print('No change requested; skipping update for [{0}].'.format(
          objects[0].name))
      return objects

    return client.MakeRequests(
        [self.GetSetRequest(client, router_ref, new_object)])
