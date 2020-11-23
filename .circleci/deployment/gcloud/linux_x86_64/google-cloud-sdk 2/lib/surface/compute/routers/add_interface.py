# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""Command for adding an interface to a Compute Engine router."""

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
from googlecloudsdk.command_lib.compute.networks.subnets import flags as subnet_flags
from googlecloudsdk.command_lib.compute.routers import flags as router_flags
from googlecloudsdk.command_lib.compute.vpn_tunnels import (flags as
                                                            vpn_tunnel_flags)


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA)
class AddInterface(base.UpdateCommand):
  """Add an interface to a Compute Engine router.

  *{command}* is used to add an interface to a Compute Engine
  router.
  """

  ROUTER_ARG = None
  VPN_TUNNEL_ARG = None
  INTERCONNECT_ATTACHMENT_ARG = None
  SUBNETWORK_ARG = None

  @classmethod
  def _Args(cls, parser, support_router_appliance=False):
    cls.ROUTER_ARG = router_flags.RouterArgument()
    cls.ROUTER_ARG.AddArgument(parser, operation_type='update')

    link_parser = parser.add_mutually_exclusive_group(required=True)

    vpn_tunnel_group = link_parser.add_argument_group('VPN Tunnel')
    cls.VPN_TUNNEL_ARG = vpn_tunnel_flags.VpnTunnelArgumentForRouter(
        required=False)
    cls.VPN_TUNNEL_ARG.AddArgument(vpn_tunnel_group)

    interconnect_attachment_group = link_parser.add_argument_group(
        'Interconnect Attachment')
    cls.INTERCONNECT_ATTACHMENT_ARG = (
        attachment_flags.InterconnectAttachmentArgumentForRouter())
    cls.INTERCONNECT_ATTACHMENT_ARG.AddArgument(interconnect_attachment_group)
    if support_router_appliance:
      subnetwork_group = link_parser.add_argument_group('Subnetwork')
      cls.SUBNETWORK_ARG = (subnet_flags.SubnetworkArgumentForRouter())
      cls.SUBNETWORK_ARG.AddArgument(subnetwork_group)

    router_flags.AddInterfaceArgs(
        parser, support_router_appliance=support_router_appliance)

  @classmethod
  def Args(cls, parser):
    cls._Args(parser)

  def _GetGetRequest(self, client, router_ref):
    return (client.apitools_client.routers, 'Get',
            client.messages.ComputeRoutersGetRequest(
                router=router_ref.Name(),
                region=router_ref.region,
                project=router_ref.project))

  def _GetSetRequest(self, client, router_ref, replacement):
    return (client.apitools_client.routers, 'Patch',
            client.messages.ComputeRoutersPatchRequest(
                router=router_ref.Name(),
                routerResource=replacement,
                region=router_ref.region,
                project=router_ref.project))

  def Modify(self,
             client,
             resources,
             args,
             existing,
             support_router_appliance=False):
    replacement = encoding.CopyProtoMessage(existing)
    mask = None
    interface_name = args.interface_name

    if support_router_appliance:
      if args.ip_address is not None:
        if args.subnetwork is None and args.mask_length is not None:
          mask = '{0}/{1}'.format(args.ip_address, args.mask_length)
        elif args.subnetwork is None:
          raise parser_errors.ArgumentException(
              '--mask-length must be set if --ip-address is set')
        elif args.subnetwork is not None and args.mask_length is not None:
          raise parser_errors.ArgumentException(
              '--mask-length cannot be set if --subnetwork is set')
    else:
      if args.ip_address is not None:
        if args.mask_length is not None:
          mask = '{0}/{1}'.format(args.ip_address, args.mask_length)
        else:
          raise parser_errors.ArgumentException(
              '--mask-length must be set if --ip-address is set')

    if args.mask_length is not None:
      if args.mask_length < 0 or args.mask_length > 31:
        raise parser_errors.ArgumentException(
            '--mask-length must be a non-negative integer less than 32')

    if not args.vpn_tunnel_region:
      args.vpn_tunnel_region = replacement.region
    vpn_ref = None
    if args.vpn_tunnel is not None:
      vpn_ref = self.VPN_TUNNEL_ARG.ResolveAsResource(
          args,
          resources,
          scope_lister=compute_flags.GetDefaultScopeLister(client))

    if not args.interconnect_attachment_region:
      args.interconnect_attachment_region = replacement.region
    attachment_ref = None
    if args.interconnect_attachment is not None:
      attachment_ref = self.INTERCONNECT_ATTACHMENT_ARG.ResolveAsResource(
          args, resources)

    subnetwork_ref = None
    private_ip_address = None
    redundant_interface = None
    if support_router_appliance and args.subnetwork is not None:
      subnetwork_ref = self.SUBNETWORK_ARG.ResolveAsResource(args, resources)
      private_ip_address = args.ip_address
      redundant_interface = args.redundant_interface

    if support_router_appliance:
      interface = client.messages.RouterInterface(
          name=interface_name,
          linkedVpnTunnel=(vpn_ref.SelfLink() if vpn_ref else None),
          linkedInterconnectAttachment=(attachment_ref.SelfLink()
                                        if attachment_ref else None),
          subnetwork=(subnetwork_ref.SelfLink() if subnetwork_ref else None),
          ipRange=mask,
          privateIpAddress=private_ip_address,
          redundantInterface=redundant_interface)
    else:
      interface = client.messages.RouterInterface(
          name=interface_name,
          linkedVpnTunnel=(vpn_ref.SelfLink() if vpn_ref else None),
          linkedInterconnectAttachment=(attachment_ref.SelfLink()
                                        if attachment_ref else None),
          ipRange=mask)

    replacement.interfaces.append(interface)

    return replacement

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    router_ref = self.ROUTER_ARG.ResolveAsResource(args, holder.resources)
    get_request = self._GetGetRequest(client, router_ref)

    objects = client.MakeRequests([get_request])

    new_object = self.Modify(client, holder.resources, args, objects[0])

    return client.MakeRequests(
        [self._GetSetRequest(client, router_ref, new_object)])


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class AddInterfaceAlpha(AddInterface):
  """Add an interface to a Compute Engine router.

  *{command}* is used to add an interface to a Compute Engine
  router.
  """

  ROUTER_ARG = None
  VPN_TUNNEL_ARG = None
  INTERCONNECT_ATTACHMENT_ARG = None
  SUBNETWORK_ARG = None

  @classmethod
  def Args(cls, parser):
    cls._Args(parser, support_router_appliance=True)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    router_ref = self.ROUTER_ARG.ResolveAsResource(args, holder.resources)
    get_request = self._GetGetRequest(client, router_ref)

    objects = client.MakeRequests([get_request])

    new_object = self.Modify(
        client,
        holder.resources,
        args,
        objects[0],
        support_router_appliance=True)

    return client.MakeRequests(
        [self._GetSetRequest(client, router_ref, new_object)])
