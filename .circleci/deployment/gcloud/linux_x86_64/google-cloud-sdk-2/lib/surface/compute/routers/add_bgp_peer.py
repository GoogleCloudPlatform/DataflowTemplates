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
"""Command for adding a BGP peer to a Compute Engine router."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute.operations import poller
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute.instances import flags as instance_flags
from googlecloudsdk.command_lib.compute.routers import flags
from googlecloudsdk.command_lib.compute.routers import router_utils
from googlecloudsdk.core import log
from googlecloudsdk.core import resources
import six


@base.ReleaseTracks(base.ReleaseTrack.GA)
class AddBgpPeer(base.UpdateCommand):
  """Add a BGP peer to a Compute Engine router."""

  ROUTER_ARG = None

  @classmethod
  def _Args(cls, parser, support_bfd=False, support_enable=False):
    cls.ROUTER_ARG = flags.RouterArgument()
    cls.ROUTER_ARG.AddArgument(parser)
    base.ASYNC_FLAG.AddToParser(parser)
    flags.AddBgpPeerArgs(
        parser, for_add_bgp_peer=True, support_bfd=support_bfd,
        support_enable=support_enable)
    flags.AddReplaceCustomAdvertisementArgs(parser, 'peer')

  @classmethod
  def Args(cls, parser):
    cls._Args(parser)

  def _Run(self,
           args,
           support_bfd=False,
           support_enable=False,
           support_bfd_mode=False,
           instance_ref=None):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    messages = holder.client.messages
    service = holder.client.apitools_client.routers

    router_ref = self.ROUTER_ARG.ResolveAsResource(args, holder.resources)

    request_type = messages.ComputeRoutersGetRequest
    replacement = service.Get(request_type(**router_ref.AsDict()))

    peer = _CreateBgpPeerMessage(
        messages,
        args,
        support_bfd=support_bfd,
        support_enable=support_enable,
        support_bfd_mode=support_bfd_mode,
        instance_ref=instance_ref)

    if router_utils.HasReplaceAdvertisementFlags(args):
      mode, groups, ranges = router_utils.ParseAdvertisements(
          messages=messages, resource_class=messages.RouterBgpPeer, args=args)

      attrs = {
          'advertiseMode': mode,
          'advertisedGroups': groups,
          'advertisedIpRanges': ranges,
      }

      for attr, value in six.iteritems(attrs):
        if value is not None:
          setattr(peer, attr, value)

    replacement.bgpPeers.append(peer)

    result = service.Patch(
        messages.ComputeRoutersPatchRequest(
            project=router_ref.project,
            region=router_ref.region,
            router=router_ref.Name(),
            routerResource=replacement))

    operation_ref = resources.REGISTRY.Parse(
        result.name,
        collection='compute.regionOperations',
        params={
            'project': router_ref.project,
            'region': router_ref.region,
        })

    if args.async_:
      log.UpdatedResource(
          operation_ref,
          kind='router [{0}] to add peer [{1}]'.format(router_ref.Name(),
                                                       peer.name),
          is_async=True,
          details='Run the [gcloud compute operations describe] command '
          'to check the status of this operation.')
      return result

    target_router_ref = holder.resources.Parse(
        router_ref.Name(),
        collection='compute.routers',
        params={
            'project': router_ref.project,
            'region': router_ref.region,
        })

    operation_poller = poller.Poller(service, target_router_ref)
    return waiter.WaitFor(operation_poller, operation_ref,
                          'Creating peer [{0}] in router [{1}]'.format(
                              peer.name, router_ref.Name()))

  def Run(self, args):
    """See base.UpdateCommand."""
    return self._Run(args)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class AddBgpPeerBeta(AddBgpPeer):
  """Add a BGP peer to a Compute Engine router."""

  ROUTER_ARG = None

  @classmethod
  def Args(cls, parser):
    cls._Args(parser, support_bfd=True, support_enable=True)

  def Run(self, args):
    """See base.UpdateCommand."""
    return self._Run(
        args, support_bfd=True, support_enable=True, support_bfd_mode=False)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class AddBgpPeerAlpha(AddBgpPeerBeta):
  """Add a BGP peer to a Compute Engine router."""

  ROUTER_ARG = None
  INSTANCE_ARG = None

  @classmethod
  def Args(cls, parser):
    cls.INSTANCE_ARG = instance_flags.InstanceArgumentForRouter()
    cls.INSTANCE_ARG.AddArgument(parser)
    cls._Args(parser, support_bfd=True, support_enable=True)

  def Run(self, args):
    """See base.UpdateCommand."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())

    instance_ref = None
    if args.instance is not None:
      instance_ref = self.INSTANCE_ARG.ResolveAsResource(
          args,
          holder.resources,
          scope_lister=instance_flags.GetInstanceZoneScopeLister(holder.client))
    return self._Run(
        args,
        support_bfd=True,
        support_enable=True,
        support_bfd_mode=True,
        instance_ref=instance_ref)


def _CreateBgpPeerMessage(messages,
                          args,
                          support_bfd=False,
                          support_enable=False,
                          support_bfd_mode=False,
                          instance_ref=None):
  """Creates a BGP peer with base attributes based on flag arguments."""
  bfd = None
  if support_bfd:
    if support_bfd_mode:
      bfd = _CreateBgpPeerBfdMessageMode(messages, args)
    else:
      bfd = _CreateBgpPeerBfdMessage(messages, args)
  enable = None
  if support_enable and args.enabled is not None:
    if args.enabled:
      enable = messages.RouterBgpPeer.EnableValueValuesEnum.TRUE
    else:
      enable = messages.RouterBgpPeer.EnableValueValuesEnum.FALSE

  if instance_ref is not None:
    return messages.RouterBgpPeer(
        name=args.peer_name,
        interfaceName=args.interface,
        peerIpAddress=args.peer_ip_address,
        peerAsn=args.peer_asn,
        advertisedRoutePriority=args.advertised_route_priority,
        enable=enable,
        routerApplianceInstance=instance_ref.SelfLink(),
        bfd=bfd)
  elif support_bfd or support_enable:
    return messages.RouterBgpPeer(
        name=args.peer_name,
        interfaceName=args.interface,
        peerIpAddress=args.peer_ip_address,
        peerAsn=args.peer_asn,
        advertisedRoutePriority=args.advertised_route_priority,
        enable=enable,
        bfd=bfd)
  else:
    return messages.RouterBgpPeer(
        name=args.peer_name,
        interfaceName=args.interface,
        peerIpAddress=args.peer_ip_address,
        peerAsn=args.peer_asn,
        advertisedRoutePriority=args.advertised_route_priority)


def _CreateBgpPeerBfdMessage(messages, args):
  """Creates a BGP peer with base attributes based on flag arguments."""
  if not (args.IsSpecified('bfd_min_receive_interval') or
          args.IsSpecified('bfd_min_transmit_interval') or
          args.IsSpecified('bfd_session_initialization_mode') or
          args.IsSpecified('bfd_multiplier')):
    return None
  bfd_session_initialization_mode = None
  if args.bfd_session_initialization_mode is not None:
    bfd_session_initialization_mode = (
        messages.RouterBgpPeerBfd.SessionInitializationModeValueValuesEnum(
            args.bfd_session_initialization_mode))
  return messages.RouterBgpPeerBfd(
      minReceiveInterval=args.bfd_min_receive_interval,
      minTransmitInterval=args.bfd_min_transmit_interval,
      sessionInitializationMode=bfd_session_initialization_mode,
      multiplier=args.bfd_multiplier,
  )


def _CreateBgpPeerBfdMessageMode(messages, args):
  """Creates a BGP peer with base attributes based on flag arguments."""
  if not (args.IsSpecified('bfd_min_receive_interval') or
          args.IsSpecified('bfd_min_transmit_interval') or
          args.IsSpecified('bfd_session_initialization_mode') or
          args.IsSpecified('bfd_multiplier')):
    return None
  mode = None
  bfd_session_initialization_mode = None
  if args.bfd_session_initialization_mode is not None:
    mode = messages.RouterBgpPeerBfd.ModeValueValuesEnum(
        args.bfd_session_initialization_mode)
    bfd_session_initialization_mode = (
        messages.RouterBgpPeerBfd.SessionInitializationModeValueValuesEnum(
            args.bfd_session_initialization_mode))
  return messages.RouterBgpPeerBfd(
      minReceiveInterval=args.bfd_min_receive_interval,
      minTransmitInterval=args.bfd_min_transmit_interval,
      mode=mode,
      sessionInitializationMode=bfd_session_initialization_mode,
      multiplier=args.bfd_multiplier,
  )
