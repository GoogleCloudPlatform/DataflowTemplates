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
"""Command for creating forwarding rules."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import ipaddress

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import constants
from googlecloudsdk.api_lib.compute import forwarding_rules_utils as utils
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.forwarding_rules import flags
from googlecloudsdk.core import log
import six
from six.moves import range  # pylint: disable=redefined-builtin


def _Args(parser, support_global_access, support_l7_internal_load_balancing,
          support_target_grpc_proxy, support_psc_google_apis,
          support_all_protocol, support_target_service_attachment):
  """Add the flags to create a forwarding rule."""

  flags.AddUpdateArgs(
      parser,
      include_l7_internal_load_balancing=support_l7_internal_load_balancing,
      include_target_grpc_proxy=support_target_grpc_proxy,
      include_psc_google_apis=support_psc_google_apis,
      include_target_service_attachment=support_target_service_attachment)
  flags.AddIPProtocols(parser, support_all_protocol)
  flags.AddDescription(parser)
  flags.AddPortsAndPortRange(parser)
  flags.AddNetworkTier(
      parser, supports_network_tier_flag=True, for_update=False)

  if support_global_access:
    flags.AddAllowGlobalAccess(parser)

  flags.AddIsMirroringCollector(parser)

  parser.add_argument(
      '--service-label',
      help='(Only for Internal Load Balancing): '
      'https://cloud.google.com/load-balancing/docs/dns-names/\n'
      'The DNS label to use as the prefix of the fully qualified domain '
      'name for this forwarding rule. The full name will be internally '
      'generated and output as dnsName. If this field is not specified, '
      'no DNS record will be generated and no DNS name will be output. '
      'You cannot use the `--service-label` flag  if the forwarding rule '
      'references an internal IP address that has the '
      '`--purpose=SHARED_LOADBALANCER_VIP` flag set.')
  flags.AddAddressesAndIPVersions(
      parser,
      required=False,
      include_l7_internal_load_balancing=support_l7_internal_load_balancing)
  forwarding_rule_arg = flags.ForwardingRuleArgument()
  forwarding_rule_arg.AddArgument(parser, operation_type='create')
  parser.display_info.AddCacheUpdater(flags.ForwardingRulesCompleter)
  return forwarding_rule_arg


class CreateHelper(object):
  """Helper class to create a forwarding rule."""

  FORWARDING_RULE_ARG = None

  def __init__(self, holder, support_global_access,
               support_l7_internal_load_balancing, support_target_grpc_proxy,
               support_psc_google_apis, support_all_protocol,
               support_target_service_attachment):
    self._holder = holder
    self._support_global_access = support_global_access
    self._support_l7_internal_load_balancing = support_l7_internal_load_balancing
    self._support_target_grpc_proxy = support_target_grpc_proxy
    self._support_psc_google_apis = support_psc_google_apis
    self._support_all_protocol = support_all_protocol
    self._support_target_service_attachment = support_target_service_attachment

  @classmethod
  def Args(cls, parser, support_global_access,
           support_l7_internal_load_balancing, support_target_grpc_proxy,
           support_psc_google_apis, support_all_protocol,
           support_target_service_attachment):
    cls.FORWARDING_RULE_ARG = _Args(parser, support_global_access,
                                    support_l7_internal_load_balancing,
                                    support_target_grpc_proxy,
                                    support_psc_google_apis,
                                    support_all_protocol,
                                    support_target_service_attachment)

  def ConstructProtocol(self, messages, args):
    if args.ip_protocol:
      return messages.ForwardingRule.IPProtocolValueValuesEnum(args.ip_protocol)
    else:
      return

  def Run(self, args):
    """Issues requests necessary to create Forwarding Rules."""
    client = self._holder.client

    forwarding_rule_ref = self.FORWARDING_RULE_ARG.ResolveAsResource(
        args,
        self._holder.resources,
        scope_lister=compute_flags.GetDefaultScopeLister(client))

    if forwarding_rule_ref.Collection() == 'compute.globalForwardingRules':
      requests = self._CreateGlobalRequests(client, self._holder.resources,
                                            args, forwarding_rule_ref)
    elif forwarding_rule_ref.Collection() == 'compute.forwardingRules':
      requests = self._CreateRegionalRequests(client, self._holder.resources,
                                              args, forwarding_rule_ref)

    return client.MakeRequests(requests)

  def _CreateGlobalRequests(self, client, resources, args, forwarding_rule_ref):
    """Create a globally scoped request."""

    is_psc_google_apis = False
    if hasattr(args,
               'target_google_apis_bundle') and args.target_google_apis_bundle:
      if not self._support_psc_google_apis:
        raise exceptions.InvalidArgumentException(
            '--target-google-apis-bundle',
            'Private Service Connect for Google APIs (the target-google-apis-bundle option '
            'for forwarding rules) is not supported in this API version.')
      else:
        is_psc_google_apis = True

    ports_all_specified, range_list = _ExtractPortsAndAll(args.ports)
    port_range = _ResolvePortRange(args.port_range, range_list)
    load_balancing_scheme = _GetLoadBalancingScheme(args, client.messages,
                                                    is_psc_google_apis)

    if ports_all_specified and not is_psc_google_apis:
      raise exceptions.ToolException(
          '[--ports] can not be specified to all for global forwarding rules.')
    if not is_psc_google_apis and not port_range:
      raise exceptions.ToolException(
          '[--ports] is required for global forwarding rules.')

    if is_psc_google_apis:
      rule_name = forwarding_rule_ref.Name()
      if len(rule_name) > 20 or rule_name[0].isdigit(
      ) or not rule_name.isalnum():
        raise exceptions.ToolException(
            'A forwarding rule to Google APIs must have a name that is between '
            ' 1-20 characters long, alphanumeric, starting with a letter.')

      if port_range:
        raise exceptions.ToolException(
            '[--ports] is not allowed for PSC-GoogleApis forwarding rules.')
      if load_balancing_scheme:
        raise exceptions.InvalidArgumentException(
            '--load-balancing-scheme',
            'The --load-balancing-scheme flag is not allowed for PSC-GoogleApis'
            ' forwarding rules.')

      if args.target_google_apis_bundle in flags.PSC_GOOGLE_APIS_BUNDLES:
        target_as_str = args.target_google_apis_bundle
      else:
        bundles_list = ', '.join(flags.PSC_GOOGLE_APIS_BUNDLES)
        raise exceptions.InvalidArgumentException(
            '--target-google-apis-bundle',
            'The valid values for target-google-apis-bundle are: ' +
            bundles_list)
    else:
      target_ref = utils.GetGlobalTarget(resources, args,
                                         self._support_target_grpc_proxy)
      target_as_str = target_ref.SelfLink()
    protocol = self.ConstructProtocol(client.messages, args)

    if args.address is None or args.ip_version:
      ip_version = client.messages.ForwardingRule.IpVersionValueValuesEnum(
          args.ip_version or 'IPV4')
    else:
      ip_version = None

    address = self._ResolveAddress(resources, args,
                                   compute_flags.compute_scope.ScopeEnum.GLOBAL,
                                   forwarding_rule_ref)
    forwarding_rule = client.messages.ForwardingRule(
        description=args.description,
        name=forwarding_rule_ref.Name(),
        IPAddress=address,
        IPProtocol=protocol,
        portRange=port_range,
        target=target_as_str,
        ipVersion=ip_version,
        networkTier=_ConstructNetworkTier(client.messages, args),
        loadBalancingScheme=load_balancing_scheme)

    if args.IsSpecified('network'):
      forwarding_rule.network = flags.NetworkArg(
          self._support_l7_internal_load_balancing).ResolveAsResource(
              args, resources).SelfLink()

    if self._support_global_access and args.IsSpecified('allow_global_access'):
      forwarding_rule.allowGlobalAccess = args.allow_global_access

    request = client.messages.ComputeGlobalForwardingRulesInsertRequest(
        forwardingRule=forwarding_rule, project=forwarding_rule_ref.project)

    return [(client.apitools_client.globalForwardingRules, 'Insert', request)]

  def _CreateRegionalRequests(self, client, resources, args,
                              forwarding_rule_ref):
    """Create a regionally scoped request."""
    is_psc_ilb = False
    if hasattr(args,
               'target_service_attachment') and args.target_service_attachment:
      if not self._support_target_service_attachment:
        raise exceptions.InvalidArgumentException(
            '--target-service-attachment',
            'Private Service Connect for ILB (the target-service-attachment '
            'option) is not supported in this API version.')
      else:
        is_psc_ilb = True

    target_ref, region_ref = utils.GetRegionalTarget(
        client,
        resources,
        args,
        forwarding_rule_ref,
        include_l7_internal_load_balancing=self
        ._support_l7_internal_load_balancing,
        include_target_service_attachment=self
        ._support_target_service_attachment)
    if not args.region and region_ref:
      args.region = region_ref
    protocol = self.ConstructProtocol(client.messages, args)

    address = self._ResolveAddress(resources, args,
                                   compute_flags.compute_scope.ScopeEnum.REGION,
                                   forwarding_rule_ref)
    load_balancing_scheme = _GetLoadBalancingScheme(args, client.messages,
                                                    is_psc_ilb)
    if is_psc_ilb and load_balancing_scheme:
      raise exceptions.InvalidArgumentException(
          '--load-balancing-scheme',
          'The --load-balancing-scheme flag is not allowed for PSC-ILB '
          'forwarding rules.')

    forwarding_rule = client.messages.ForwardingRule(
        description=args.description,
        name=forwarding_rule_ref.Name(),
        IPAddress=address,
        IPProtocol=protocol,
        networkTier=_ConstructNetworkTier(client.messages, args),
        loadBalancingScheme=load_balancing_scheme)

    ports_all_specified, range_list = _ExtractPortsAndAll(args.ports)
    if target_ref.Collection() == 'compute.serviceAttachments':
      forwarding_rule.target = target_ref.SelfLink()
      if args.network is not None:
        forwarding_rule.network = flags.NetworkArg(
            self._support_l7_internal_load_balancing).ResolveAsResource(
                args, resources).SelfLink()
    elif (target_ref.Collection() == 'compute.regionBackendServices') or (
        target_ref.Collection() == 'compute.targetInstances' and
        args.load_balancing_scheme == 'INTERNAL'):
      forwarding_rule.portRange = (
          six.text_type(args.port_range) if args.port_range else None)
      if target_ref.Collection() == 'compute.regionBackendServices':
        forwarding_rule.backendService = target_ref.SelfLink()
      else:
        forwarding_rule.target = target_ref.SelfLink()
      if ports_all_specified:
        forwarding_rule.allPorts = True
      if range_list:
        forwarding_rule.portRange = None
        forwarding_rule.ports = [
            six.text_type(p) for p in _GetPortList(range_list)
        ]
      if args.subnet is not None:
        if not args.subnet_region:
          args.subnet_region = forwarding_rule_ref.region
        forwarding_rule.subnetwork = flags.SUBNET_ARG.ResolveAsResource(
            args, resources).SelfLink()
      if args.network is not None:
        forwarding_rule.network = flags.NetworkArg(
            self._support_l7_internal_load_balancing).ResolveAsResource(
                args, resources).SelfLink()
    elif ((target_ref.Collection() == 'compute.regionTargetHttpProxies' or
           target_ref.Collection() == 'compute.regionTargetHttpsProxies') and
          args.load_balancing_scheme == 'INTERNAL'):
      forwarding_rule.ports = [
          six.text_type(p) for p in _GetPortList(range_list)
      ]
      if args.subnet is not None:
        if not args.subnet_region:
          args.subnet_region = forwarding_rule_ref.region
        forwarding_rule.subnetwork = flags.SUBNET_ARG.ResolveAsResource(
            args, resources).SelfLink()
      if args.network is not None:
        forwarding_rule.network = flags.NetworkArg(
            self._support_l7_internal_load_balancing).ResolveAsResource(
                args, resources).SelfLink()
      forwarding_rule.target = target_ref.SelfLink()
    elif args.load_balancing_scheme == 'INTERNAL':
      raise exceptions.InvalidArgumentException(
          '--load-balancing-scheme',
          'Only target instances and backend services should be specified as '
          'a target for internal load balancing.')
    elif args.load_balancing_scheme == 'INTERNAL_MANAGED':
      forwarding_rule.portRange = (
          _ResolvePortRange(args.port_range, range_list))
      if args.subnet is not None:
        if not args.subnet_region:
          args.subnet_region = forwarding_rule_ref.region
        forwarding_rule.subnetwork = flags.SUBNET_ARG.ResolveAsResource(
            args, resources).SelfLink()
      if args.network is not None:
        forwarding_rule.network = flags.NetworkArg(
            self._support_l7_internal_load_balancing).ResolveAsResource(
                args, resources).SelfLink()
      forwarding_rule.target = target_ref.SelfLink()
    else:
      forwarding_rule.portRange = (
          _ResolvePortRange(args.port_range, range_list))
      forwarding_rule.target = target_ref.SelfLink()
    if hasattr(args, 'service_label'):
      forwarding_rule.serviceLabel = args.service_label

    if self._support_global_access and args.IsSpecified('allow_global_access'):
      forwarding_rule.allowGlobalAccess = args.allow_global_access

    if hasattr(args, 'is_mirroring_collector'):
      forwarding_rule.isMirroringCollector = args.is_mirroring_collector

    request = client.messages.ComputeForwardingRulesInsertRequest(
        forwardingRule=forwarding_rule,
        project=forwarding_rule_ref.project,
        region=forwarding_rule_ref.region)

    return [(client.apitools_client.forwardingRules, 'Insert', request)]

  def _ResolveAddress(self, resources, args, scope, forwarding_rule_ref):
    """Resolve address resource."""

    # Address takes either an ip address or an address resource. If parsing as
    # an IP address fails, then we resolve as a resource.
    address = args.address
    if address is not None:
      try:
        # ipaddress only allows unicode input
        ipaddress.ip_address(six.text_type(args.address))
      except ValueError:
        # TODO(b/37086838): Make sure global/region settings are inherited by
        # address resource.
        if scope == compute_flags.compute_scope.ScopeEnum.REGION:
          if not args.global_address and not args.address_region:
            if forwarding_rule_ref.Collection() == 'compute.forwardingRules':
              args.address_region = forwarding_rule_ref.region
        address_ref = flags.AddressArg(
            self._support_l7_internal_load_balancing).ResolveAsResource(
                args, resources, default_scope=scope)
        address = address_ref.SelfLink()

    return address


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Create a forwarding rule to direct network traffic to a load balancer."""

  _support_global_access = True
  _support_l7_internal_load_balancing = True
  _support_target_grpc_proxy = True
  _support_psc_google_apis = False
  _support_all_protocol = False
  _support_target_service_attachment = False

  @classmethod
  def Args(cls, parser):
    CreateHelper.Args(parser, cls._support_global_access,
                      cls._support_l7_internal_load_balancing,
                      cls._support_target_grpc_proxy,
                      cls._support_psc_google_apis, cls._support_all_protocol,
                      cls._support_target_service_attachment)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return CreateHelper(holder, self._support_global_access,
                        self._support_l7_internal_load_balancing,
                        self._support_target_grpc_proxy,
                        self._support_psc_google_apis,
                        self._support_all_protocol,
                        self._support_target_service_attachment).Run(args)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):
  """Create a forwarding rule to direct network traffic to a load balancer."""
  _support_global_access = True
  _support_l7_internal_load_balancing = True
  _support_target_grpc_proxy = True
  _support_psc_google_apis = True
  _support_all_protocol = False
  _support_target_service_attachment = False


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):
  """Create a forwarding rule to direct network traffic to a load balancer."""
  _support_global_access = True
  _support_l7_internal_load_balancing = True
  _support_target_grpc_proxy = True
  _support_psc_google_apis = True
  _support_all_protocol = True
  _support_target_service_attachment = True


Create.detailed_help = {
    'DESCRIPTION': ("""
*{{command}}* is used to create a forwarding rule. {overview}

When creating a forwarding rule, exactly one of  ``--target-instance'',
``--target-pool'', ``--target-http-proxy'', ``--target-https-proxy'',
``--target-grpc-proxy'', ``--target-ssl-proxy'', ``--target-tcp-proxy'',
``--target-vpn-gateway'' or ``--backend-service'' must be specified.""".format(
    overview=flags.FORWARDING_RULES_OVERVIEW)),
    'EXAMPLES':
        """
    To create a global forwarding rule that will forward all traffic on port
    8080 for IP address ADDRESS to a target http proxy PROXY, run:

      $ {command} RULE_NAME --global --target-http-proxy=PROXY --ports=8080 --address=ADDRESS

    To create a regional forwarding rule for the subnet SUBNET_NAME on the
    default network that will forward all traffic on ports 80-82 to a
    backend service SERVICE_NAME, run:

      $ {command} RULE_NAME --load-balancing-scheme=INTERNAL --backend-service=SERVICE_NAME --subnet=SUBNET_NAME --network=default --region=REGION --ports=80-82
"""
}

CreateBeta.detailed_help = Create.detailed_help
CreateAlpha.detailed_help = Create.detailed_help


def _GetPortRange(ports_range_list):
  """Return single range by combining the ranges."""
  if not ports_range_list:
    return None, None
  ports = sorted(ports_range_list)
  combined_port_range = ports.pop(0)
  for port_range in ports_range_list:
    try:
      combined_port_range = combined_port_range.Combine(port_range)
    except arg_parsers.Error:
      raise exceptions.InvalidArgumentException(
          '--ports', 'Must specify consecutive ports at this time.')
  return combined_port_range


def _ExtractPortsAndAll(ports_with_all):
  if ports_with_all:
    return ports_with_all.all_specified, ports_with_all.ranges
  else:
    return False, []


def _ResolvePortRange(port_range, port_range_list):
  """Reconciles deprecated port_range value and list of port ranges."""
  if port_range:
    log.warning(
        'The --port-range flag is deprecated. Use equivalent --ports=%s'
        ' flag.', port_range)
  elif port_range_list:
    port_range = _GetPortRange(port_range_list)
  return six.text_type(port_range) if port_range else None


def _GetPortList(range_list):
  ports = []
  for port_range in range_list:
    ports.extend(list(range(port_range.start, port_range.end + 1)))
  return sorted(ports)


def _GetLoadBalancingScheme(args, messages, is_psc):
  """Get load balancing scheme."""
  if not args.load_balancing_scheme:
    # The default is EXTERNAL for non-PSC forwarding rules.
    return None if is_psc else messages.ForwardingRule.LoadBalancingSchemeValueValuesEnum.EXTERNAL
  if args.load_balancing_scheme == 'INTERNAL':
    return messages.ForwardingRule.LoadBalancingSchemeValueValuesEnum.INTERNAL
  elif args.load_balancing_scheme == 'EXTERNAL':
    return messages.ForwardingRule.LoadBalancingSchemeValueValuesEnum.EXTERNAL
  elif args.load_balancing_scheme == 'INTERNAL_SELF_MANAGED':
    return (messages.ForwardingRule.LoadBalancingSchemeValueValuesEnum
            .INTERNAL_SELF_MANAGED)
  elif args.load_balancing_scheme == 'INTERNAL_MANAGED':
    return (messages.ForwardingRule.LoadBalancingSchemeValueValuesEnum
            .INTERNAL_MANAGED)
  return None


def _ConstructNetworkTier(messages, args):
  """Get network tier."""
  if args.network_tier:
    network_tier = args.network_tier.upper()
    if network_tier in constants.NETWORK_TIER_CHOICES_FOR_INSTANCE:
      return messages.ForwardingRule.NetworkTierValueValuesEnum(
          args.network_tier)
    else:
      raise exceptions.InvalidArgumentException(
          '--network-tier',
          'Invalid network tier [{tier}]'.format(tier=network_tier))
  else:
    return
