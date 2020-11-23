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
"""Command for creating subnetworks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import utils as compute_api
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.networks import flags as network_flags
from googlecloudsdk.command_lib.compute.networks.subnets import flags
from googlecloudsdk.command_lib.util.apis import arg_utils
import six


def _DetailedHelp():
  return {
      'brief':
          'Define a subnet for a network in custom subnet mode.',
      'DESCRIPTION':
          """\
      Define a subnet for a network in custom subnet mode. Subnets must be
      uniquely named per region.
      """,
  }


def _AddArgs(parser, include_alpha_logging, include_l7_internal_load_balancing,
             include_aggregate_purpose, include_private_service_connect,
             include_stack_type, include_ipv6_access_type, api_version):
  """Add subnetwork create arguments to parser."""
  parser.display_info.AddFormat(flags.DEFAULT_LIST_FORMAT)

  flags.SubnetworkArgument().AddArgument(parser, operation_type='create')
  network_flags.NetworkArgumentForOtherResource(
      'The network to which the subnetwork belongs.').AddArgument(parser)

  messages = apis.GetMessagesModule('compute',
                                    compute_api.COMPUTE_GA_API_VERSION)

  parser.add_argument(
      '--description', help='An optional description of this subnetwork.')

  parser.add_argument(
      '--range',
      required=True,
      help='The IP space allocated to this subnetwork in CIDR format.')

  parser.add_argument(
      '--enable-private-ip-google-access',
      action='store_true',
      default=False,
      help=('Enable/disable access to Google Cloud APIs from this subnet for '
            'instances without a public ip address.'))

  parser.add_argument(
      '--secondary-range',
      type=arg_parsers.ArgDict(min_length=1),
      action='append',
      metavar='PROPERTY=VALUE',
      help="""\
      Adds a secondary IP range to the subnetwork for use in IP aliasing.

      For example, `--secondary-range range1=192.168.64.0/24` adds
      a secondary range 192.168.64.0/24 with name range1.

      * `RANGE_NAME` - Name of the secondary range.
      * `RANGE` - `IP range in CIDR format.`
      """)

  parser.add_argument(
      '--enable-flow-logs',
      action='store_true',
      default=None,
      help=('Enable/disable VPC Flow Logs for this subnet. More information '
            'for VPC Flow Logs can be found at '
            'https://cloud.google.com/vpc/docs/using-flow-logs.'))

  flags.AddLoggingAggregationInterval(parser, messages)
  parser.add_argument(
      '--logging-flow-sampling',
      type=arg_parsers.BoundedFloat(lower_bound=0.0, upper_bound=1.0),
      help="""\
      Can only be specified if VPC Flow Logs for this subnetwork is
      enabled. The value of the field must be in [0, 1]. Set the sampling rate
      of VPC flow logs within the subnetwork where 1.0 means all collected
      logs are reported and 0.0 means no logs are reported. Default is 0.5
      which means half of all collected logs are reported.
      """)

  parser.add_argument(
      '--logging-filter-expr',
      help="""\
        Can only be specified if VPC Flow Logs for this subnetwork is enabled.
        Export filter used to define which logs should be generated.
        """)
  flags.AddLoggingMetadata(parser, messages)
  parser.add_argument(
      '--logging-metadata-fields',
      type=arg_parsers.ArgList(),
      metavar='METADATA_FIELD',
      default=None,
      help="""\
      Can only be specified if VPC Flow Logs for this subnetwork is enabled
      and "metadata" is set to CUSTOM_METADATA. The comma-separated list of
      metadata fields that should be added to reported logs.
      """)

  if include_alpha_logging:
    messages = apis.GetMessagesModule('compute',
                                      compute_api.COMPUTE_ALPHA_API_VERSION)
    flags.AddLoggingAggregationIntervalDeprecated(parser, messages)
    parser.add_argument(
        '--flow-sampling',
        type=arg_parsers.BoundedFloat(lower_bound=0.0, upper_bound=1.0),
        help="""\
        Can only be specified if VPC flow logging for this subnetwork is
        enabled. The value of the field must be in [0, 1]. Set the sampling rate
        of VPC flow logs within the subnetwork where 1.0 means all collected
        logs are reported and 0.0 means no logs are reported. Default is 0.5
        which means half of all collected logs are reported.
        """)
    flags.AddLoggingMetadataDeprecated(parser, messages)

  purpose_choices = {
      'PRIVATE':
          'Regular user created or automatically created subnet.',
      'INTERNAL_HTTPS_LOAD_BALANCER':
          'Reserved for Internal HTTP(S) Load Balancing.',
  }

  if include_aggregate_purpose:
    purpose_choices['AGGREGATE'] = (
        'Reserved for Aggregate Ranges used for aggregating '
        'private subnetworks.')

  if include_private_service_connect:
    purpose_choices['PRIVATE_SERVICE_CONNECT'] = (
        'Reserved for Private Service Connect Internal Load Balancing.')

  # Subnetwork purpose is introduced with L7ILB feature. Aggregate purpose
  # will have to be enabled for a given release track only after L7ILB feature
  # is enabled for that release track. Hence if include_aggregate_purpose
  # true, this code assumes that L7ILB purpose is enabled.
  if include_l7_internal_load_balancing:
    parser.add_argument(
        '--purpose',
        choices=purpose_choices,
        type=arg_utils.ChoiceToEnumName,
        help='The purpose of this subnetwork.')

  if include_l7_internal_load_balancing:
    parser.add_argument(
        '--role',
        choices={
            'ACTIVE': 'The ACTIVE subnet that is currently used.',
            'BACKUP': 'The BACKUP subnet that could be promoted to ACTIVE.'
        },
        type=lambda x: x.replace('-', '_').upper(),
        help=('The role of subnetwork. This field is required when '
              'purpose=INTERNAL_HTTPS_LOAD_BALANCER. The value can be set to '
              'ACTIVE or BACKUP. An ACTIVE subnetwork is one that is currently '
              'being used for Internal HTTP(S) Load Balancing. A BACKUP '
              'subnetwork is one that is ready to be promoted to ACTIVE or is '
              'currently draining.'))

  # Add private ipv6 google access enum based on api version.
  messages = apis.GetMessagesModule('compute', api_version)
  GetPrivateIpv6GoogleAccessTypeFlagMapper(messages).choice_arg.AddToParser(
      parser)

  if include_stack_type:
    parser.add_argument(
        '--stack-type',
        choices={
            'IPV4_ONLY':
                'New VMs in this subnet will only be assigned IPv4 addresses',
            'IPV4_IPV6':
                'New VMs in this subnet can have both IPv4 and IPv6 addresses'
        },
        type=arg_utils.ChoiceToEnumName,
        help=('The stack type for this subnet to identify whether the IPv6 '
              'feature is enabled or not. If not specified IPV4_ONLY will be '
              'used.'))

  if include_ipv6_access_type:
    parser.add_argument(
        '--ipv6-access-type',
        choices={
            'INTERNAL': 'VMs in this subnet can have internal IPv6.',
            'EXTERNAL': 'VMs in this subnet can have external IPv6.'
        },
        type=arg_utils.ChoiceToEnumName,
        help=('The access type of IPv6 address this subnet holds. It\'s '
              'immutable and can only be specified during creation or the '
              'time the subnet is updated into IPV4_IPV6 dual stack. If the '
              'ipv6 access type is EXTERNAL then this subnet cannot enable '
              'direct path.'))

  parser.display_info.AddCacheUpdater(network_flags.NetworksCompleter)


def GetPrivateIpv6GoogleAccessTypeFlagMapper(messages):
  return arg_utils.ChoiceEnumMapper(
      '--private-ipv6-google-access-type',
      messages.Subnetwork.PrivateIpv6GoogleAccessValueValuesEnum,
      custom_mappings={
          'DISABLE_GOOGLE_ACCESS':
              'disable',
          'ENABLE_BIDIRECTIONAL_ACCESS_TO_GOOGLE':
              'enable-bidirectional-access',
          'ENABLE_OUTBOUND_VM_ACCESS_TO_GOOGLE':
              'enable-outbound-vm-access'
      },
      help_str='The private IPv6 google access type for the VMs in this subnet.'
  )


def _CreateSubnetwork(messages, subnet_ref, network_ref, args,
                      include_alpha_logging, include_l7_internal_load_balancing,
                      include_aggregate_purpose,
                      include_private_service_connect, include_stack_type,
                      include_ipv6_access_type):
  """Create the subnet resource."""
  subnetwork = messages.Subnetwork(
      name=subnet_ref.Name(),
      description=args.description,
      network=network_ref.SelfLink(),
      ipCidrRange=args.range,
      privateIpGoogleAccess=args.enable_private_ip_google_access)

  if (args.enable_flow_logs is not None or
      args.logging_aggregation_interval is not None or
      args.logging_flow_sampling is not None or
      args.logging_metadata is not None or
      args.logging_filter_expr is not None or
      args.logging_metadata_fields is not None):
    log_config = messages.SubnetworkLogConfig(enable=args.enable_flow_logs)
    if args.logging_aggregation_interval:
      log_config.aggregationInterval = flags.GetLoggingAggregationIntervalArg(
          messages).GetEnumForChoice(args.logging_aggregation_interval)
    if args.logging_flow_sampling is not None:
      log_config.flowSampling = args.logging_flow_sampling
    if args.logging_metadata:
      log_config.metadata = flags.GetLoggingMetadataArg(
          messages).GetEnumForChoice(args.logging_metadata)
    if args.logging_filter_expr is not None:
      log_config.filterExpr = args.logging_filter_expr
    if args.logging_metadata_fields is not None:
      log_config.metadataFields = args.logging_metadata_fields
    subnetwork.logConfig = log_config

  if include_alpha_logging:
    if (args.enable_flow_logs is not None or
        args.aggregation_interval is not None or
        args.flow_sampling is not None or args.metadata is not None):
      log_config = (
          subnetwork.logConfig if subnetwork.logConfig is not None else
          messages.SubnetworkLogConfig(enable=args.enable_flow_logs))
      if args.aggregation_interval:
        log_config.aggregationInterval = (
            flags.GetLoggingAggregationIntervalArgDeprecated(
                messages).GetEnumForChoice(args.aggregation_interval))
      if args.flow_sampling is not None:
        log_config.flowSampling = args.flow_sampling
      if args.metadata:
        log_config.metadata = flags.GetLoggingMetadataArgDeprecated(
            messages).GetEnumForChoice(args.metadata)
      if args.logging_filter_expr is not None:
        log_config.filterExpr = args.logging_filter_expr
      if args.logging_metadata_fields is not None:
        log_config.metadataFields = args.logging_metadata_fields
      subnetwork.logConfig = log_config

  if include_l7_internal_load_balancing:
    if args.purpose:
      subnetwork.purpose = messages.Subnetwork.PurposeValueValuesEnum(
          args.purpose)
    if (subnetwork.purpose == messages.Subnetwork.PurposeValueValuesEnum
        .INTERNAL_HTTPS_LOAD_BALANCER):
      # Clear unsupported fields in the subnet resource
      subnetwork.privateIpGoogleAccess = None
      subnetwork.enableFlowLogs = None
      subnetwork.logConfig = None
    if getattr(args, 'role', None):
      subnetwork.role = messages.Subnetwork.RoleValueValuesEnum(args.role)

  # At present aggregate purpose is available only in alpha whereas
  # https_load_balancer is available in Beta. Given Aggregate Purpose Enum
  # is not available in Beta, the code duplication below is necessary.
  if include_aggregate_purpose:
    if args.purpose:
      subnetwork.purpose = messages.Subnetwork.PurposeValueValuesEnum(
          args.purpose)
      if (subnetwork.purpose ==
          messages.Subnetwork.PurposeValueValuesEnum.AGGREGATE):
        # Clear unsupported fields in the subnet resource
        subnetwork.privateIpGoogleAccess = None
        subnetwork.enableFlowLogs = None
        subnetwork.logConfig = None

  if include_private_service_connect:
    if args.purpose:
      subnetwork.purpose = messages.Subnetwork.PurposeValueValuesEnum(
          args.purpose)
      if (subnetwork.purpose ==
          messages.Subnetwork.PurposeValueValuesEnum.PRIVATE_SERVICE_CONNECT):
        # Clear unsupported fields in the subnet resource
        subnetwork.privateIpGoogleAccess = None
        subnetwork.enableFlowLogs = None
        subnetwork.logConfig = None

  if args.private_ipv6_google_access_type is not None:
    subnetwork.privateIpv6GoogleAccess = (
        flags.GetPrivateIpv6GoogleAccessTypeFlagMapper(
            messages).GetEnumForChoice(args.private_ipv6_google_access_type))

  if include_stack_type and args.stack_type:
    subnetwork.stackType = messages.Subnetwork.StackTypeValueValuesEnum(
        args.stack_type)

  if include_ipv6_access_type and args.ipv6_access_type:
    subnetwork.ipv6AccessType = (
        messages.Subnetwork.Ipv6AccessTypeValueValuesEnum(
            args.ipv6_access_type))

  return subnetwork


def _Run(args, holder, include_alpha_logging,
         include_l7_internal_load_balancing, include_aggregate_purpose,
         include_private_service_connect, include_stack_type,
         include_ipv6_access_type):
  """Issues a list of requests necessary for adding a subnetwork."""
  client = holder.client

  network_ref = network_flags.NetworkArgumentForOtherResource(
      'The network to which the subnetwork belongs.').ResolveAsResource(
          args, holder.resources)
  subnet_ref = flags.SubnetworkArgument().ResolveAsResource(
      args,
      holder.resources,
      scope_lister=compute_flags.GetDefaultScopeLister(client))

  subnetwork = _CreateSubnetwork(client.messages, subnet_ref, network_ref, args,
                                 include_alpha_logging,
                                 include_l7_internal_load_balancing,
                                 include_aggregate_purpose,
                                 include_private_service_connect,
                                 include_stack_type, include_ipv6_access_type)
  request = client.messages.ComputeSubnetworksInsertRequest(
      subnetwork=subnetwork,
      region=subnet_ref.region,
      project=subnet_ref.project)

  secondary_ranges = []
  if args.secondary_range:
    for secondary_range in args.secondary_range:
      for range_name, ip_cidr_range in sorted(six.iteritems(secondary_range)):
        secondary_ranges.append(
            client.messages.SubnetworkSecondaryRange(
                rangeName=range_name, ipCidrRange=ip_cidr_range))

  request.subnetwork.secondaryIpRanges = secondary_ranges
  return client.MakeRequests([(client.apitools_client.subnetworks, 'Insert',
                               request)])


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Create a GA subnet."""

  _include_alpha_logging = False
  # TODO(b/144022508): Remove _include_l7_internal_load_balancing
  _include_l7_internal_load_balancing = True
  _include_aggregate_purpose = False
  _include_private_service_connect = False
  _include_stack_type = False
  _include_ipv6_access_type = False
  _api_version = compute_api.COMPUTE_GA_API_VERSION

  detailed_help = _DetailedHelp()

  @classmethod
  def Args(cls, parser):
    _AddArgs(parser, cls._include_alpha_logging,
             cls._include_l7_internal_load_balancing,
             cls._include_aggregate_purpose,
             cls._include_private_service_connect, cls._include_stack_type,
             cls._include_ipv6_access_type, cls._api_version)

  def Run(self, args):
    """Issues a list of requests necessary for adding a subnetwork."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return _Run(args, holder, self._include_alpha_logging,
                self._include_l7_internal_load_balancing,
                self._include_aggregate_purpose,
                self._include_private_service_connect, self._include_stack_type,
                self._include_ipv6_access_type)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):

  _api_version = compute_api.COMPUTE_BETA_API_VERSION


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):

  _include_alpha_logging = True
  _include_aggregate_purpose = True
  _include_private_service_connect = True
  _include_stack_type = True
  _include_ipv6_access_type = True
  _api_version = compute_api.COMPUTE_ALPHA_API_VERSION
