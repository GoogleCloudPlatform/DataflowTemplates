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
"""Command for creating HTTP2 health checks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import health_checks_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute.health_checks import flags


def _DetailedHelp():
  return {
      'brief':
          'Create a HTTP2 health check to monitor load balanced instances',
      'DESCRIPTION':
          """\
          *{command}* is used to create a non-legacy health check using the
          HTTP/2 protocol. You can use this health check for Google Cloud
          load balancers or for managed instance group autohealing.
          For more information, see the health checks overview at:
          [](https://cloud.google.com/load-balancing/docs/health-check-concepts)
          """,
      'EXAMPLES':
          """\
          To create a HTTP2 health check with default options, run:

            $ {command} my-health-check-name
          """,
  }


def _Args(parser, include_l7_internal_load_balancing, include_log_config):
  """Set up arguments to create an HTTP2 HealthCheck."""
  parser.display_info.AddFormat(flags.DEFAULT_LIST_FORMAT)
  flags.HealthCheckArgument(
      'HTTP2',
      include_l7_internal_load_balancing=include_l7_internal_load_balancing
  ).AddArgument(
      parser, operation_type='create')
  health_checks_utils.AddHttpRelatedCreationArgs(parser)
  health_checks_utils.AddHttpRelatedResponseArg(parser)
  health_checks_utils.AddProtocolAgnosticCreationArgs(parser, 'HTTP2')
  if include_log_config:
    health_checks_utils.AddHealthCheckLoggingRelatedArgs(parser)
  parser.display_info.AddCacheUpdater(
      completers.HealthChecksCompleterAlpha if
      include_l7_internal_load_balancing else completers.HealthChecksCompleter)


def _Run(args, holder, include_l7_internal_load_balancing, include_log_config):
  """Issues the request necessary for adding the health check."""
  client = holder.client
  messages = client.messages

  health_check_ref = flags.HealthCheckArgument(
      'HTTP2',
      include_l7_internal_load_balancing=include_l7_internal_load_balancing
  ).ResolveAsResource(
      args, holder.resources, default_scope=compute_scope.ScopeEnum.GLOBAL)
  proxy_header = messages.HTTP2HealthCheck.ProxyHeaderValueValuesEnum(
      args.proxy_header)
  http2_health_check = messages.HTTP2HealthCheck(
      host=args.host,
      port=args.port,
      portName=args.port_name,
      requestPath=args.request_path,
      proxyHeader=proxy_header,
      response=args.response)

  health_checks_utils.ValidateAndAddPortSpecificationToHealthCheck(
      args, http2_health_check)

  if health_checks_utils.IsRegionalHealthCheckRef(health_check_ref):
    request = messages.ComputeRegionHealthChecksInsertRequest(
        healthCheck=messages.HealthCheck(
            name=health_check_ref.Name(),
            description=args.description,
            type=messages.HealthCheck.TypeValueValuesEnum.HTTP2,
            http2HealthCheck=http2_health_check,
            checkIntervalSec=args.check_interval,
            timeoutSec=args.timeout,
            healthyThreshold=args.healthy_threshold,
            unhealthyThreshold=args.unhealthy_threshold,
        ),
        project=health_check_ref.project,
        region=health_check_ref.region)
    collection = client.apitools_client.regionHealthChecks
  else:
    request = messages.ComputeHealthChecksInsertRequest(
        healthCheck=messages.HealthCheck(
            name=health_check_ref.Name(),
            description=args.description,
            type=messages.HealthCheck.TypeValueValuesEnum.HTTP2,
            http2HealthCheck=http2_health_check,
            checkIntervalSec=args.check_interval,
            timeoutSec=args.timeout,
            healthyThreshold=args.healthy_threshold,
            unhealthyThreshold=args.unhealthy_threshold),
        project=health_check_ref.project)
    collection = client.apitools_client.healthChecks

  if include_log_config:
    request.healthCheck.logConfig = health_checks_utils.CreateLogConfig(
        client, args)

  return client.MakeRequests([(collection, 'Insert', request)])


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Create a HTTP2 health check."""

  _include_l7_internal_load_balancing = True
  _include_log_config = False
  detailed_help = _DetailedHelp()

  @classmethod
  def Args(cls, parser):
    _Args(parser, cls._include_l7_internal_load_balancing,
          cls._include_log_config)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return _Run(args, holder, self._include_l7_internal_load_balancing,
                self._include_log_config)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):

  _include_log_config = True


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):

  pass
