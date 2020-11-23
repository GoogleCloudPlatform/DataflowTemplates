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
"""Command for deleting health checks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import health_checks_utils
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute.health_checks import flags


def _DetailedHelp():
  return {
      'brief':
          'Delete health checks.',
      'DESCRIPTION':
          """\
      *{command}* deletes one or more Compute Engine
      health checks.
      """,
  }


def _Args(parser, include_l7_internal_load_balancing):
  health_check_arg = flags.HealthCheckArgument(
      '',
      plural=True,
      include_l7_internal_load_balancing=include_l7_internal_load_balancing)
  health_check_arg.AddArgument(parser, operation_type='delete')
  parser.display_info.AddCacheUpdater(completers.HealthChecksCompleterAlpha
                                      if include_l7_internal_load_balancing
                                      else completers.HealthChecksCompleter)


def _Run(holder, args, include_l7_internal_load_balancing):
  """Issues the request necessary for deleting the health check."""
  client = holder.client

  health_check_arg = flags.HealthCheckArgument(
      '',
      plural=True,
      include_l7_internal_load_balancing=include_l7_internal_load_balancing)
  health_check_refs = health_check_arg.ResolveAsResource(
      args,
      holder.resources,
      default_scope=compute_scope.ScopeEnum.GLOBAL,
      scope_lister=compute_flags.GetDefaultScopeLister(client))

  utils.PromptForDeletion(health_check_refs)

  requests = []

  for health_check_ref in health_check_refs:
    if health_checks_utils.IsRegionalHealthCheckRef(health_check_ref):
      requests.append((client.apitools_client.regionHealthChecks, 'Delete',
                       client.messages.ComputeRegionHealthChecksDeleteRequest(
                           **health_check_ref.AsDict())))
    else:
      requests.append((client.apitools_client.healthChecks, 'Delete',
                       client.messages.ComputeHealthChecksDeleteRequest(
                           **health_check_ref.AsDict())))

  return client.MakeRequests(requests)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class Delete(base.DeleteCommand):
  """Delete Ga/Beta health checks."""

  _include_l7_internal_load_balancing = True
  detailed_help = _DetailedHelp()

  @classmethod
  def Args(cls, parser):
    _Args(parser, cls._include_l7_internal_load_balancing)

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return _Run(holder, args, self._include_l7_internal_load_balancing)
