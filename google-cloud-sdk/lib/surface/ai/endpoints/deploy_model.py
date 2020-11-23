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
"""AI Platform endpoints deploy-model command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from googlecloudsdk.api_lib.ai import operations
from googlecloudsdk.api_lib.ai.endpoints import client
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import endpoint_util
from googlecloudsdk.command_lib.ai import endpoints_util
from googlecloudsdk.command_lib.ai import flags
from googlecloudsdk.command_lib.ai import operations_util
from googlecloudsdk.command_lib.ai import validation
from googlecloudsdk.core import log


def _AddArgs(parser, version):
  flags.AddEndpointResourceArg(parser, 'to deploy a model to')
  flags.GetModelIdArg().AddToParser(parser)
  flags.GetDisplayNameArg('deployed model').AddToParser(parser)
  flags.GetTrafficSplitArg().AddToParser(parser)
  flags.AddPredictionResourcesArgs(parser, version)
  flags.GetEnableAccessLoggingArg().AddToParser(parser)
  flags.GetEnableContainerLoggingArg().AddToParser(parser)
  flags.GetServiceAccountArg().AddToParser(parser)


def _Run(args, version):
  """Deploy a model to an existing AI Platform endpoint."""
  validation.ValidateDisplayName(args.display_name)

  endpoint_ref = args.CONCEPTS.endpoint.Parse()
  args.region = endpoint_ref.AsDict()['locationsId']
  with endpoint_util.AiplatformEndpointOverrides(version, region=args.region):
    endpoints_client = client.EndpointsClient(version=version)
    operation_client = operations.OperationsClient()
    op = endpoints_client.DeployModelBeta(endpoint_ref, args)
    response_msg = operations_util.WaitForOpMaybe(
        operation_client, op, endpoints_util.ParseOperation(op.name))
    if response_msg is not None:
      response = encoding.MessageToPyValue(response_msg)
      if 'deployedModel' in response and 'id' in response['deployedModel']:
        log.status.Print(('Deployed a model to the endpoint {}. '
                          'Id of the deployed model: {}.').format(
                              endpoint_ref.AsDict()['endpointsId'],
                              response['deployedModel']['id']))
    return response_msg


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class DeployModelBeta(base.Command):
  """Deploy a model to an existing AI Platform endpoint."""

  @staticmethod
  def Args(parser):
    _AddArgs(parser, constants.BETA_VERSION)

  def Run(self, args):
    _Run(args, constants.BETA_VERSION)
