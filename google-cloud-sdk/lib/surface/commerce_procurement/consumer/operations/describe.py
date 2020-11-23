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
"""Implementation of gcloud Procurement consumer operations describe command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.commerce_procurement import apis
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.commerce_procurement import resource_args


@base.Hidden
@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Describe(base.DescribeCommand):
  """Returns the Operation object resulting from the Get API."""

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: argparse.ArgumentParser to register arguments with.
    """
    operation_name_group = parser.add_mutually_exclusive_group(required=True)
    resource_args.AddFreeTrialOperationResourceArg(
        operation_name_group, 'Free trial operation to describe.')
    resource_args.AddOrderOperationResourceArg(operation_name_group,
                                               'Order operation to describe.')

  def Run(self, args):
    """Runs the command.

    Args:
      args: The arguments that were provided to this command invocation.

    Returns:
      An Operation message.
    """
    free_trial_operation_ref = args.CONCEPTS.free_trial_operation.Parse()
    order_operation_ref = args.CONCEPTS.order_operation.Parse()
    if free_trial_operation_ref:
      return apis.Operations.GetFreeTrialOperation(
          free_trial_operation_ref.RelativeName())
    elif order_operation_ref:
      return apis.Operations.GetOrderOperation(
          order_operation_ref.RelativeName())
    else:
      raise ValueError('No matching operation spoecified')
