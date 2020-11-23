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
"""Remove IAM Policy Binding."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.ml_engine import models
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iam import iam_util
from googlecloudsdk.command_lib.ml_engine import endpoint_util
from googlecloudsdk.command_lib.ml_engine import flags
from googlecloudsdk.command_lib.ml_engine import models_util


def _GetRemoveIamPolicyBindingArgs(parser, add_condition=False):
  iam_util.AddArgsForRemoveIamPolicyBinding(parser, add_condition=add_condition)
  flags.GetModelResourceArg(
      required=True,
      verb='for which to remove IAM policy binding from').AddToParser(parser)
  flags.GetRegionArg().AddToParser(parser)
  base.URI_FLAG.RemoveFromParser(parser)


@base.ReleaseTracks(base.ReleaseTrack.GA)
class RemoveIamPolicyBinding(base.Command):
  """Removes IAM policy binding from an AI Platform Model resource.

  Removes a policy binding from an AI Platform Model. One
  binding consists of a member, a role and an optional condition.
  See $ {parent_command} get-iam-policy for examples of how to
  specify a model resource.
  """

  description = 'remove IAM policy binding from an AI Platform model'
  detailed_help = iam_util.GetDetailedHelpForRemoveIamPolicyBinding(
      'model', 'my_model', role='roles/ml.admin', condition=False)

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    _GetRemoveIamPolicyBindingArgs(parser, add_condition=False)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      The specified function with its description and configured filter.
    """
    with endpoint_util.MlEndpointOverrides(region=args.region):
      client = models.ModelsClient()
      return models_util.RemoveIamPolicyBinding(client, args.model, args.member,
                                                args.role)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class RemoveIamPolicyBindingBeta(base.Command):
  """Removes IAM policy binding from an AI Platform Model resource.

  Removes a policy binding from an AI Platform Model. One
  binding consists of a member, a role and an optional condition.
  See $ {parent_command} get-iam-policy for examples of how to
  specify a model resource.
  """

  description = 'remove IAM policy binding from an AI Platform model'
  detailed_help = iam_util.GetDetailedHelpForRemoveIamPolicyBinding(
      'model', 'my_model', role='roles/ml.admin', condition=False)

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    _GetRemoveIamPolicyBindingArgs(parser, add_condition=False)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      The specified function with its description and configured filter.
    """
    with endpoint_util.MlEndpointOverrides(region=args.region):
      client = models.ModelsClient()
      return models_util.RemoveIamPolicyBinding(client, args.model, args.member,
                                                args.role)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class RemoveIamPolicyBindingAlpha(base.Command):
  r"""Removes IAM policy binding from an AI Platform Model resource.

  Remove an IAM policy binding from the IAM policy of a ML model. One binding
  consists of a member, a role, and an optional condition.
  See $ {parent_command} get-iam-policy for examples of how to
  specify a model resource.
  """

  description = 'remove IAM policy binding from an AI Platform model'
  detailed_help = iam_util.GetDetailedHelpForRemoveIamPolicyBinding(
      'model', 'my_model', role='roles/ml.admin', condition=False)

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    _GetRemoveIamPolicyBindingArgs(parser, add_condition=True)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      The specified function with its description and configured filter.
    """
    with endpoint_util.MlEndpointOverrides(region=args.region):
      condition = iam_util.ValidateAndExtractCondition(args)
      iam_util.ValidateMutexConditionAndPrimitiveRoles(condition, args.role)
      return models_util.RemoveIamPolicyBindingWithCondition(
          models.ModelsClient(), args.model, args.member, args.role, condition)
