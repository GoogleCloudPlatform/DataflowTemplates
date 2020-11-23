# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""Utility methods for the `compute sole-tenancy node-groups` commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.util.apis.arg_utils import ChoiceToEnumName


def ParseNodeTemplate(resources, name, project=None, region=None):
  """Parses a node template resource and returns a resource reference.

  Args:
    resources: Resource registry used to parse the node template.
    name: The name of the node template.
    project: The project the node template is associated with.
    region: The region the node temlpate is associated with.

  Returns:
    The parsed node template resource reference.
  """
  return resources.Parse(
      name, {'project': project, 'region': region},
      collection='compute.nodeTemplates')


def BuildAutoscaling(args, messages):
  """Build NodeGroupAutoscalingPolicy object from args."""

  autoscaling_policy = messages.NodeGroupAutoscalingPolicy(
      mode=(messages.NodeGroupAutoscalingPolicy.ModeValueValuesEnum(
          ChoiceToEnumName(args.autoscaler_mode))
            if args.autoscaler_mode else None),
      minNodes=args.min_nodes if args.IsSpecified('min_nodes') else None,
      maxNodes=args.max_nodes if args.IsSpecified('max_nodes') else None)
  return autoscaling_policy
