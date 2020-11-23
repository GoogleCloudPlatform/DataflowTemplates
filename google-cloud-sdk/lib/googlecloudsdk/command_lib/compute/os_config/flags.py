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
"""Flags and helpers for the compute os-config related commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import completers as compute_completers
from googlecloudsdk.command_lib.compute import flags as compute_flags

INSTANCES_ARG_FOR_OS_UPGRADE = compute_flags.ResourceArgument(
    resource_name='instance',
    name='instance_name',
    completer=compute_completers.InstancesCompleter,
    zonal_collection='compute.instances',
    zone_explanation=compute_flags.ZONE_PROPERTY_EXPLANATION,
    plural=False)


def AddPatchDeploymentsCreateFlags(parser, api_version):
  """Adds flags for os-config create command to parser."""
  parser.add_argument(
      'PATCH_DEPLOYMENT_ID',
      type=str,
      help="""\
        Name of the patch deployment to create.

        This name must contain only lowercase letters, numbers, and hyphens,
        start with a letter, end with a number or a letter, be between 1-63
        characters, and unique within the project.""",
  )
  parser.add_argument(
      '--file',
      required=True,
      help="""\
        The JSON or YAML file with the patch deployment to create. For
        information about the patch deployment format, see https://cloud.google.com/compute/docs/osconfig/rest/{api_version}/projects.patchDeployments."""
      .format(api_version=api_version),
  )
