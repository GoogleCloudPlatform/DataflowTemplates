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
"""The command group for cloud container clusters."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.container import transforms
from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.container import container_command_util
from googlecloudsdk.command_lib.container import flags
from googlecloudsdk.core import log


class Clusters(base.Group):
  """Deploy and teardown Google Kubernetes Engine clusters.

  The gcloud container clusters command group lets you deploy and teardown
  Google Kubernetes Engine clusters.

  See $ gcloud docker --help for information about deploying docker images
  to clusters.
  """

  @staticmethod
  def Args(parser):
    """Add arguments to the parser.

    Args:
      parser: argparse.ArgumentParser, This is a standard argparser parser with
        which you can register arguments.  See the public argparse documentation
        for its capabilities.
    """
    flags.AddZoneAndRegionFlags(parser)
    parser.display_info.AddTransforms(transforms.GetTransforms())

  def Filter(self, context, args):
    """Modify the context that will be given to this group's commands when run.

    Args:
      context: {str:object}, A set of key-value pairs that can be used for
        common initialization among commands.
      args: argparse.Namespace: The same namespace given to the corresponding
        .Run() invocation.

    Returns:
      The refined command context.
    """
    context['location_get'] = container_command_util.GetZoneOrRegion
    return context
