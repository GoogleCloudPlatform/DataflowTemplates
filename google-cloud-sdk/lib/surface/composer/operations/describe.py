# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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
"""Command to show metadata for an operation."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.composer import operations_util as operations_api_util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.composer import resource_args


DETAILED_HELP = {
    'EXAMPLES':
        """\
          To get details for the operation ``operation-1'', run:

            $ {command} operation-1
        """
}


class Describe(base.DescribeCommand):
  """Get details about an asynchronous operation."""

  detailed_help = DETAILED_HELP

  @staticmethod
  def Args(parser):
    resource_args.AddOperationResourceArg(parser, 'to describe')

  def Run(self, args):
    operation_ref = args.CONCEPTS.operation.Parse()
    return operations_api_util.Get(
        operation_ref, release_track=self.ReleaseTrack())
