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

"""Transcoder API jobs list command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.transcoder import jobs
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.transcoder import resource_args


_FORMAT = """\
table(
    name,
    state,
    failureReason
)
"""


class List(base.ListCommand):
  """List transcoder jobs."""

  detailed_help = {
      'EXAMPLES': """
          To list transcoder jobs in ``us-central1'':

              $ {command} --location=us-central1
              """
  }

  @staticmethod
  def Args(parser):
    resource_args.AddLocationResourceArg(parser)
    parser.display_info.AddFormat(_FORMAT)

  def Run(self, args):
    """List jobs."""
    client = jobs.JobsClient()

    parent_ref = args.CONCEPTS.location.Parse()

    return client.List(parent_ref, page_size=args.page_size)
