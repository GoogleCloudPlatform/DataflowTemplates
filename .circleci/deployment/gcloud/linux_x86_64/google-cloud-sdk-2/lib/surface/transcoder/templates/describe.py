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

"""Transcoder API job templates describe command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.transcoder import templates
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.transcoder import resource_args


class Describe(base.DescribeCommand):
  """Describe transcoder job templates."""

  detailed_help = {
      'EXAMPLES': """
          To describe a transcoder job template:

              $ {command} TEMPLATE_ID --location=us-central1
              """
  }

  @staticmethod
  def Args(parser):
    resource_args.AddTemplateResourceArg(parser)

  def Run(self, args):
    """Describe a job template."""
    client = templates.TemplatesClient()

    template_ref = args.CONCEPTS.template_id.Parse()

    return client.Get(template_ref)
