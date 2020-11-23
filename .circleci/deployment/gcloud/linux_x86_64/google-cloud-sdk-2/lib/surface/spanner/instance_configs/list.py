# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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
"""Command for spanner instance configs list."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.spanner import instance_configs
from googlecloudsdk.calliope import base


class List(base.ListCommand):
  """List the available Cloud Spanner instance configs."""

  detailed_help = {
      'EXAMPLES':
          textwrap.dedent("""\
        To list the Cloud Spanner instance configs that are availble for this
        project, run:

          $ {command}
        """),
  }

  @staticmethod
  def Args(parser):
    parser.display_info.AddFormat("""
          table(
            name.basename(),
            displayName
          )
        """)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      Some value that we want to have printed later.
    """
    return instance_configs.List()
