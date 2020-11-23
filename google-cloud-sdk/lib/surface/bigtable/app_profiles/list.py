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
"""bigtable app profiles list command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.bigtable import app_profiles
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.bigtable import arguments


class ListAppProfiles(base.ListCommand):
  """List Bigtable app profiles."""

  detailed_help = {
      'EXAMPLES':
          textwrap.dedent("""\
          To list all app profiles for an instance, run:

            $ {command} --instance=my-instance-id

          """),
  }

  @staticmethod
  def Args(parser):
    arguments.AddInstanceResourceArg(parser, 'to list app profiles for')
    # ROUTING is a oneof SingleClusterRouting, MultiClusterRoutingUseAny.
    # Combine into a single ROUTING column in the table.
    parser.display_info.AddFormat("""
          table(
            name.basename():sort=1,
            description:wrap,
            singleClusterRouting.clusterId.yesno(no="MULTI_CLUSTER_USE_ANY"):label=ROUTING,
            singleClusterRouting.allowTransactionalWrites.yesno("Yes"):label=TRANSACTIONAL_WRITES
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
    instance_ref = args.CONCEPTS.instance.Parse()
    return app_profiles.List(instance_ref)
