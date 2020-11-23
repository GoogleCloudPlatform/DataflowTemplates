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

"""List command for gcloud debug targets command group."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.debug import debug
from googlecloudsdk.calliope import base
from googlecloudsdk.core import properties

DETAILED_HELP = {
    'brief':
        'List the debug targets (debuggees) registered with the Cloud Debugger.',
    'DESCRIPTION':
        """\
        *{command}* is used to display a list of the debug targets (debuggees)
        registered with the Cloud Debugger. By default only the active targets
        are shown, the --include-inactive can be provided to also include the
        targets that are no longer active.
    """,
    'EXAMPLES':
        """\
        To list the active debug targets (debuggees) registered with the Cloud
        Debugger, run:

          $ {command}

        To list all (both active and inactive) debug targets (debuggees)
        registered with the Cloud Debugger, run:

          $ {command} --include-inactive
    """
}


class List(base.ListCommand):
  """List the debug targets (debuggees) registered with the Cloud Debugger."""

  detailed_help = DETAILED_HELP

  @staticmethod
  def Args(parser):
    parser.add_argument(
        '--include-inactive', action='store_true', default=False,
        help="""\
            If set, include targets which are no longer active.
        """)
    parser.display_info.AddFormat("""
          table(
            name,
            target_id:label=ID,
            description
          )
    """)

  def Run(self, args):
    """Run the list command."""
    project_id = properties.VALUES.core.project.Get(required=True)
    debugger = debug.Debugger(project_id)
    # Include stale debuggees whenever including inactive debuggees.
    # It is likely not important to expose the ability to display stale versions
    # without displaying all inactive versions. If users request the feature,
    # we should add a second flag to allow including stale versions that are
    # not inactive.
    return debugger.ListDebuggees(include_inactive=args.include_inactive,
                                  include_stale=args.include_inactive)
