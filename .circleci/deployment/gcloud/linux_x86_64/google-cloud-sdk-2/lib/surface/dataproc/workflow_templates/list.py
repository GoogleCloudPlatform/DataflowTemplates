# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""List workflow template command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager

from googlecloudsdk.api_lib.dataproc import constants
from googlecloudsdk.api_lib.dataproc import dataproc as dp
from googlecloudsdk.api_lib.dataproc import util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.dataproc import flags

DETAILED_HELP = {
    'EXAMPLES':
        """\
      To list all workflow-templates from region 'us-central1' run:

        $ {command} --region=us-central1
      """,
}


class List(base.ListCommand):
  """List workflow templates."""

  detailed_help = DETAILED_HELP

  @staticmethod
  def Args(parser):
    # TODO(b/65634121): Implement URI listing for dataproc
    base.URI_FLAG.RemoveFromParser(parser)
    base.PAGE_SIZE_FLAG.SetDefault(parser, constants.DEFAULT_PAGE_SIZE)
    flags.AddRegionFlag(parser)
    parser.display_info.AddFormat("""
          table(
            id:label=ID,
            jobs.len():label=JOBS,
            updateTime:label=UPDATE_TIME,
            version:label=VERSION
          )
    """)

  def Run(self, args):
    dataproc = dp.Dataproc(self.ReleaseTrack())
    messages = dataproc.messages

    region_ref = util.ParseRegion(dataproc)

    request = messages.DataprocProjectsRegionsWorkflowTemplatesListRequest(
        parent=region_ref.RelativeName())

    return list_pager.YieldFromList(
        dataproc.client.projects_regions_workflowTemplates,
        request,
        limit=args.limit,
        field='templates',
        batch_size=args.page_size,
        batch_size_attribute='pageSize')
