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
"""`gcloud tasks queues describe` command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.tasks import GetApiAdapter
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.tasks import flags
from googlecloudsdk.command_lib.tasks import list_formats
from googlecloudsdk.command_lib.tasks import parsers


_DEFAULT_PAGE_SIZE = 25


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.GA)
class List(base.ListCommand):
  """List tasks."""
  detailed_help = {
      'DESCRIPTION': """\
          {description}
          """,
      'EXAMPLES': """\
          To list tasks in a queue:

              $ {command} --queue=my-queue
         """,
  }

  @staticmethod
  def Args(parser):
    list_formats.AddListTasksFormats(parser)
    flags.AddQueueResourceFlag(parser, plural_tasks=True)
    flags.AddLocationFlag(parser)
    base.PAGE_SIZE_FLAG.SetDefault(parser, _DEFAULT_PAGE_SIZE)

  def Run(self, args):
    tasks_client = GetApiAdapter(self.ReleaseTrack()).tasks
    queue_ref = parsers.ParseQueue(args.queue, args.location)
    return tasks_client.List(queue_ref, args.limit, args.page_size)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class AlphaList(List):
  """List tasks."""
  detailed_help = {
      'DESCRIPTION': """\
          {description}
          """,
      'EXAMPLES': """\
          To list tasks in a queue:

              $ {command} --queue=my-queue
         """,
  }

  @staticmethod
  def Args(parser):
    list_formats.AddListTasksFormats(parser, is_alpha=True)
    flags.AddQueueResourceFlag(parser, plural_tasks=True)
    flags.AddLocationFlag(parser)
    base.PAGE_SIZE_FLAG.SetDefault(parser, _DEFAULT_PAGE_SIZE)
