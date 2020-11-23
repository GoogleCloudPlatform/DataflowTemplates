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
"""List operations command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions

from googlecloudsdk.api_lib.container import util
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import properties


class List(base.ListCommand):
  """List operations for container clusters."""

  detailed_help = {
      'DESCRIPTION':
          '{description}',
      'EXAMPLES':
          """\
          To list operations, run:

            $ {command}
          """,
  }

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    # --uri is inherited for all ListCommands but is not implemented here.
    base.URI_FLAG.RemoveFromParser(parser)
    parser.display_info.AddFormat(util.OPERATIONS_FORMAT)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      Some value that we want to have printed later.
    """
    adapter = self.context['api_adapter']
    location_get = self.context['location_get']

    project_id = properties.VALUES.core.project.GetOrFail()
    location = location_get(args, ignore_property=True, required=False)

    try:
      return adapter.ListOperations(project_id, location).operations
    except apitools_exceptions.HttpError as error:
      raise exceptions.HttpException(error, util.HTTP_ERROR_FORMAT)
