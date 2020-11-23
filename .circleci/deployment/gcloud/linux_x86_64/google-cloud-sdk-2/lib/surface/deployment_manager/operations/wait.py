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

"""operations wait command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.deployment_manager import dm_base
from googlecloudsdk.api_lib.deployment_manager import exceptions
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.deployment_manager import dm_write
from googlecloudsdk.core import log

# Number of seconds (approximately) to wait for each operation to complete.
OPERATION_TIMEOUT = 120 * 60  # 2 hr


@dm_base.UseDmApi(dm_base.DmApiVersion.V2)
class Wait(base.Command, dm_base.DmCommand):
  """Wait for all operations specified to complete before returning.

  Polls until all operations have finished, then prints the resulting operations
  along with any operation errors.
  """

  detailed_help = {
      'EXAMPLES': """\
          To poll until an operation has completed, run:

            $ {command} operation-name

          To poll until several operations have all completed, run:

            $ {command} operation-one operation-two operation-three
          """,
  }

  @staticmethod
  def Args(parser):
    """Args is called by calliope to gather arguments for this command.

    Args:
      parser: An argparse parser that you can use to add arguments that go
          on the command line after this command. Positional arguments are
          allowed.
    """
    parser.add_argument('operation_name', nargs='+', help='Operation name.')

  def Run(self, args):
    """Run 'operations wait'.

    Args:
      args: argparse.Namespace, The arguments that this command was invoked
          with.

    Raises:
      HttpException: An http error response was received while executing api
          request.
    Raises:
      OperationError: Operation finished with error(s) or timed out.
    """
    failed_ops = []
    for operation_name in args.operation_name:
      try:
        dm_write.WaitForOperation(self.client,
                                  self.messages,
                                  operation_name, '', dm_base.GetProject(),
                                  timeout=OPERATION_TIMEOUT)
      except exceptions.OperationError:
        failed_ops.append(operation_name)
    if failed_ops:
      if len(failed_ops) == 1:
        raise exceptions.OperationError(
            'Operation %s failed to complete or has errors.' % failed_ops[0])
      else:
        raise exceptions.OperationError(
            'Some operations failed to complete without errors:\n'
            + '\n'.join(failed_ops))
    else:
      log.status.Print('All operations completed successfully.')
