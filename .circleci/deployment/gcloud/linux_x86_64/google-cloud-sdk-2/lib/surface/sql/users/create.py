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
"""Creates a user in a given instance.

Creates a user in a given instance with specified username, host, and password.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.sql import api_util
from googlecloudsdk.api_lib.sql import operations
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.sql import flags
from googlecloudsdk.command_lib.sql import users
from googlecloudsdk.core import log
from googlecloudsdk.core import properties


def AddBaseArgs(parser):
  flags.AddInstance(parser)
  flags.AddUsername(parser)
  flags.AddHost(parser)
  flags.AddPassword(parser)


def AddBetaArgs(parser):
  flags.AddType(parser)


def AddAlphaArgs(parser):
  AddBetaArgs(parser)


def RunBaseCreateCommand(args, release_track):
  """Creates a user in a given instance.

  Args:
    args: argparse.Namespace, The arguments that this command was invoked with.
    release_track: base.ReleaseTrack, the release track that this was run under.

  Returns:
    SQL user resource iterator.
  """
  client = api_util.SqlClient(api_util.API_VERSION_DEFAULT)
  sql_client = client.sql_client
  sql_messages = client.sql_messages

  instance_ref = client.resource_parser.Parse(
      args.instance,
      params={'project': properties.VALUES.core.project.GetOrFail},
      collection='sql.instances')
  operation_ref = None

  if release_track in [base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA]:
    user_type = users.ParseUserType(sql_messages, args)
    new_user = sql_messages.User(
        kind='sql#user',
        project=instance_ref.project,
        instance=args.instance,
        name=args.username,
        host=args.host,
        password=args.password,
        type=user_type)
  else:
    new_user = sql_messages.User(
        kind='sql#user',
        project=instance_ref.project,
        instance=args.instance,
        name=args.username,
        host=args.host,
        password=args.password)

  result_operation = sql_client.users.Insert(new_user)
  operation_ref = client.resource_parser.Create(
      'sql.operations',
      operation=result_operation.name,
      project=instance_ref.project)
  if args.async_:
    result = sql_client.operations.Get(
        sql_messages.SqlOperationsGetRequest(
            project=operation_ref.project, operation=operation_ref.operation))
  else:
    operations.OperationsV1Beta4.WaitForOperation(sql_client, operation_ref,
                                                  'Creating Cloud SQL user')
    result = new_user
    result.kind = None

  log.CreatedResource(args.username, kind='user', is_async=args.async_)

  return result


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Creates a user in a given instance.

  Creates a user in a given instance with specified username, host, and
  password.
  """

  @staticmethod
  def Args(parser):
    AddBaseArgs(parser)
    base.ASYNC_FLAG.AddToParser(parser)
    parser.display_info.AddCacheUpdater(flags.UserCompleter)

  def Run(self, args):
    return RunBaseCreateCommand(args, self.ReleaseTrack())


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):
  """Creates a user in a given instance.

  Creates a user in a given instance with specified username, host,
  type, and password.
  """

  @staticmethod
  def Args(parser):
    AddBaseArgs(parser)
    AddBetaArgs(parser)
    base.ASYNC_FLAG.AddToParser(parser)
    parser.display_info.AddCacheUpdater(flags.UserCompleter)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):
  """Creates a user in a given instance.

  Creates a user in a given instance with specified username, host,
  type, and password.
  """

  @staticmethod
  def Args(parser):
    AddBaseArgs(parser)
    AddAlphaArgs(parser)
    base.ASYNC_FLAG.AddToParser(parser)
    parser.display_info.AddCacheUpdater(flags.UserCompleter)
