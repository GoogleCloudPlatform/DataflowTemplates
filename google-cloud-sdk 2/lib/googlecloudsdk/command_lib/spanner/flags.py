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
"""Provides common arguments for the Spanner command surface."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


from argcomplete.completers import FilesCompleter
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.spanner import ddl_parser
from googlecloudsdk.command_lib.util import completers
from googlecloudsdk.core.util import files


class BackupCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(BackupCompleter, self).__init__(
        collection='spanner.projects.instances.backups',
        list_command='spanner backups list --uri',
        flags=['instance'],
        **kwargs)


class DatabaseCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(DatabaseCompleter, self).__init__(
        collection='spanner.projects.instances.databases',
        list_command='spanner databases list --uri',
        flags=['instance'],
        **kwargs)


class DatabaseOperationCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(DatabaseOperationCompleter, self).__init__(
        collection='spanner.projects.instances.databases.operations',
        list_command='spanner operations list --uri',
        flags=['instance'],
        **kwargs)


class InstanceCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(InstanceCompleter, self).__init__(
        collection='spanner.projects.instances',
        list_command='spanner instances list --uri',
        **kwargs)


class InstanceConfigCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(InstanceConfigCompleter, self).__init__(
        collection='spanner.projects.instanceConfigs',
        list_command='spanner instance-configs list --uri',
        **kwargs)


class OperationCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(OperationCompleter, self).__init__(
        collection='spanner.projects.instances.operations',
        list_command='spanner operations list --uri',
        flags=['instance'],
        **kwargs)


class DatabaseSessionCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(DatabaseSessionCompleter, self).__init__(
        collection='spanner.projects.instances.databases.sessions',
        list_command='spanner databases sessions list --uri',
        flags=['database', 'instance'],
        **kwargs)


def Database(positional=True,
             required=True,
             text='Cloud Spanner database ID.'):
  if positional:
    return base.Argument(
        'database',
        completer=DatabaseCompleter,
        help=text)
  else:
    return base.Argument(
        '--database',
        required=required,
        completer=DatabaseCompleter,
        help=text)


def Backup(positional=True, required=True, text='Cloud Spanner backup ID.'):
  if positional:
    return base.Argument('backup', completer=BackupCompleter, help=text)
  else:
    return base.Argument(
        '--backup', required=required, completer=BackupCompleter, help=text)


def Ddl(help_text=''):
  return base.Argument(
      '--ddl',
      action='append',
      required=False,
      help=help_text,
  )


def DdlFile(help_text):
  return base.Argument(
      '--ddl-file',
      required=False,
      completer=FilesCompleter,
      help=help_text,
  )


def GetDDLsFromArgs(args):
  if args.ddl_file:
    return [files.ReadFileContents(args.ddl_file)]
  return args.ddl or []


def SplitDdlIntoStatements(args):
  """Break DDL statements on semicolon while preserving string literals."""
  ddls = GetDDLsFromArgs(args)
  statements = []
  for x in ddls:
    statements.extend(ddl_parser.PreprocessDDLWithParser(x))
  return statements


def Config(required=True):
  return base.Argument(
      '--config',
      completer=InstanceConfigCompleter,
      required=required,
      help='Instance configuration defines the geographic placement and '
      'replication of the databases in that instance. Available '
      'configurations can be found by running '
      '"gcloud spanner instance-configs list"')


def Description(required=True):
  return base.Argument(
      '--description',
      required=required,
      help='Description of the instance.')


def Instance(positional=True, text='Cloud Spanner instance ID.'):
  if positional:
    return base.Argument(
        'instance',
        completer=InstanceCompleter,
        help=text)
  else:
    return base.Argument(
        '--instance',
        required=True,
        completer=InstanceCompleter,
        help=text)


def Nodes(required=True):
  return base.Argument(
      '--nodes',
      required=required,
      type=int,
      help='Number of nodes for the instance.')


def OperationId(database=False):
  return base.Argument(
      'operation',
      metavar='OPERATION-ID',
      completer=DatabaseOperationCompleter if database else OperationCompleter,
      help='ID of the operation')


def Session(positional=True, required=True, text='Cloud Spanner session ID'):
  if positional:
    return base.Argument(
        'session', completer=DatabaseSessionCompleter, help=text)

  else:
    return base.Argument(
        '--session',
        required=required,
        completer=DatabaseSessionCompleter,
        help=text)
