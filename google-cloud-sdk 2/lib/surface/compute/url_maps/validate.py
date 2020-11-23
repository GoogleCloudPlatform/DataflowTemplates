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
"""Validate URL maps command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.export import util as export_util
from googlecloudsdk.core import properties
from googlecloudsdk.core import yaml_validator
from googlecloudsdk.core.console import console_io


def _DetailedHelp():
  return {
      'brief':
          'Validates URL map configs from your project.',
      'DESCRIPTION':
          """\
        Runs static validation for the UrlMap.
        In particular, the tests of the provided UrlMap will be run.
        Calling this method does NOT create the UrlMap.
        """,
      'EXAMPLES':
          """\
        A URL map can be validated by running:

          $ {command} --source=<path-to-file>
        """
  }


def _GetApiVersion(release_track):
  """Returns the API version based on the release track."""
  if release_track == base.ReleaseTrack.ALPHA:
    return 'alpha'
  elif release_track == base.ReleaseTrack.BETA:
    return 'beta'
  return 'v1'


def _GetSchemaPath(release_track, for_help=False):
  """Returns the resource schema path."""
  return export_util.GetSchemaPath(
      'compute', _GetApiVersion(release_track), 'UrlMap', for_help=for_help)


def _AddSourceFlag(parser, schema_path=None):
  help_text = """Path to the file that contains the URL map config
          for validation. The file must not contain any output-only fields.
          A schema describing the export/import format can be found in: {}.
      """.format(schema_path)
  parser.add_argument(
      '--source', help=textwrap.dedent(help_text), required=True)


def _AddFileFormatFlag(parser):
  help_text = """The format of the file containing the URL map config.
          Currently supports yaml which is also the default.
          FILE_FORMAT must be: yaml."""
  parser.add_argument(
      '--file_format', help=textwrap.dedent(help_text), required=False)


def _AddGlobalFlag(parser):
  parser.add_argument(
      '--global', action='store_true', help='If set, the URL map is global.')


def _AddRegionFlag(parser):
  parser.add_argument('--region', help='Region of the URL map to validate.')


def _AddScopeFlags(parser):
  scope = parser.add_mutually_exclusive_group()
  _AddGlobalFlag(scope)
  _AddRegionFlag(scope)


def _MakeGlobalRequest(client, project, url_map):
  return client.messages.ComputeUrlMapsValidateRequest(
      project=project,
      urlMap=url_map.name,
      urlMapsValidateRequest=client.messages.UrlMapsValidateRequest(
          resource=url_map))


def _MakeRegionalRequest(client, project, region, url_map):
  return client.messages.ComputeRegionUrlMapsValidateRequest(
      project=project,
      region=region,
      urlMap=url_map.name,
      regionUrlMapsValidateRequest=client.messages.RegionUrlMapsValidateRequest(
          resource=url_map))


def _SendGlobalRequest(client, project, url_map):
  return client.apitools_client.urlMaps.Validate(
      _MakeGlobalRequest(client, project, url_map))


def _SendRegionalRequest(client, project, region, url_map):
  return client.apitools_client.regionUrlMaps.Validate(
      _MakeRegionalRequest(client, project, region, url_map))


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Validate(base.Command):
  """Validates URL map configs from your project."""

  detailed_help = _DetailedHelp()

  @classmethod
  def Args(cls, parser):
    _AddSourceFlag(parser, _GetSchemaPath(cls.ReleaseTrack(), for_help=True))
    _AddFileFormatFlag(parser)
    _AddScopeFlags(parser)

  def Run(self, args):
    """Runs the command.

    Args:
      args: argparse.Namespace, An object that contains the values for the
        arguments specified in the .Args() method.

    Returns:
      A response object returned by rpc call Validate.
    """
    project = properties.VALUES.core.project.GetOrFail()
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    # Import UrlMap to be verified
    data = console_io.ReadFromFileOrStdin(args.source, binary=False)
    try:
      url_map = export_util.Import(
          message_type=client.messages.UrlMap,
          stream=data,
          schema_path=_GetSchemaPath(self.ReleaseTrack()))
    except yaml_validator.ValidationError as e:
      raise exceptions.ToolException(str(e))

    # Send UrlMap.validate request
    if args.region is not None:
      return _SendRegionalRequest(client, project, args.region, url_map)
    return _SendGlobalRequest(client, project, url_map)
