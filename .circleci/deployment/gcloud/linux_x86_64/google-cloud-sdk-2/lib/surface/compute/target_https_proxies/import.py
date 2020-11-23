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
"""Import target HTTPS Proxies command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions
from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute.target_https_proxies import flags
from googlecloudsdk.command_lib.compute.target_https_proxies import target_https_proxies_utils
from googlecloudsdk.command_lib.export import util as export_util
from googlecloudsdk.core import yaml_validator
from googlecloudsdk.core.console import console_io


def _DetailedHelp():
  return {
      'brief':
          'Import a target HTTPS proxy.',
      'DESCRIPTION':
          """\
          Imports a target HTTPS proxy's configuration from a file.
          """,
      'EXAMPLES':
          """\
          A target HTTPS proxy can be imported by running:

            $ {command} NAME --source=<path-to-file>
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
      'compute',
      _GetApiVersion(release_track),
      'TargetHttpsProxy',
      for_help=for_help)


def _SendInsertRequest(client, target_https_proxy_ref, target_https_proxy):
  """Sends Target HTTPS Proxy insert request."""
  if target_https_proxy_ref.Collection() == 'compute.regionTargetHttpsProxies':
    return client.apitools_client.regionTargetHttpsProxies.Insert(
        client.messages.ComputeRegionTargetHttpsProxiesInsertRequest(
            project=target_https_proxy_ref.project,
            region=target_https_proxy_ref.region,
            targetHttpsProxy=target_https_proxy))

  return client.apitools_client.targetHttpsProxies.Insert(
      client.messages.ComputeTargetHttpsProxiesInsertRequest(
          project=target_https_proxy_ref.project,
          targetHttpsProxy=target_https_proxy))


def _Run(args, holder, target_https_proxy_arg, release_track):
  """Issues requests necessary to import target HTTPS proxies."""
  client = holder.client

  target_https_proxy_ref = target_https_proxy_arg.ResolveAsResource(
      args,
      holder.resources,
      default_scope=compute_scope.ScopeEnum.GLOBAL,
      scope_lister=compute_flags.GetDefaultScopeLister(client))

  data = console_io.ReadFromFileOrStdin(args.source or '-', binary=False)

  try:
    target_https_proxy = export_util.Import(
        message_type=client.messages.TargetHttpsProxy,
        stream=data,
        schema_path=_GetSchemaPath(release_track))
  except yaml_validator.ValidationError as e:
    raise exceptions.ToolException(str(e))

  # Get existing target HTTPS proxy.
  try:
    target_https_proxies_utils.SendGetRequest(client, target_https_proxy_ref)
  except apitools_exceptions.HttpError as error:
    if error.status_code != 404:
      raise error
    # Target HTTPS proxy does not exist, create a new one.
    return _SendInsertRequest(client, target_https_proxy_ref,
                              target_https_proxy)

  console_message = ('Target HTTPS Proxy [{0}] cannot be updated'.format(
      target_https_proxy_ref.Name()))
  raise NotImplementedError(console_message)


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.ALPHA)
class Import(base.UpdateCommand):
  """Import a target HTTPS Proxy."""

  detailed_help = _DetailedHelp()
  TARGET_HTTPS_PROXY_ARG = None

  @classmethod
  def Args(cls, parser):
    cls.TARGET_HTTPS_PROXY_ARG = flags.TargetHttpsProxyArgument(
        include_l7_internal_load_balancing=True)
    cls.TARGET_HTTPS_PROXY_ARG.AddArgument(parser, operation_type='import')
    export_util.AddImportFlags(
        parser, _GetSchemaPath(cls.ReleaseTrack(), for_help=True))

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    return _Run(args, holder, self.TARGET_HTTPS_PROXY_ARG, self.ReleaseTrack())
