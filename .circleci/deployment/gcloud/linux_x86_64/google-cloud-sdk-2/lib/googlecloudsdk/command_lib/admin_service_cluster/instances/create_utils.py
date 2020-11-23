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
"""Instance creation request modifier."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions

BUNDLE_ANTHOS = 'Anthos'
BUNDLE_YAKIMA = 'Yakima'


def Messages(api_version):
  return apis.GetMessagesModule('krmapihosting', api_version)


def AddExtraArgs():
  """Adds additional args that can't be easily represented in create.yaml."""

  # TODO(b/169528795): this is a list even though it takes exactly one element
  # because the API is expected to accept multiple elements in later versions.
  bundles = base.Argument(
      '--bundles',
      dest='bundles',
      required=True,
      type=arg_parsers.ArgList(),
      metavar='BUNDLE',
      help="""
Bundles that should be enabled. Exactly one bundle must be enabled.
BUNDLE must be one of: %s, %s.
""" % (BUNDLE_ANTHOS, BUNDLE_YAKIMA))
  return [bundles]


def CreateUpdateRequest(ref, args):
  """Returns an updated request formatted to the right URI endpoint."""
  messages = Messages(ref.GetCollectionInfo().api_version)

  # krmapihosting create endpoint uses a different uri from the one generated,
  # we will need to construct it manually
  custom_uri = 'projects/{project_id}/locations/{location}'.format(
      project_id=ref.projectsId, location=args.location)

  # Default values if flags not specified
  git_secret_type = args.git_secret_type
  # if git_sync_repo is provided, but not secret_type, then "ssh" as secret type
  if args.git_sync_repo is not None and args.git_secret_type is None:
    git_secret_type = 'ssh'

  # Default master ipv4 cidr block address if not provided
  master_ipv4_cidr_block = '172.16.0.128/28'
  if args.master_ipv4_cidr_block is not None:
    master_ipv4_cidr_block = args.master_ipv4_cidr_block

  # The current version of the API only supports exactly one bundle.
  if len(args.bundles) != 1:
    raise exceptions.InvalidArgumentException(
        '--bundles', 'Exactly one bundle must be enabled')

  bundles_config = None
  if args.bundles[0].lower() == BUNDLE_ANTHOS.lower():
    bundles_config = messages.BundlesConfig(
        anthosSseConfig=messages.AnthosSseConfig(enabled=True))
  elif args.bundles[0].lower() == BUNDLE_YAKIMA.lower():
    bundles_config = messages.BundlesConfig(
        yakimaConfig=messages.YakimaConfig(enabled=True))
  else:
    raise exceptions.InvalidArgumentException(
        '--bundles',
        'BUNDLE must be one of: %s, %s' % (BUNDLE_ANTHOS, BUNDLE_YAKIMA))

  krm_api_host = messages.KrmApiHost(
      masterIpv4CidrBlock=master_ipv4_cidr_block,
      gitSecretType=git_secret_type,
      gitEndpoint=args.git_sync_repo,
      gitBranch=args.git_branch,
      gitPolicyDir=args.git_policy_dir,
      bundlesConfig=bundles_config)

  request = (
      messages.KrmapihostingProjectsLocationsKrmApiHostsCreateRequest(
          parent=custom_uri,
          krmApiHostId=ref.krmApiHostsId,
          krmApiHost=krm_api_host))

  return request
