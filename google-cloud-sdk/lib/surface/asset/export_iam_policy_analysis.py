# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Command to export IAM policy analysis in the specified root asset."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.asset import client_util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.asset import flags
from googlecloudsdk.command_lib.asset import utils as asset_utils
from googlecloudsdk.core import log

OPERATION_DESCRIBE_COMMAND = 'gcloud asset operations describe'


def AddDestinationArgs(parser):
  destination_group = parser.add_group(
      mutex=True,
      required=True,
      help='The destination path for exporting IAM policy analysis.')
  AddOutputPathArgs(destination_group)


def AddOutputPathArgs(parser):
  parser.add_argument(
      '--output-path',
      metavar='OUTPUT_PATH',
      required=True,
      type=arg_parsers.RegexpValidator(
          r'^gs://.*',
          '--output-path must be a Google Cloud Storage URI starting with '
          '"gs://". For example, "gs://bucket_name/object_name"'),
      help='Google Cloud Storage URI where the results will go. '
      'URI must start with "gs://". For example, "gs://bucket_name/object_name"'
  )


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class ExportIamPolicyAnalysisBeta(base.Command):
  """Export IAM policy analysis that match a request to Google Cloud Storage."""

  detailed_help = {
      'DESCRIPTION':
          """\
      Export IAM policy analysis that matches a request to Google Cloud Storage.
      """,
      'EXAMPLES':
          """\
          To find out which users have been granted the
          iam.serviceAccounts.actAs permission on a service account, run:

            $ {command} --organization=YOUR_ORG_ID --full-resource-name=YOUR_SERVICE_ACCOUNT_FULL_RESOURCE_NAME --permissions='iam.serviceAccounts.actAs' --output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

          To find out which resources a user can access, run:

            $ {command} --organization=YOUR_ORG_ID --identity='user:u1@foo.com' --output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

          To find out which roles or permissions a user has been granted on a
          project, run:

            $ {command} --organization=YOUR_ORG_ID --full-resource-name=YOUR_PROJECT_FULL_RESOURCE_NAME --identity='user:u1@foo.com' --output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

          To find out which users have been granted the
          iam.serviceAccounts.actAs permission on any applicable resources, run:

            $ {command} --organization=YOUR_ORG_ID --permissions='iam.serviceAccounts.actAs' --output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

      """
  }

  @staticmethod
  def Args(parser):
    flags.AddAnalyzerParentArgs(parser)
    flags.AddAnalyzerSelectorsGroup(parser)
    flags.AddAnalyzerOptionsGroup(parser, False)
    AddDestinationArgs(parser)

  def Run(self, args):
    parent = asset_utils.GetParentNameForAnalyzeIamPolicy(
        args.organization, args.project, args.folder)
    client = client_util.IamPolicyAnalysisLongrunningClient(
        client_util.V1P4BETA1_API_VERSION)
    operation = client.Analyze(parent, args, client_util.V1P4BETA1_API_VERSION)

    log.ExportResource(parent, is_async=True, kind='root asset')
    log.status.Print('Use [{} {}] to check the status of the operation.'.format(
        OPERATION_DESCRIBE_COMMAND, operation.name))
