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
"""Command to analyze IAM policy asynchronously in the specified root asset."""

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


def AddDestinationGroup(parser):
  destination_group = parser.add_group(
      mutex=True,
      required=True,
      help='The destination path for writing IAM policy analysis results.')
  AddGcsOutputPathArgs(destination_group)
  AddBigQueryDestinationGroup(destination_group)


def AddGcsOutputPathArgs(parser):
  parser.add_argument(
      '--gcs-output-path',
      metavar='GCS_OUTPUT_PATH',
      required=False,
      type=arg_parsers.RegexpValidator(
          r'^gs://.*',
          '--gcs-output-path must be a Google Cloud Storage URI starting with '
          '"gs://". For example, "gs://bucket_name/object_name".'),
      help='Google Cloud Storage URI where the results will be written. URI '
      'must start with "gs://". For example, "gs://bucket_name/object_name".')


def AddBigQueryDestinationGroup(parser):
  bigquery_destination_group = parser.add_group(
      mutex=False,
      required=False,
      help='BigQuery destination where the results will go.')
  AddBigQueryDatasetArgs(bigquery_destination_group)
  AddBigQueryTablePrefixArgs(bigquery_destination_group)
  AddBigQueryPartitionKeyArgs(bigquery_destination_group)
  AddBigQueryWriteDispositionArgs(bigquery_destination_group)


def AddBigQueryDatasetArgs(parser):
  parser.add_argument(
      '--bigquery-dataset',
      metavar='BIGQUERY_DATASET',
      required=True,
      type=arg_parsers.RegexpValidator(
          r'^projects/[A-Za-z0-9\-]+/datasets/[\w]+',
          '--bigquery-dataset must be a dataset relative name starting with '
          '"projects/". For example, '
          '"projects/project_id/datasets/dataset_id".'),
      help='BigQuery dataset where the results will be written. Must be a '
      'dataset relative name starting with "projects/". For example, '
      '"projects/project_id/datasets/dataset_id".')


def AddBigQueryTablePrefixArgs(parser):
  parser.add_argument(
      '--bigquery-table-prefix',
      metavar='BIGQUERY_TABLE_PREFIX',
      required=True,
      type=arg_parsers.RegexpValidator(
          r'[\w]+',
          '--bigquery-table-prefix must be a BigQuery table name consists of '
          'letters, numbers and underscores".'),
      help='The prefix of the BigQuery tables to which the analysis results '
      'will be written. A table name consists of letters, numbers and '
      'underscores".')


def AddBigQueryPartitionKeyArgs(parser):
  parser.add_argument(
      '--bigquery-partition-key',
      choices=['PARTITION_KEY_UNSPECIFIED', 'REQUEST_TIME'],
      help='This enum determines the partition key column for the bigquery '
      'tables. Partitioning can improve query performance and reduce query cost'
      ' by filtering partitions. Refer to '
      'https://cloud.google.com/bigquery/docs/partitioned-tables for details.')


def AddBigQueryWriteDispositionArgs(parser):
  parser.add_argument(
      '--bigquery-write-disposition',
      metavar='BIGQUERY_WRITE_DISPOSITION',
      help='Specifies the action that occurs if the destination table or '
      'partition already exists. The following values are supported: '
      'WRITE_TRUNCATE, WRITE_APPEND and WRITE_EMPTY. The default value is '
      'WRITE_APPEND.')


@base.ReleaseTracks(base.ReleaseTrack.GA)
class AnalyzeIamPolicyLongrunning(base.Command):
  """Analyzes IAM policies that match a request asynchronously and writes the analysis results to Google Cloud Storage or BigQuery destination."""

  detailed_help = {
      'DESCRIPTION':
          """\
      Analyzes IAM policies that match a request asynchronously and writes
      the analysis results to Google Cloud Storage or BigQuery destination.""",
      'EXAMPLES':
          """\
          To find out which users have been granted the
          iam.serviceAccounts.actAs permission on a service account, and write
          analysis results to Google Cloud Storage, run:

            $ {command} --organization=YOUR_ORG_ID --full-resource-name=YOUR_SERVICE_ACCOUNT_FULL_RESOURCE_NAME --permissions='iam.serviceAccounts.actAs' --gcs-output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

          To find out which resources a user can access, and write analysis
          results to Google Cloud Storage, run:

            $ {command} --organization=YOUR_ORG_ID --identity='user:u1@foo.com' --gcs-output-path='gs://YOUR_BUCKET_NAME/YOUR_OBJECT_NAME'

          To find out which roles or permissions a user has been granted on a
          project, and write analysis results to BigQuery, run:

            $ {command} --organization=YOUR_ORG_ID --full-resource-name=YOUR_PROJECT_FULL_RESOURCE_NAME --identity='user:u1@foo.com' --bigquery-table-prefix='projects/YOUR_PROJECT_ID/datasets/YOUR_DATASET_ID/tables/YOUR_TABLE_PREFIX'

          To find out which users have been granted the
          iam.serviceAccounts.actAs permission on any applicable resources, and
          write analysis results to BigQuery, run:

            $ {command} --organization=YOUR_ORG_ID --permissions='iam.serviceAccounts.actAs' --bigquery-table-prefix='projects/YOUR_PROJECT_ID/datasets/YOUR_DATASET_ID/tables/YOUR_TABLE_PREFIX'

      """
  }

  @staticmethod
  def Args(parser):
    flags.AddAnalyzerParentArgs(parser)
    flags.AddAnalyzerSelectorsGroup(parser)
    flags.AddAnalyzerOptionsGroup(parser, False)
    AddDestinationGroup(parser)

  def Run(self, args):
    parent = asset_utils.GetParentNameForAnalyzeIamPolicy(
        args.organization, args.project, args.folder)
    client = client_util.IamPolicyAnalysisLongrunningClient()
    operation = client.Analyze(parent, args)

    log.status.Print('Analyze IAM Policy in progress.')
    log.status.Print('Use [{} {}] to check the status of the operation.'.format(
        OPERATION_DESCRIBE_COMMAND, operation.name))
