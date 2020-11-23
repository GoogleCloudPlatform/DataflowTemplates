# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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

"""Base class for Hive Job."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.command_lib.dataproc.jobs import base as job_base


class HiveBase(job_base.JobBase):
  """Common functionality between release tracks."""

  @staticmethod
  def Args(parser):
    """Performs command line parsing specific to Hive."""
    driver = parser.add_mutually_exclusive_group(required=True)
    driver.add_argument(
        '--execute', '-e',
        metavar='QUERY',
        dest='queries',
        action='append',
        default=[],
        help='A Hive query to execute as part of the job.')
    driver.add_argument(
        '--file', '-f',
        help='HCFS URI of file containing Hive script to execute as the job.')
    parser.add_argument(
        '--jars',
        type=arg_parsers.ArgList(),
        metavar='JAR',
        default=[],
        help=('Comma separated list of jar files to be provided to the '
              'Hive and MR. May contain UDFs.'))
    parser.add_argument(
        '--params',
        type=arg_parsers.ArgDict(),
        metavar='PARAM=VALUE',
        help='A list of key value pairs to set variables in the Hive queries.')
    parser.add_argument(
        '--properties',
        type=arg_parsers.ArgDict(),
        metavar='PROPERTY=VALUE',
        help='A list of key value pairs to configure Hive.')
    parser.add_argument(
        '--continue-on-failure',
        action='store_true',
        help='Whether to continue if a single query fails.')

  @staticmethod
  def GetFilesByType(args):
    return {
        'jars': args.jars,
        'file': args.file}

  @staticmethod
  def ConfigureJob(messages, job, files_by_type, args):
    """Populates the hiveJob member of the given job."""

    hive_job = messages.HiveJob(
        continueOnFailure=args.continue_on_failure,
        jarFileUris=files_by_type['jars'],
        queryFileUri=files_by_type['file'])

    if args.queries:
      hive_job.queryList = messages.QueryList(queries=args.queries)
    if args.params:
      hive_job.scriptVariables = encoding.DictToAdditionalPropertyMessage(
          args.params, messages.HiveJob.ScriptVariablesValue)
    if args.properties:
      hive_job.properties = encoding.DictToAdditionalPropertyMessage(
          args.properties, messages.HiveJob.PropertiesValue)

    job.hiveJob = hive_job
