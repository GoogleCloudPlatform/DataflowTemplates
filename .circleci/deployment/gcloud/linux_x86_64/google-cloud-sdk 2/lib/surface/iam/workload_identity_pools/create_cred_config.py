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
"""Command to create a configuration file to allow authentication from 3rd party sources."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import textwrap

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iam.workload_identity_pools import cred_config
from googlecloudsdk.core import log
from googlecloudsdk.core.util import files

OAUTH_URL = 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='
RESOURCE_TYPE = 'credential configuration file'


class CreateCredConfig(base.CreateCommand):
  """Create a configuration file for generated credentials.

  This command creates a configuration file to allow access to authenticated
  Cloud Platform actions from a variety of external accounts.
  """

  detailed_help = {
      'EXAMPLES':
          textwrap.dedent("""\
          To create a file-sourced credential configuration for your project, run:

            $ {command} projects/$PROJECT_NUMBER/locations/$REGION/workloadIdentityPools/$WORKLOAD_POOL_ID/providers/$PROVIDER_ID --service-account=$EMAIL --credential-source-file=$PATH_TO_OIDC_ID_TOKEN --output-file=credentials.json

          To create a url-sourced credential configuration for your project, run:

            $ {command} projects/$PROJECT_NUMBER/locations/$REGION/workloadIdentityPools/$WORKLOAD_POOL_ID/providers/$PROVIDER_ID --service-account=$EMAIL --credential-source-url=$URL_FOR_OIDC_TOKEN --credential-source-headers=Key=Value --output-file=credentials.json

          To create an aws-based credential configuration for your project, run:

            $ {command} projects/$PROJECT_NUMBER/locations/$REGION/workloadIdentityPools/$WORKLOAD_POOL_ID/providers/$PROVIDER_ID --service-account=$EMAIL --aws --output-file=credentials.json

          To create an azure-based credential configuration for your project, run:

            $ {command} projects/$PROJECT_NUMBER/locations/$REGION/workloadIdentityPools/$WORKLOAD_POOL_ID/providers/$PROVIDER_ID --service-account=$EMAIL --azure --app-id-uri=$URI_FOR_AZURE_APP_ID --output-file=credentials.json

          To use the resulting file for any of these commands, set the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to the generated file
          """),
  }

  @staticmethod
  def Args(parser):
    parser.add_argument(
        'audience', help='The workload identity pool provider resource name.')

    credential_types = parser.add_group(
        mutex=True, required=True, help='Credential types.')
    credential_types.add_argument(
        '--credential-source-file',
        help='Location of the credential source file.')
    credential_types.add_argument(
        '--credential-source-url',
        help='URL to obtain the credential from.')
    credential_types.add_argument('--aws', help='Use AWS.', action='store_true')
    credential_types.add_argument(
        '--azure', help='Use Azure.', action='store_true')

    parser.add_argument(
        '--service-account',
        help='The email of the service account to impersonate.')
    parser.add_argument(
        '--credential-source-headers',
        type=arg_parsers.ArgDict(),
        metavar='key=value',
        help='Headers to use when querying the credential-source-url.')
    parser.add_argument(
        '--credential-source-type',
        help='The format of the credential source (JSON or text).')
    parser.add_argument(
        '--credential-source-field-name',
        help='The subject token field name (key) in a JSON credential source.')
    parser.add_argument(
        '--app-id-uri',
        help='The custom Application ID URI for the Azure access token.')
    parser.add_argument(
        '--output-file',
        help='Location to store the generated credential configuration file.',
        required=True)
    parser.add_argument(
        '--subject-token-type',
        help='The type of token being used for authorization.')

  def Run(self, args):
    try:
      generator = cred_config.get_generator(args)

      output = {
          'type': 'external_account',
          'audience': '//iam.googleapis.com/' + args.audience,
          'subject_token_type':
              generator.get_token_type(args.subject_token_type),
          'token_url':
              'https://sts.googleapis.com/v1beta/token',
          'credential_source':
              generator.get_source(args.credential_source_type,
                                   args.credential_source_field_name),
      }

      if args.service_account:
        output['service_account_impersonation_url'] = ''.join((
            'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/',
            args.service_account, ':generateAccessToken'))

      files.WriteFileContents(args.output_file, json.dumps(output))
      log.CreatedResource(args.output_file, RESOURCE_TYPE)
    except cred_config.GeneratorError as cce:
      log.CreatedResource(args.output_file, RESOURCE_TYPE, failed=cce.message)
