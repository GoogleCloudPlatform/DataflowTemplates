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

"""Command to describe credentials."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

from googlecloudsdk.calliope import base
from googlecloudsdk.core import exceptions
from googlecloudsdk.core.credentials import store


class Error(exceptions.Error):
  """Errors raised by this module."""


class CredentialsNotFould(Error):
  """Raised when credentials could not be located."""


@base.Hidden
class Describe(base.DescribeCommand):
  """Describes credentials.

  Returns internal details about specified credentials.

  ## EXAMPLES

  To describe existing credentials, run:

            $ {command} ACCOUNT_NAME
  """

  @staticmethod
  def Args(parser):
    """Adds args for this command."""
    parser.add_argument(
        'account_name',
        help='Name of the account to describe')

  def Run(self, args):
    credential = store.Load(args.account_name)
    if not credential:
      raise CredentialsNotFould(
          'The credentials for account [{0}] do not exist.'
          .format(args.account_name))

    return json.loads(credential.to_json())
