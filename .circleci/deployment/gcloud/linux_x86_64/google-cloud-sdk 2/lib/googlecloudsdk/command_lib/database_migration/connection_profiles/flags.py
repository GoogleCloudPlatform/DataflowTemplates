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
"""Flags and helpers for the connection profiles related commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def AddDisplayNameFlag(parser):
  """Adds a --display-name flag to the given parser."""
  help_text = """\
    Friendly name for the connection profile.
    """
  parser.add_argument('--display-name', help=help_text)


def AddUsernameFlag(parser, required=False):
  """Adds a --username flag to the given parser."""
  help_text = """\
    Username that Database Migration Service will use to connect to the
    database. The value is encrypted when stored in Database Migration Service.
    """
  parser.add_argument('--username', help=help_text, required=required)


def AddPasswordFlagGroup(parser, required=False):
  """Adds --password and --prompt-for-password flags to the given parser."""

  password_group = parser.add_group(required=required, mutex=True)
  password_group.add_argument(
      '--password',
      help="""\
          Password for the user that Database Migration Service will be using to
          connect to the database. This field is not returned on request, and
          the value is encrypted when stored in Database Migration Service.
          """
  )
  password_group.add_argument(
      '--prompt-for-password',
      action='store_true',
      help='Prompt for the password used to connect to the database.'
  )


def AddHostFlag(parser, required=False):
  """Adds --host flag to the given parser."""
  help_text = """\
    IP or hostname of the source MySQL database.
  """
  parser.add_argument('--host', help=help_text, required=required)


def AddPortFlag(parser, required=False):
  """Adds --host flag to the given parser."""
  help_text = """\
    Network port of the source MySQL database.
  """
  parser.add_argument('--port', help=help_text, required=required, type=int)


def AddSslConfigGroup(parser):
  """Adds --password and --prompt-for-password flags to the given parser."""
  ssl_config = parser.add_group()
  AddCaCertificateFlag(ssl_config, True)
  client_cert = ssl_config.add_group()
  AddCertificateFlag(client_cert, required=True)
  AddPrivateKeyFlag(client_cert, required=True)


def AddCaCertificateFlag(parser, required=False):
  """Adds --ca-certificate flag to the given parser."""
  help_text = """\
    x509 PEM-encoded certificate of the CA that signed the source database
    server's certificate. The replica will use this certificate to verify
    it's connecting to the right host.
  """
  parser.add_argument('--ca-certificate', help=help_text, required=required)


def AddCertificateFlag(parser, required=False):
  """Adds --certificate flag to the given parser."""
  help_text = """\
    x509 PEM-encoded certificate that will be used by the replica to
    authenticate against the source database server.
  """
  parser.add_argument('--certificate', help=help_text, required=required)


def AddPrivateKeyFlag(parser, required=False):
  """Adds --private-key flag to the given parser."""
  help_text = """\
    Unencrypted PKCS#1 or PKCS#8 PEM-encoded private key associated with
    the Client Certificate.
  """
  parser.add_argument('--private-key', help=help_text, required=required)


def AddCloudSQLInstanceFlag(parser, required=False):
  """Adds --instance flag to the given parser."""
  help_text = """\
    If the source is a Cloud SQL database, use this field to provide the Cloud
    SQL instance ID of the source.
  """
  parser.add_argument('--instance', help=help_text, required=required)


def AddProviderFlag(parser):
  """Adds --provider flag to the given parser."""
  help_text = """\
    Database provider (CLOUDSQL or RDS).
  """
  choices = ['RDS', 'CLOUDSQL']
  parser.add_argument('--provider', help=help_text, choices=choices)

