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

"""Manages logic for service accounts."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import io
import json
import os

from googlecloudsdk.core import config
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import log
from googlecloudsdk.core.credentials import creds as c_creds
from googlecloudsdk.core.util import encoding
from googlecloudsdk.core.util import files

from oauth2client import service_account


class Error(exceptions.Error):
  """Errors raised by this module."""


class UnsupportedCredentialsType(Error):
  """Raised when given type credentials cannot be created."""


class BadCredentialFileException(Error):
  """Raised when file cannot be read."""


class BadCredentialJsonFileException(Error):
  """Raised when file cannot be read."""


def CredentialsFromAdcFile(filename):
  """Load credentials from given service account json file."""
  content = files.ReadFileContents(filename)
  try:
    json_key = json.loads(content)
    return CredentialsFromAdcDict(json_key)
  except ValueError as e:
    raise BadCredentialFileException('Could not read json file {0}: {1}'
                                     .format(filename, e))


def CredentialsFromAdcDict(json_key):
  """Creates oauth2client creds from a dict of application default creds."""
  if 'client_email' not in json_key:
    raise BadCredentialJsonFileException(
        'The .json key file is not in a valid format.')

  json_key['token_uri'] = c_creds.GetEffectiveTokenUri(json_key)

  creds = service_account.ServiceAccountCredentials.from_json_keyfile_dict(
      json_key, scopes=config.CLOUDSDK_SCOPES)
  # User agent needs to be set separately, see
  # https://github.com/google/oauth2client/issues/445
  # pylint: disable=protected-access
  creds.user_agent = creds._user_agent = config.CLOUDSDK_USER_AGENT
  return creds


def CredentialsFromAdcDictGoogleAuth(json_key):
  """Creates google-auth creds from a dict of application default creds."""
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  from google.oauth2 import service_account as google_auth_service_account
  # pylint: enable=g-import-not-at-top

  if 'client_email' not in json_key:
    raise BadCredentialJsonFileException(
        'The .json key file is not in a valid format.')

  # 'token_uri' is required by google-auth credentails construction. However,
  # the service account keys generated before 2015 do not provide this field.
  # More details in http://shortn/_LtMjDvpgfh.
  json_key['token_uri'] = c_creds.GetEffectiveTokenUri(json_key)

  service_account_credentials = (
      google_auth_service_account.Credentials.from_service_account_info)
  creds = service_account_credentials(json_key, scopes=config.CLOUDSDK_SCOPES)
  # The below additional fields are not natively supported in the google-auth
  # library but are needed in gcloud:
  # private_key, private_key_id: required by credentials deserialization;
  # client_id: to provid backward compatibility for oauth2client. This field is
  # used in oauth2client service account credentials.
  creds.private_key = json_key.get('private_key')
  creds.private_key_id = json_key.get('private_key_id')
  creds.client_id = json_key.get('client_id')
  return creds


def CredentialsFromP12File(filename, account, password=None):
  """Create p12 service account credentials from given file."""

  try:
    private_key = files.ReadBinaryFileContents(filename)
  except EnvironmentError as e:
    raise BadCredentialFileException('Could not read file {0}'.format(e))

  return CredentialsFromP12Key(private_key, account, password)


def CredentialsFromP12Key(private_key, account, password=None):
  """Creates creadentials object from given private key and account name."""
  log.warning('.p12 service account keys are not recomended unless it is '
              'necessary for backwards compatability. Please switch to '
              'a newer .json service account key for this account.')

  try:
    cred = service_account.ServiceAccountCredentials.from_p12_keyfile_buffer(
        service_account_email=account,
        file_buffer=io.BytesIO(private_key),
        private_key_password=password,
        scopes=config.CLOUDSDK_SCOPES,
        token_uri=c_creds.GetEffectiveTokenUri({})
    )
  except NotImplementedError:
    if not encoding.GetEncodedValue(os.environ, 'CLOUDSDK_PYTHON_SITEPACKAGES'):
      raise UnsupportedCredentialsType(
          ('PyOpenSSL is not available. If you have already installed '
           'PyOpenSSL, you will need to enable site packages by '
           'setting the environment variable CLOUDSDK_PYTHON_SITEPACKAGES '
           'to 1. If that does not work, see '
           'https://developers.google.com/cloud/sdk/crypto for details '
           'or consider using .json private key instead.'))
    else:
      raise UnsupportedCredentialsType(
          ('PyOpenSSL is not available. See '
           'https://developers.google.com/cloud/sdk/crypto for details '
           'or consider using .json private key instead.'))

  # pylint: disable=protected-access
  cred.user_agent = cred._user_agent = config.CLOUDSDK_USER_AGENT

  return cred
