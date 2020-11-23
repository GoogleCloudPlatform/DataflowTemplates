# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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

"""Utilities for the iamcredentials API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import datetime

from apitools.base.py import exceptions as apitools_exceptions
from googlecloudsdk.api_lib.util import apis_internal
from googlecloudsdk.api_lib.util import exceptions
from googlecloudsdk.core import exceptions as core_exceptions
from googlecloudsdk.core import http as http_core
from googlecloudsdk.core import resources
from googlecloudsdk.core import transport
from googlecloudsdk.core.credentials import transports
from oauth2client import client
from google.auth import exceptions as google_auth_exceptions


class Error(core_exceptions.Error):
  """Exception that are defined by this module."""


class ImpersonatedCredGoogleAuthRefreshError(Error):
  """Exception for google auth impersonated credentials refresh error."""


def GenerateAccessToken(service_account_id, scopes):
  """Generates an access token for the given service account."""
  service_account_ref = resources.REGISTRY.Parse(
      service_account_id, collection='iamcredentials.serviceAccounts',
      params={'projectsId': '-', 'serviceAccountsId': service_account_id})

  http_client = transports.GetApitoolsTransport(
      enable_resource_quota=False,
      response_encoding=transport.ENCODING,
      allow_account_impersonation=False)
  # pylint: disable=protected-access
  iam_client = apis_internal._GetClientInstance(
      'iamcredentials', 'v1', http_client=http_client)

  try:
    response = iam_client.projects_serviceAccounts.GenerateAccessToken(
        iam_client.MESSAGES_MODULE
        .IamcredentialsProjectsServiceAccountsGenerateAccessTokenRequest(
            name=service_account_ref.RelativeName(),
            generateAccessTokenRequest=iam_client.MESSAGES_MODULE
            .GenerateAccessTokenRequest(scope=scopes)
        )
    )
    return response
  except apitools_exceptions.HttpForbiddenError as e:
    raise exceptions.HttpException(
        e,
        error_format='Error {code} (Forbidden) - failed to impersonate '
                     '[{service_acc}]. Make sure the account that\'s trying '
                     'to impersonate it has access to the service account '
                     'itself and the "roles/iam.serviceAccountTokenCreator" '
                     'role.'.format(
                         code=e.status_code, service_acc=service_account_id))
  except apitools_exceptions.HttpError as e:
    raise exceptions.HttpException(e)


def GenerateIdToken(service_account_id, audience, include_email=False):
  """Generates an id token for the given service account."""
  service_account_ref = resources.REGISTRY.Parse(
      service_account_id, collection='iamcredentials.serviceAccounts',
      params={'projectsId': '-', 'serviceAccountsId': service_account_id})

  http_client = transports.GetApitoolsTransport(
      enable_resource_quota=False,
      response_encoding=transport.ENCODING,
      allow_account_impersonation=False)
  # pylint: disable=protected-access
  iam_client = apis_internal._GetClientInstance(
      'iamcredentials', 'v1', http_client=http_client)

  response = iam_client.projects_serviceAccounts.GenerateIdToken(
      iam_client.MESSAGES_MODULE
      .IamcredentialsProjectsServiceAccountsGenerateIdTokenRequest(
          name=service_account_ref.RelativeName(),
          generateIdTokenRequest=iam_client.MESSAGES_MODULE
          .GenerateIdTokenRequest(audience=audience, includeEmail=include_email)
      )
  )
  return response.token


class ImpersonationAccessTokenProvider(object):
  """A token provider for service account elevation.

  This supports the interface required by the core/credentials module.
  """

  def GetElevationAccessToken(self, service_account_id, scopes):
    response = GenerateAccessToken(service_account_id, scopes)
    return ImpersonationCredentials(
        service_account_id, response.accessToken, response.expireTime, scopes)

  def GetElevationIdToken(self, service_account_id, audience, include_email):
    return GenerateIdToken(service_account_id, audience, include_email)

  def GetElevationAccessTokenGoogleAuth(self, source_credentials,
                                        service_account_id, scopes):
    """Creates a fresh impersonation credential using google-auth library."""
    request_client = http_core.GoogleAuthRequest()
    # google-auth makes a shadow copy of the source_credentials and refresh
    # the copy instead of the original source_credentials. During the copying,
    # the monkey patch
    # (creds.CredentialStoreWithCache._WrapCredentialsRefreshWithAutoCaching)
    # is lost. Here, before passing to google-auth, we refresh
    # source_credentials.
    source_credentials.refresh(request_client)
    # Import only when necessary to decrease the startup time. Move it to
    # global once google-auth is ready to replace oauth2client.
    # pylint: disable=g-import-not-at-top
    from google.auth import impersonated_credentials as google_auth_impersonated_creds
    # pylint: enable=g-import-not-at-top
    cred = google_auth_impersonated_creds.Credentials(source_credentials,
                                                      service_account_id,
                                                      scopes)
    try:
      cred.refresh(request_client)
    except google_auth_exceptions.RefreshError:
      raise ImpersonatedCredGoogleAuthRefreshError(
          'Failed to impersonate [{service_acc}]. Make sure the '
          'account that\'s trying to impersonate it has access to the service '
          'account itself and the "roles/iam.serviceAccountTokenCreator" '
          'role.'.format(service_acc=service_account_id))
    return cred

  def GetElevationIdTokenGoogleAuth(self, google_auth_impersonation_credentials,
                                    audience, include_email):
    """Creates an ID token credentials for impersonated credentials."""
    # Import only when necessary to decrease the startup time. Move it to
    # global once google-auth is ready to replace oauth2client.
    # pylint: disable=g-import-not-at-top
    from google.auth import impersonated_credentials as google_auth_impersonated_creds
    # pylint: enable=g-import-not-at-top
    cred = google_auth_impersonated_creds.IDTokenCredentials(
        google_auth_impersonation_credentials,
        target_audience=audience,
        include_email=include_email)
    request_client = http_core.GoogleAuthRequest()
    cred.refresh(request_client)
    return cred

  @classmethod
  def IsImpersonationCredential(cls, cred):
    # Import only when necessary to decrease the startup time. Move it to
    # global once google-auth is ready to replace oauth2client.
    # pylint: disable=g-import-not-at-top
    from google.auth import impersonated_credentials as google_auth_impersonated_creds
    # pylint: enable=g-import-not-at-top
    return isinstance(cred, ImpersonationCredentials) or isinstance(
        cred, google_auth_impersonated_creds.Credentials)


class ImpersonationCredentials(client.OAuth2Credentials):
  """Implementation of a credential that refreshes using the iamcredentials API.
  """
  _EXPIRY_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

  def __init__(self, service_account_id, access_token, token_expiry, scopes):
    self._service_account_id = service_account_id
    token_expiry = self._ConvertExpiryTime(token_expiry)
    super(ImpersonationCredentials, self).__init__(
        access_token, None, None, None, token_expiry, None, None, scopes=scopes)

  def _refresh(self, http):
    # client.OAuth2Credentials converts scopes into a set, so we need to convert
    # back to a list before making the API request.
    response = GenerateAccessToken(self._service_account_id, list(self.scopes))
    self.access_token = response.accessToken
    self.token_expiry = self._ConvertExpiryTime(response.expireTime)

  def _ConvertExpiryTime(self, value):
    return datetime.datetime.strptime(value,
                                      ImpersonationCredentials._EXPIRY_FORMAT)
