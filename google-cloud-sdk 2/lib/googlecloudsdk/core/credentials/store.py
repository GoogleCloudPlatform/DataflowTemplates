# -*- coding: utf-8 -*- #
# Copyright 2013 Google LLC. All Rights Reserved.
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

"""One-line documentation for auth module.

A detailed description of auth.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import contextlib
import datetime
import json
import os
import textwrap
import time

import dateutil
from googlecloudsdk.core import config
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import http
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import transport
from googlecloudsdk.core.configurations import named_configs
from googlecloudsdk.core.credentials import creds as c_creds
from googlecloudsdk.core.credentials import devshell as c_devshell
from googlecloudsdk.core.credentials import gce as c_gce
from googlecloudsdk.core.util import files
from googlecloudsdk.core.util import times


import httplib2
from oauth2client import client
from oauth2client import crypt
from oauth2client import service_account
from oauth2client.contrib import gce as oauth2client_gce
from oauth2client.contrib import reauth_errors
import six
from six.moves import urllib
from google.auth import exceptions as google_auth_exceptions
import google.auth.compute_engine as google_auth_gce


GOOGLE_OAUTH2_PROVIDER_AUTHORIZATION_URI = (
    'https://accounts.google.com/o/oauth2/auth')
GOOGLE_OAUTH2_PROVIDER_REVOKE_URI = (
    'https://accounts.google.com/o/oauth2/revoke')
GOOGLE_OAUTH2_PROVIDER_TOKEN_URI = (
    'https://accounts.google.com/o/oauth2/token')
_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:jwt-bearer'

_CREDENTIALS_EXPIRY_WINDOW = '300s'

AUTH_LOGIN_COMMAND = 'gcloud auth login'
ADC_LOGIN_COMMAND = 'gcloud auth application-default login'

CONTEXT_AWARE_ACCESS_DENIED_ERROR = 'access_denied'
CONTEXT_AWARE_ACCESS_DENIED_ERROR_DESCRIPTION = 'Account restricted'
CONTEXT_AWARE_ACCESS_HELP_MSG = (
    'Access was blocked due to an organization policy, please contact your '
    'admin to gain access.'
)


def IsContextAwareAccessDeniedError(exc):
  exc_text = six.text_type(exc)
  return (CONTEXT_AWARE_ACCESS_DENIED_ERROR in exc_text and
          CONTEXT_AWARE_ACCESS_DENIED_ERROR_DESCRIPTION in exc_text)


class Error(exceptions.Error):
  """Exceptions for the credentials module."""


class AuthenticationException(Error):
  """Exceptions that tell the users to re-login."""

  def __init__(self, message, for_adc=False, should_relogin=True):
    if should_relogin:
      login_command = ADC_LOGIN_COMMAND if for_adc else AUTH_LOGIN_COMMAND
      message = textwrap.dedent("""\
        {message}
        Please run:

          $ {login_command}

        to obtain new credentials.""".format(
            message=message, login_command=login_command))
    if not for_adc:
      switch_account_msg = textwrap.dedent("""\
      If you have already logged in with a different account:

          $ gcloud config set account ACCOUNT

      to select an already authenticated account to use.""")
      message = '\n\n'.join([message, switch_account_msg])
    super(AuthenticationException, self).__init__(message)


class PrintTokenAuthenticationException(Error):
  """Exceptions that tell the users to run auth login."""

  def __init__(self, message):
    super(PrintTokenAuthenticationException, self).__init__(textwrap.dedent("""\
        {message}
        Please run:

          $ gcloud auth login

        to obtain new credentials.

        For service account, please activate it first:

          $ gcloud auth activate-service-account ACCOUNT""".format(
              message=message)))


class NoCredentialsForAccountException(PrintTokenAuthenticationException):
  """Exception for when no credentials are found for an account."""

  def __init__(self, account):
    super(NoCredentialsForAccountException, self).__init__(
        'Your current active account [{account}] does not have any'
        ' valid credentials'.format(account=account))


class NoActiveAccountException(AuthenticationException):
  """Exception for when there are no valid active credentials."""

  def __init__(self, active_config_path=None):
    if active_config_path:
      if not os.path.exists(active_config_path):
        log.warning('Could not open the configuration file: [%s].',
                    active_config_path)
    super(NoActiveAccountException, self).__init__(
        'You do not currently have an active account selected.')


class TokenRefreshError(AuthenticationException):
  """An exception raised when the auth tokens fail to refresh."""

  def __init__(self, error, for_adc=False, should_relogin=True):
    message = ('There was a problem refreshing your current auth tokens: {0}'
               .format(error))
    super(TokenRefreshError, self).__init__(
        message, for_adc=for_adc, should_relogin=should_relogin)


class TokenRefreshDeniedByCAAError(TokenRefreshError):
  """Raises when token refresh is denied by context aware access policies."""

  def __init__(self, error, for_adc=False):
    compiled_msg = '{}\n\n{}'.format(error, CONTEXT_AWARE_ACCESS_HELP_MSG)

    super(TokenRefreshDeniedByCAAError, self).__init__(
        compiled_msg, for_adc=for_adc, should_relogin=False)


class ReauthenticationException(Error):
  """Exceptions that tells the user to retry his command or run auth login."""

  def __init__(self, message, for_adc=False):
    login_command = ADC_LOGIN_COMMAND if for_adc else AUTH_LOGIN_COMMAND
    super(ReauthenticationException, self).__init__(
        textwrap.dedent("""\
        {message}
        Please retry your command or run:

          $ {login_command}

        to obtain new credentials.""".format(
            message=message, login_command=login_command)))


class TokenRefreshReauthError(ReauthenticationException):
  """An exception raised when the auth tokens fail to refresh due to reauth."""

  def __init__(self, error, for_adc=False):
    message = ('There was a problem reauthenticating while refreshing your '
               'current auth tokens: {0}').format(error)
    super(TokenRefreshReauthError, self).__init__(message, for_adc=for_adc)


class WebLoginRequiredReauthError(Error):
  """An exception raised when login through browser is required for reauth.

  This applies to SAML users who set password as their reauth method today.
  Since SAML uers do not have knowledge of their Google password, we require
  web login and allow users to be authenticated by their IDP.
  """

  def __init__(self, for_adc=False):
    login_command = ADC_LOGIN_COMMAND if for_adc else AUTH_LOGIN_COMMAND
    super(WebLoginRequiredReauthError, self).__init__(textwrap.dedent("""\
        Please run:

          $ {login_command}

        to complete reauthentication.""".format(login_command=login_command)))


class InvalidCredentialFileException(Error):
  """Exception for when an external credential file could not be loaded."""

  def __init__(self, f, e):
    super(InvalidCredentialFileException, self).__init__(
        'Failed to load credential file: [{f}].  {message}'
        .format(f=f, message=six.text_type(e)))


class AccountImpersonationError(Error):
  """Exception for when attempting to impersonate a service account fails."""
  pass


class FlowError(Error):
  """Exception for when something goes wrong with a web flow."""


class RevokeError(Error):
  """Exception for when there was a problem revoking."""


class InvalidCodeVerifierError(Error):
  """Exception for invalid code verifier for pkce."""


IMPERSONATION_TOKEN_PROVIDER = None


class StaticCredentialProviders(object):
  """Manages a list of credential providers."""

  def __init__(self):
    self._providers = []

  def AddProvider(self, provider):
    self._providers.append(provider)

  def RemoveProvider(self, provider):
    self._providers.remove(provider)

  def GetCredentials(self, account, use_google_auth=False):
    for provider in self._providers:
      cred = provider.GetCredentials(account, use_google_auth)
      if cred is not None:
        return cred
    return None

  def GetAccounts(self):
    accounts = set()
    for provider in self._providers:
      accounts |= provider.GetAccounts()
    return accounts


STATIC_CREDENTIAL_PROVIDERS = StaticCredentialProviders()


class DevShellCredentialProvider(object):
  """Provides account, project and credential data for devshell env."""

  def GetCredentials(self, account, use_google_auth=False):
    devshell_creds = c_devshell.LoadDevshellCredentials(use_google_auth)
    if devshell_creds and (devshell_creds.devshell_response.user_email ==
                           account):
      return devshell_creds
    return None

  def GetAccount(self):
    return c_devshell.DefaultAccount()

  def GetAccounts(self):
    # DevShellCredentialsGoogleAuth and DevShellCredentials use the same code
    # to get devshell_response, so here it is safe to load
    # DevShellCredentialsGoogleAuth.
    devshell_creds = c_devshell.LoadDevshellCredentials(use_google_auth=True)
    if devshell_creds:
      return set([devshell_creds.devshell_response.user_email])
    return set()

  def GetProject(self):
    return c_devshell.Project()

  def Register(self):
    properties.VALUES.core.account.AddCallback(self.GetAccount)
    properties.VALUES.core.project.AddCallback(self.GetProject)
    STATIC_CREDENTIAL_PROVIDERS.AddProvider(self)

  def UnRegister(self):
    properties.VALUES.core.account.RemoveCallback(self.GetAccount)
    properties.VALUES.core.project.RemoveCallback(self.GetProject)
    STATIC_CREDENTIAL_PROVIDERS.RemoveProvider(self)


class GceCredentialProvider(object):
  """Provides account, project and credential data for gce vm env."""

  def GetCredentials(self, account, use_google_auth=False):
    if account in c_gce.Metadata().Accounts():
      return AcquireFromGCE(account, use_google_auth)
    return None

  def GetAccount(self):
    if properties.VALUES.core.check_gce_metadata.GetBool():
      return c_gce.Metadata().DefaultAccount()
    return None

  def GetAccounts(self):
    return set(c_gce.Metadata().Accounts())

  def GetProject(self):
    if properties.VALUES.core.check_gce_metadata.GetBool():
      return c_gce.Metadata().Project()
    return None

  def Register(self):
    properties.VALUES.core.account.AddCallback(self.GetAccount)
    properties.VALUES.core.project.AddCallback(self.GetProject)
    STATIC_CREDENTIAL_PROVIDERS.AddProvider(self)

  def UnRegister(self):
    properties.VALUES.core.account.RemoveCallback(self.GetAccount)
    properties.VALUES.core.project.RemoveCallback(self.GetProject)
    STATIC_CREDENTIAL_PROVIDERS.RemoveProvider(self)


def AvailableAccounts():
  """Get all accounts that have credentials stored for the CloudSDK.

  This function will also ping the GCE metadata server to see if GCE credentials
  are available.

  Returns:
    [str], List of the accounts.

  """
  store = c_creds.GetCredentialStore()
  accounts = store.GetAccounts() | STATIC_CREDENTIAL_PROVIDERS.GetAccounts()

  return sorted(accounts)


def GoogleAuthDisabledGlobally():
  """Returns True if google-auth is disabled globally."""
  return properties.VALUES.auth.disable_load_google_auth.GetBool()


def _TokenExpiresWithinWindow(expiry_window,
                              token_expiry_time,
                              max_window_seconds=3600):
  """Determines if token_expiry_time is within expiry_window_duration.

  Calculates the amount of time between utcnow() and token_expiry_time and
  returns true, if that amount is less than the provided duration window. All
  calculations are done in number of seconds for consistency.


  Args:
    expiry_window: string, Duration representing the amount of time between
      now and token_expiry_time to compare against.
    token_expiry_time: datetime, The time when token expires.
    max_window_seconds: int, Maximum size of expiry window, in seconds.

  Raises:
    ValueError: If expiry_window is invalid or can not be parsed.

  Returns:
    True if token is expired or will expire with in the provided window,
    False otherwise.
  """
  try:
    min_expiry = times.ParseDuration(expiry_window, default_suffix='s')
    if min_expiry.total_seconds > max_window_seconds:
      raise ValueError('Invalid expiry window duration [{}]: '
                       'Must be between 0s and 1h'.format(expiry_window))
  except times.Error as e:
    message = six.text_type(e).rstrip('.')
    raise ValueError('Error Parsing expiry window duration '
                     '[{}]: {}'.format(expiry_window, message))

  token_expiry_time = times.LocalizeDateTime(token_expiry_time,
                                             tzinfo=dateutil.tz.tzutc())
  window_end = times.GetDateTimePlusDuration(
      times.Now(tzinfo=dateutil.tz.tzutc()), min_expiry)

  return token_expiry_time <= window_end


def _GetAccessTokenFromCreds(creds):
  if creds is None:
    return None
  if c_creds.IsGoogleAuthCredentials(creds):
    return creds.token
  else:
    return creds.access_token


def GetAccessToken(account=None, scopes=None, allow_account_impersonation=True):
  """Returns the access token of the given account or the active account.

  GetAccessToken ignores whether credentials have been disabled via properties.
  Use this function when the caller absolutely requires credentials.

  Args:
    account: str, The account to get the access token for. If None, the
      account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES are
      requested.
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured).
  """
  creds = Load(account, scopes, False, allow_account_impersonation, True)
  return _GetAccessTokenFromCreds(creds)


def GetAccessTokenIfEnabled(account=None,
                            scopes=None,
                            allow_account_impersonation=True):
  """Returns the access token of the given account or the active account.

  If credentials have been disabled via properties, this will return None.
  Otherwise it return the access token of the account like normal. Use this
  function when credentials are optional for the caller, or the caller want to
  handle the situation of credentials being disabled by properties.

  Args:
    account: str, The account to get the access token for. If None, the
      account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES are
      requested.
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured).
  """
  if properties.VALUES.auth.disable_credentials.GetBool():
    return None
  return GetAccessToken(account, scopes, allow_account_impersonation)


def GetFreshAccessToken(account=None,
                        scopes=None,
                        min_expiry_duration='1h',
                        allow_account_impersonation=True):
  """Returns a fresh access token of the given account or the active account.

  Same as GetAccessToken except that the access token returned by
  this function is valid for at least min_expiry_duration.

  Args:
    account: str, The account to get the access token for. If None, the
      account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES are
      requested.
    min_expiry_duration: Duration str, Refresh the token if they are
      within this duration from expiration. Must be a valid duration between 0
      seconds and 1 hour (e.g. '0s' >x< '1h').
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured).
  """
  creds = LoadFreshCredential(account, scopes, min_expiry_duration,
                              allow_account_impersonation, True)
  return _GetAccessTokenFromCreds(creds)


def GetFreshAccessTokenIfEnabled(account=None,
                                 scopes=None,
                                 min_expiry_duration='1h',
                                 allow_account_impersonation=True):
  """Returns a fresh access token of the given account or the active account.

  Same as GetAccessTokenIfEnabled except that the access token returned by
  this function is valid for at least min_expiry_duration.

  Args:
    account: str, The account to get the access token for. If None, the
      account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES are
      requested.
    min_expiry_duration: Duration str, Refresh the token if they are
      within this duration from expiration. Must be a valid duration between 0
      seconds and 1 hour (e.g. '0s' >x< '1h').
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured).
  """
  if properties.VALUES.auth.disable_credentials.GetBool():
    return None
  return GetFreshAccessToken(account, scopes, min_expiry_duration,
                             allow_account_impersonation)


def LoadFreshCredential(account=None,
                        scopes=None,
                        min_expiry_duration='1h',
                        allow_account_impersonation=True,
                        use_google_auth=False):
  """Load credentials and force a refresh.

    Will always refresh loaded credential if it is expired or would expire
    within min_expiry_duration.

  Args:
    account: str, The account address for the credentials being fetched. If
      None, the account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES are
      requested.
    min_expiry_duration: Duration str, Refresh the credentials if they are
      within this duration from expiration. Must be a valid duration between 0
      seconds and 1 hour (e.g. '0s' >x< '1h').
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured). If False, the active user
      credentials will always be loaded.
    use_google_auth: bool, True to load credentials as google-auth credentials.
      False to load credentials as oauth2client credentials..

  Returns:
    oauth2client.client.Credentials or google.auth.credentials.Credentials.
    When all of the following conditions are met, it returns
    google.auth.credentials.Credentials and otherwise it returns
    oauth2client.client.Credentials.

    * use_google_auth is True
    * google-auth is not globally disabled by auth/disable_load_google_auth.

  Raises:
    NoActiveAccountException: If account is not provided and there is no
        active account.
    NoCredentialsForAccountException: If there are no valid credentials
        available for the provided or active account.
    c_gce.CannotConnectToMetadataServerException: If the metadata server cannot
        be reached.
    TokenRefreshError: If the credentials fail to refresh.
    TokenRefreshReauthError: If the credentials fail to refresh due to reauth.
    AccountImpersonationError: If impersonation is requested but an
      impersonation provider is not configured.
   ValueError:
  """
  cred = Load(
      account=account,
      scopes=scopes,
      allow_account_impersonation=allow_account_impersonation,
      use_google_auth=use_google_auth)
  RefreshIfExpireWithinWindow(cred, min_expiry_duration)

  return cred


def LoadIfEnabled(allow_account_impersonation=True, use_google_auth=False):
  """Get the credentials associated with the current account.

  If credentials have been disabled via properties, this will return None.
  Otherwise it will load credentials like normal. If credential loading fails
  for any reason (including the user not being logged in), the usual exception
  is raised.

  Args:
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured). If False, the active user
      credentials will always be loaded.
    use_google_auth: bool, True to load credentials as google-auth credentials.
    False to load credentials as oauth2client credentials..

  Returns:
    oauth2client.client.Credentials or google.auth.credentials.Credentials if
    credentials are enabled. When all of the following conditions are met, it
    returns google.auth.credentials.Credentials and otherwise it returns
    oauth2client.client.Credentials.

    * use_google_auth is True
    * google-auth is not globally disabled by auth/disable_load_google_auth.

  Raises:
    NoActiveAccountException: If account is not provided and there is no
        active account.
    c_gce.CannotConnectToMetadataServerException: If the metadata server cannot
        be reached.
    TokenRefreshError: If the credentials fail to refresh.
    TokenRefreshReauthError: If the credentials fail to refresh due to reauth.
  """
  if properties.VALUES.auth.disable_credentials.GetBool():
    return None
  return Load(
      allow_account_impersonation=allow_account_impersonation,
      use_google_auth=use_google_auth)


def Load(account=None,
         scopes=None,
         prevent_refresh=False,
         allow_account_impersonation=True,
         use_google_auth=False):
  """Get the credentials associated with the provided account.

  This loads credentials regardless of whether credentials have been disabled
  via properties. Only use this when the functionality of the caller absolutely
  requires credentials (like printing out a token) vs logically requiring
  credentials (like for an http request).

  Credential information may come from the stored credential file (representing
  the last gcloud auth command), or the credential cache (representing the last
  time the credentials were refreshed). If they come from the cache, the
  token_response field will be None, as the full server response from the cached
  request was not stored.

  Args:
    account: str, The account address for the credentials being fetched. If
        None, the account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES
        are requested.
    prevent_refresh: bool, If True, do not refresh the access token even if it
        is out of date. (For use with operations that do not require a current
        access token, such as credential revocation.)
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials (if that is configured). If False, the active user
      credentials will always be loaded.
    use_google_auth: bool, True to load credentials as google-auth credentials.
    False to load credentials as oauth2client credentials..

  Returns:
    oauth2client.client.Credentials or google.auth.credentials.Credentials.
    When all of the following conditions are met, it returns
    google.auth.credentials.Credentials and otherwise it returns
    oauth2client.client.Credentials.

    * use_google_auth is True
    * google-auth is not globally disabled by auth/disable_load_google_auth.

  Raises:
    NoActiveAccountException: If account is not provided and there is no
        active account.
    NoCredentialsForAccountException: If there are no valid credentials
        available for the provided or active account.
    c_gce.CannotConnectToMetadataServerException: If the metadata server cannot
        be reached.
    TokenRefreshError: If the credentials fail to refresh.
    TokenRefreshReauthError: If the credentials fail to refresh due to reauth.
    AccountImpersonationError: If impersonation is requested but an
      impersonation provider is not configured.
  """
  use_google_auth = use_google_auth and (not GoogleAuthDisabledGlobally())

  impersonate_service_account = (
      properties.VALUES.auth.impersonate_service_account.Get())
  if allow_account_impersonation and impersonate_service_account:
    if not IMPERSONATION_TOKEN_PROVIDER:
      raise AccountImpersonationError(
          'gcloud is configured to impersonate service account [{}] but '
          'impersonation support is not available.'.format(
              impersonate_service_account))
    log.warning(
        'This command is using service account impersonation. All API calls will '
        'be executed as [{}].'.format(impersonate_service_account))

    if use_google_auth:
      google_auth_source_creds = Load(
          account=account,
          allow_account_impersonation=False,
          use_google_auth=use_google_auth)
      cred = IMPERSONATION_TOKEN_PROVIDER.GetElevationAccessTokenGoogleAuth(
          google_auth_source_creds, impersonate_service_account, scopes or
          config.CLOUDSDK_SCOPES)
    else:
      cred = IMPERSONATION_TOKEN_PROVIDER.GetElevationAccessToken(
          impersonate_service_account, scopes or config.CLOUDSDK_SCOPES)
  else:
    cred = _Load(account, scopes, prevent_refresh, use_google_auth)

  return cred


def _LoadFromFileOverride(cred_file_override, scopes, use_google_auth):
  """Load credentials from cred file override."""
  log.info('Using alternate credentials from file: [%s]', cred_file_override)

  if not use_google_auth:
    try:
      cred = client.GoogleCredentials.from_stream(cred_file_override)
    except client.Error as e:
      raise InvalidCredentialFileException(cred_file_override, e)

    if cred.create_scoped_required():
      if scopes is None:
        scopes = config.CLOUDSDK_SCOPES
      cred = cred.create_scoped(scopes)

    # Set token_uri after scopes since token_uri needs to be explicitly
    # preserved when scopes are applied.
    token_uri_override = properties.VALUES.auth.token_host.Get()
    if token_uri_override:
      cred_type = c_creds.CredentialType.FromCredentials(cred)
      if cred_type in (c_creds.CredentialType.SERVICE_ACCOUNT,
                       c_creds.CredentialType.P12_SERVICE_ACCOUNT):
        cred.token_uri = token_uri_override
    # The credential override is not stored in credential store, but we still
    # want to cache access tokens between invocations.
    cred = c_creds.MaybeAttachAccessTokenCacheStore(cred)
  else:
    # Import only when necessary to decrease the startup time. Move it to
    # global once google-auth is ready to replace oauth2client.
    # Ideally we should wrap the following two lines inside a pylint disable and
    # enable block just like other places, but it seems it is not working here.
    from google.auth import _default as google_auth_default  # pylint: disable=g-import-not-at-top
    from google.auth import credentials as google_auth_creds  # pylint: disable=g-import-not-at-top

    try:
      # pylint: disable=protected-access
      cred, _ = google_auth_default.load_credentials_from_file(
          cred_file_override)
      # pylint: enable=protected-access
    except google_auth_exceptions.DefaultCredentialsError as e:
      raise InvalidCredentialFileException(cred_file_override, e)

    if scopes is None:
      scopes = config.CLOUDSDK_SCOPES
    cred = google_auth_creds.with_scopes_if_required(cred, scopes)

    # Set token_uri after scopes since token_uri needs to be explicitly
    # preserved when scopes are applied.
    token_uri_override = properties.VALUES.auth.token_host.Get()
    if token_uri_override:
      cred_type = c_creds.CredentialTypeGoogleAuth.FromCredentials(cred)
      if cred_type == c_creds.CredentialTypeGoogleAuth.SERVICE_ACCOUNT:
        # pylint: disable=protected-access
        cred._token_uri = token_uri_override
        # pylint: enable=protected-access

    # The credential override is not stored in credential store, but we still
    # want to cache access tokens between invocations.
    cred = c_creds.MaybeAttachAccessTokenCacheStoreGoogleAuth(cred)

  return cred


def _Load(account, scopes, prevent_refresh, use_google_auth=False):
  """Helper for Load()."""
  # If a credential file is set, just use that and ignore the active account
  # and whatever is in the credential store.
  cred_file_override = properties.VALUES.auth.credential_file_override.Get()

  if cred_file_override:
    cred = _LoadFromFileOverride(cred_file_override, scopes, use_google_auth)
  else:
    if not account:
      account = properties.VALUES.core.account.Get()

    if not account:
      raise NoActiveAccountException(
          named_configs.ActiveConfig(False).file_path)

    cred = STATIC_CREDENTIAL_PROVIDERS.GetCredentials(account, use_google_auth)
    if cred is not None:
      return cred

    store = c_creds.GetCredentialStore()
    cred = store.Load(account, use_google_auth)
    if not cred:
      raise NoCredentialsForAccountException(account)

  if not prevent_refresh:
    RefreshIfAlmostExpire(cred)

  if use_google_auth and c_creds.IsOauth2clientP12AccountCredentials(cred):
    cred = _CreateP12GoogleAuth(cred)

  return cred


def _CreateP12GoogleAuth(credentials):
  Refresh(credentials)
  p12_cred = c_creds.P12CredentialsGoogleAuth()
  p12_cred.token = credentials.access_token
  p12_cred.expiry = credentials.token_expiry
  try:
    p12_cred.id_tokenb64 = credentials.id_tokenb64
  except AttributeError:
    pass
  return p12_cred


def Refresh(credentials,
            http_client=None,
            is_impersonated_credential=False,
            include_email=False,
            gce_token_format='standard',
            gce_include_license=False):
  """Refresh credentials.

  Calls credentials.refresh(), unless they're SignedJwtAssertionCredentials.
  If the credentials correspond to a service account or impersonated credentials
  issue an additional request to generate a fresh id_token.

  Args:
    credentials: oauth2client.client.Credentials or
      google.auth.credentials.Credentials, The credentials to refresh.
    http_client: httplib2.Http, The http transport to refresh with.
    is_impersonated_credential: bool, True treat provided credential as an
      impersonated service account credential. If False, treat as service
      account or user credential. Needed to avoid circular dependency on
      IMPERSONATION_TOKEN_PROVIDER.
    include_email: bool, Specifies whether or not the service account email is
      included in the identity token. Only applicable to impersonated service
      account.
    gce_token_format: str, Specifies whether or not the project and instance
      details are included in the identity token. Choices are "standard",
      "full".
    gce_include_license: bool, Specifies whether or not license codes for images
      associated with GCE instance are included in their identity tokens.

  Raises:
    TokenRefreshError: If the credentials fail to refresh.
    TokenRefreshReauthError: If the credentials fail to refresh due to reauth.
  """
  if c_creds.IsOauth2ClientCredentials(credentials):
    _Refresh(credentials, http_client, is_impersonated_credential,
             include_email, gce_token_format, gce_include_license)
  else:
    _RefreshGoogleAuth(credentials, http_client, is_impersonated_credential,
                       include_email, gce_token_format, gce_include_license)


def _Refresh(credentials,
             http_client,
             is_impersonated_credential=False,
             include_email=False,
             gce_token_format='standard',
             gce_include_license=False):
  """Refreshes oauth2client credentials."""
  http_client = http_client or http.Http(response_encoding=transport.ENCODING)
  try:
    credentials.refresh(http_client)
    id_token = None
    # Service accounts require an additional request to receive a fresh id_token
    if is_impersonated_credential:
      if not IMPERSONATION_TOKEN_PROVIDER:
        raise AccountImpersonationError(
            'gcloud is configured to impersonate a service account but '
            'impersonation support is not available.')
      if not IMPERSONATION_TOKEN_PROVIDER.IsImpersonationCredential(
          credentials):
        raise AccountImpersonationError(
            'Invalid impersonation account for refresh {}'.format(credentials))
      id_token = _RefreshImpersonatedAccountIdToken(
          credentials, include_email=include_email)
    # Service accounts require an additional request to receive a fresh id_token
    elif isinstance(credentials, service_account.ServiceAccountCredentials):
      id_token = _RefreshServiceAccountIdToken(credentials, http_client)
    elif isinstance(credentials, oauth2client_gce.AppAssertionCredentials):
      id_token = c_gce.Metadata().GetIdToken(
          config.CLOUDSDK_CLIENT_ID,
          token_format=gce_token_format,
          include_license=gce_include_license)

    if id_token:
      if credentials.token_response:
        credentials.token_response['id_token'] = id_token
      credentials.id_tokenb64 = id_token

  except (client.AccessTokenRefreshError, httplib2.ServerNotFoundError) as e:
    if IsContextAwareAccessDeniedError(e):
      raise TokenRefreshDeniedByCAAError(e)
    raise TokenRefreshError(six.text_type(e))
  except reauth_errors.ReauthSamlLoginRequiredError:
    raise WebLoginRequiredReauthError()
  except reauth_errors.ReauthError as e:
    raise TokenRefreshReauthError(str(e))


@contextlib.contextmanager
def HandleGoogleAuthCredentialsRefreshError(for_adc=False):
  """Handles exceptions during refreshing google auth credentials."""
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  from googlecloudsdk.core.credentials import google_auth_credentials as c_google_auth
  # pylint: enable=g-import-not-at-top
  try:
    yield
  except reauth_errors.ReauthSamlLoginRequiredError:
    raise WebLoginRequiredReauthError(for_adc=for_adc)
  except (reauth_errors.ReauthError,
          c_google_auth.ReauthRequiredError) as e:
    raise TokenRefreshReauthError(str(e), for_adc=for_adc)
  except google_auth_exceptions.RefreshError as e:
    if IsContextAwareAccessDeniedError(e):
      raise TokenRefreshDeniedByCAAError(e)
    raise TokenRefreshError(six.text_type(e), for_adc=for_adc)


def _RefreshGoogleAuth(credentials,
                       http_client=None,
                       is_impersonated_credential=False,
                       include_email=False,
                       gce_token_format='standard',
                       gce_include_license=False):
  """Refreshes google-auth credentials.

  Args:
    credentials: google.auth.credentials.Credentials, A google-auth credentials
      to refresh.
    http_client: httplib2.Http, The http transport to refresh
      with.
    is_impersonated_credential: bool, True treat provided credential as an
      impersonated service account credential. If False, treat as service
      account or user credential. Needed to avoid circular dependency on
      IMPERSONATION_TOKEN_PROVIDER.
    include_email: bool, Specifies whether or not the service account email is
      included in the identity token. Only applicable to impersonated service
      account.
    gce_token_format: str, Specifies whether or not the project and instance
      details are included in the identity token. Choices are "standard",
      "full".
    gce_include_license: bool, Specifies whether or not license codes for images
      associated with GCE instance are included in their identity tokens.

  Raises:
    AccountImpersonationError: if impersonation support is not available for
      gcloud, or if the provided credentials is not google auth impersonation
      credentials.
  """
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  from google.oauth2 import service_account as google_auth_service_account
  # pylint: enable=g-import-not-at-top
  request_client = http.GoogleAuthRequest(http_client)
  with HandleGoogleAuthCredentialsRefreshError():
    credentials.refresh(request_client)

    id_token = None
    if is_impersonated_credential:
      if not IMPERSONATION_TOKEN_PROVIDER:
        raise AccountImpersonationError(
            'gcloud is configured to impersonate a service account but '
            'impersonation support is not available.')

      # Import only when necessary to decrease the startup time. Move it to
      # global once google-auth is ready to replace oauth2client.
      # pylint: disable=g-import-not-at-top
      import google.auth.impersonated_credentials as google_auth_impersonated_creds
      # pylint: enable=g-import-not-at-top
      if not isinstance(credentials,
                        google_auth_impersonated_creds.Credentials):
        raise AccountImpersonationError(
            'Invalid impersonation account for refresh {}'.format(credentials))
      id_token_creds = IMPERSONATION_TOKEN_PROVIDER.GetElevationIdTokenGoogleAuth(
          credentials, config.CLOUDSDK_CLIENT_ID, include_email)
      id_token_creds.refresh(request_client)
      id_token = id_token_creds.token
    elif isinstance(credentials, google_auth_service_account.Credentials):
      id_token = _RefreshServiceAccountIdTokenGoogleAuth(
          credentials, request_client)
    elif isinstance(credentials, google_auth_gce.Credentials):
      id_token = c_gce.Metadata().GetIdToken(
          config.CLOUDSDK_CLIENT_ID,
          token_format=gce_token_format,
          include_license=gce_include_license)

    if id_token:
      # '_id_token' is the field supported in google-auth natively. gcloud
      # keeps an additional field 'id_tokenb64' to store this information
      # which is referenced in several places
      credentials._id_token = id_token  # pylint: disable=protected-access
      credentials.id_tokenb64 = id_token


def RefreshIfExpireWithinWindow(credentials, window):
  """Refreshes credentials if they will expire within a time window.

  Args:
    credentials: google.auth.credentials.Credentials or
      client.OAuth2Credentials, the credentials to refresh.
    window: string, The threshold of the remaining lifetime of the token which
      can trigger the refresh.
  """
  if c_creds.IsOauth2ClientCredentials(credentials):
    expiry = credentials.token_expiry
  else:
    expiry = credentials.expiry
  almost_expire = (not expiry) or _TokenExpiresWithinWindow(window, expiry)

  if almost_expire:
    Refresh(credentials)


def RefreshIfAlmostExpire(credentials):
  RefreshIfExpireWithinWindow(credentials, window=_CREDENTIALS_EXPIRY_WINDOW)


def _RefreshImpersonatedAccountIdToken(cred, include_email):
  """Get a fresh id_token for the given impersonated service account."""
  # pylint: disable=protected-access
  service_account_email = cred._service_account_id
  return IMPERSONATION_TOKEN_PROVIDER.GetElevationIdToken(
      service_account_email, config.CLOUDSDK_CLIENT_ID, include_email)
  # pylint: enable=protected-access


def _RefreshServiceAccountIdToken(cred, http_client):
  """Get a fresh id_token for the given oauth2client credentials.

  Args:
    cred: service_account.ServiceAccountCredentials, the credentials for which
      to refresh the id_token.
    http_client: httplib2.Http, the http transport to refresh with.

  Returns:
    str, The id_token if refresh was successful. Otherwise None.
  """
  http_request = http_client.request

  now = int(time.time())
  # pylint: disable=protected-access
  payload = {
      'aud': cred.token_uri,
      'iat': now,
      'exp': now + cred.MAX_TOKEN_LIFETIME_SECS,
      'iss': cred._service_account_email,
      'target_audience': config.CLOUDSDK_CLIENT_ID,
  }
  assertion = crypt.make_signed_jwt(
      cred._signer, payload, key_id=cred._private_key_id)

  body = urllib.parse.urlencode({
      'assertion': assertion,
      'grant_type': _GRANT_TYPE,
  })

  resp, content = http_request(
      cred.token_uri.encode('idna'), method='POST', body=body,
      headers=cred._generate_refresh_request_headers())
  # pylint: enable=protected-access
  if resp.status == 200:
    d = json.loads(content)
    return d.get('id_token', None)
  else:
    return None


def _RefreshServiceAccountIdTokenGoogleAuth(cred, request_client):
  """Get a fresh id_token for the given google-auth credentials.

  Args:
    cred: google.oauth2.service_account.Credentials, the credentials for which
      to refresh the id_token.
    request_client: google.auth.transport.Request, the http transport
     to refresh with.

  Returns:
    str, The id_token if refresh was successful. Otherwise None.
  """
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  from google.oauth2 import service_account as google_auth_service_account
  # pylint: enable=g-import-not-at-top

  id_token_cred = google_auth_service_account.IDTokenCredentials(
      cred.signer,
      cred.service_account_email,
      cred._token_uri,  # pylint: disable=protected-access
      config.CLOUDSDK_CLIENT_ID)

  try:
    id_token_cred.refresh(request_client)
  except google_auth_exceptions.RefreshError:
    # ID token refresh does not work in testgaia because the Cloud SDK
    # client ID (http://shortn/_BVYwsLLdaJ) is not set up to work with this
    # environment. The running command should not break because of this and
    # should proceed without a new ID token.
    return None

  return id_token_cred.token


def Store(credentials, account=None, scopes=None):
  """Store credentials according for an account address.

  gcloud only stores user account credentials, service account credentials and
  p12 service account credentials. GCE, IAM impersonation, and Devshell
  credentials are generated in runtime.

  Args:
    credentials: oauth2client.client.Credentials or
      google.auth.credentials.Credentials, The credentials to be stored.
    account: str, The account address of the account they're being stored for.
        If None, the account stored in the core.account property is used.
    scopes: tuple, Custom auth scopes to request. By default CLOUDSDK_SCOPES
        are requested.

  Raises:
    NoActiveAccountException: If account is not provided and there is no
        active account.
  """

  if c_creds.IsOauth2ClientCredentials(credentials):
    cred_type = c_creds.CredentialType.FromCredentials(credentials)
  else:
    cred_type = c_creds.CredentialTypeGoogleAuth.FromCredentials(credentials)

  if not cred_type.is_serializable:
    return

  if not account:
    account = properties.VALUES.core.account.Get()
  if not account:
    raise NoActiveAccountException()

  store = c_creds.GetCredentialStore()
  store.Store(account, credentials)

  _LegacyGenerator(account, credentials, scopes).WriteTemplate()


def ActivateCredentials(account, credentials):
  """Validates, stores and activates credentials with given account."""
  Refresh(credentials)
  Store(credentials, account)

  properties.PersistProperty(properties.VALUES.core.account, account)


def RevokeCredentials(credentials):
  """Revokes the token on the server.

  Args:
    credentials: user account credentials from either google-auth or
      oauth2client.
  Raises:
    RevokeError: If credentials to revoke is not user account credentials.
  """
  if not c_creds.IsUserAccountCredentials(credentials):
    raise RevokeError('The token cannot be revoked from server because it is '
                      'not user account credentials.')
  http_client = http.Http()
  if c_creds.IsOauth2ClientCredentials(credentials):
    credentials.revoke(http_client)
  else:
    credentials.revoke(http.GoogleAuthRequest(http_client))


def Revoke(account=None):
  """Revoke credentials and clean up related files.

  Args:
    account: str, The account address for the credentials to be revoked. If
        None, the currently active account is used.

  Returns:
    True if this call revoked the account; False if the account was already
    revoked.

  Raises:
    NoActiveAccountException: If account is not provided and there is no
        active account.
    NoCredentialsForAccountException: If the provided account is not tied to any
        known credentials.
    RevokeError: If there was a more general problem revoking the account.
  """
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  from googlecloudsdk.core.credentials import google_auth_credentials as c_google_auth
  # pylint: enable=g-import-not-at-top
  if not account:
    account = properties.VALUES.core.account.Get()
  if not account:
    raise NoActiveAccountException()

  if account in c_gce.Metadata().Accounts():
    raise RevokeError('Cannot revoke GCE-provided credentials.')

  credentials = Load(
      account, prevent_refresh=True, use_google_auth=True)
  if not credentials:
    raise NoCredentialsForAccountException(account)

  if (isinstance(credentials, c_devshell.DevshellCredentials) or
      isinstance(credentials, c_devshell.DevShellCredentialsGoogleAuth)):
    raise RevokeError(
        'Cannot revoke the automatically provisioned Cloud Shell credential.'
        'This comes from your browser session and will not persist outside'
        'of your connected Cloud Shell session.')

  rv = False
  try:
    if not account.endswith('.gserviceaccount.com'):
      RevokeCredentials(credentials)
      rv = True
  except (client.TokenRevokeError, c_google_auth.TokenRevokeError) as e:
    if e.args[0] == 'invalid_token':
      # Malformed or already revoked
      pass
    elif e.args[0] == 'invalid_request':
      # Service account token
      pass
    else:
      raise

  store = c_creds.GetCredentialStore()
  store.Remove(account)

  _LegacyGenerator(account, credentials).Clean()
  legacy_creds_dir = config.Paths().LegacyCredentialsDir(account)
  if os.path.isdir(legacy_creds_dir):
    files.RmTree(legacy_creds_dir)
  return rv


def AcquireFromWebFlow(launch_browser=True,
                       auth_uri=None,
                       token_uri=None,
                       scopes=None,
                       client_id=None,
                       client_secret=None):
  """Get credentials via a web flow.

  Args:
    launch_browser: bool, Open a new web browser window for authorization.
    auth_uri: str, URI to open for authorization.
    token_uri: str, URI to use for refreshing.
    scopes: string or iterable of strings, scope(s) of the credentials being
      requested.
    client_id: str, id of the client requesting authorization
    client_secret: str, client secret of the client requesting authorization

  Returns:
    client.Credentials, Newly acquired credentials from the web flow.

  Raises:
    FlowError: If there is a problem with the web flow.
  """
  if auth_uri is None:
    auth_uri = properties.VALUES.auth.auth_host.Get(required=True)
  if token_uri is None:
    token_uri = properties.VALUES.auth.token_host.Get(required=True)
  if scopes is None:
    scopes = config.CLOUDSDK_SCOPES
  if client_id is None:
    client_id = properties.VALUES.auth.client_id.Get(required=True)
  if client_secret is None:
    client_secret = properties.VALUES.auth.client_secret.Get(required=True)

  webflow = client.OAuth2WebServerFlow(
      client_id=client_id,
      client_secret=client_secret,
      scope=scopes,
      user_agent=config.CLOUDSDK_USER_AGENT,
      auth_uri=auth_uri,
      token_uri=token_uri,
      pkce=True,
      prompt='select_account')
  return RunWebFlow(webflow, launch_browser=launch_browser)


def RunWebFlow(webflow, launch_browser=True):
  """Runs a preconfigured webflow to get an auth token.

  Args:
    webflow: client.OAuth2WebServerFlow, The configured flow to run.
    launch_browser: bool, Open a new web browser window for authorization.

  Returns:
    client.Credentials, Newly acquired credentials from the web flow.

  Raises:
    FlowError: If there is a problem with the web flow.
  """
  # pylint:disable=g-import-not-at-top, This is imported on demand for
  # performance reasons.
  from googlecloudsdk.core.credentials import flow

  try:
    cred = flow.Run(webflow, launch_browser=launch_browser, http=http.Http())
  except flow.Error as e:
    raise FlowError(e)
  return cred


def AcquireFromToken(refresh_token,
                     token_uri=GOOGLE_OAUTH2_PROVIDER_TOKEN_URI,
                     revoke_uri=GOOGLE_OAUTH2_PROVIDER_REVOKE_URI,
                     use_google_auth=False):
  """Get credentials from an already-valid refresh token.

  Args:
    refresh_token: An oauth2 refresh token.
    token_uri: str, URI to use for refreshing.
    revoke_uri: str, URI to use for revoking.
    use_google_auth: bool, True to return google-auth credentials. False to
    return oauth2client credentials..

  Returns:
    oauth2client.client.Credentials or google.auth.credentials.Credentials.
    When all of the following conditions are true, it returns
    google.auth.credentials.Credentials and otherwise it returns
    oauth2client.client.Credentials.

    * use_google_auth=True
    * google-auth is not globally disabled by auth/disable_load_google_auth.
  """
  use_google_auth = use_google_auth and (not GoogleAuthDisabledGlobally())
  if use_google_auth:
    # Import only when necessary to decrease the startup time. Move it to
    # global once google-auth is ready to replace oauth2client.
    # pylint: disable=g-import-not-at-top
    from google.oauth2 import credentials as google_auth_creds
    # pylint: enable=g-import-not-at-top
    cred = google_auth_creds.Credentials(
        token=None,
        refresh_token=refresh_token,
        id_token=None,
        token_uri=token_uri,
        client_id=properties.VALUES.auth.client_id.Get(required=True),
        client_secret=properties.VALUES.auth.client_secret.Get(required=True))

    # always start expired
    cred.expiry = datetime.datetime.utcnow()
  else:
    cred = client.OAuth2Credentials(
        access_token=None,
        client_id=properties.VALUES.auth.client_id.Get(required=True),
        client_secret=properties.VALUES.auth.client_secret.Get(required=True),
        refresh_token=refresh_token,
        # always start expired
        token_expiry=datetime.datetime.utcnow(),
        token_uri=token_uri,
        user_agent=config.CLOUDSDK_USER_AGENT,
        revoke_uri=revoke_uri)
  return cred


def AcquireFromGCE(account=None, use_google_auth=False):
  """Get credentials from a GCE metadata server.

  Args:
    account: str, The account name to use. If none, the default is used.
    use_google_auth: bool, True to load credentials of google-auth if it is
      supported in the current authentication scenario. False to load
      credentials of oauth2client.

  Returns:
    oauth2client.client.Credentials or google.auth.credentials.Credentials based
    on use_google_auth and whether google-auth is supported in the current
    authentication sceanrio.

  Raises:
    c_gce.CannotConnectToMetadataServerException: If the metadata server cannot
      be reached.
    TokenRefreshError: If the credentials fail to refresh.
    TokenRefreshReauthError: If the credentials fail to refresh due to reauth.
  """
  if use_google_auth:
    email = account or 'default'
    credentials = google_auth_gce.Credentials(service_account_email=email)
  else:
    credentials = oauth2client_gce.AppAssertionCredentials(email=account)
  Refresh(credentials)
  return credentials


class _LegacyGenerator(object):
  """A class to generate the credential file for other tools, like gsutil & bq.

  The supported credentials types are user account credentials, service account
  credentials, and p12 service account credentials. Gcloud supports two auth
  libraries - oauth2client and google-auth. Eventually, we will deprecate
  oauth2client.
  """

  def __init__(self, account, credentials, scopes=None):
    self.credentials = credentials
    if self._cred_type not in (c_creds.USER_ACCOUNT_CREDS_NAME,
                               c_creds.SERVICE_ACCOUNT_CREDS_NAME,
                               c_creds.P12_SERVICE_ACCOUNT_CREDS_NAME):
      raise c_creds.CredentialFileSaveError(
          'Unsupported credentials type {0}'.format(type(self.credentials)))
    if scopes is None:
      self.scopes = config.CLOUDSDK_SCOPES
    else:
      self.scopes = scopes

    paths = config.Paths()
    # Bq file is not generated here. bq CLI generates it using the adc at
    # self._adc_path and uses it as the cache.
    # Register so it is cleaned up.
    self._bq_path = paths.LegacyCredentialsBqPath(account)
    self._gsutil_path = paths.LegacyCredentialsGSUtilPath(account)
    self._p12_key_path = paths.LegacyCredentialsP12KeyPath(account)
    self._adc_path = paths.LegacyCredentialsAdcPath(account)

  @property
  def _is_oauth2client(self):
    return c_creds.IsOauth2ClientCredentials(self.credentials)

  @property
  def _cred_type(self):
    if self._is_oauth2client:
      return c_creds.CredentialType.FromCredentials(self.credentials).key
    else:
      return c_creds.CredentialTypeGoogleAuth.FromCredentials(
          self.credentials).key

  def Clean(self):
    """Remove the credential file."""

    paths = [
        self._bq_path,
        self._gsutil_path,
        self._p12_key_path,
        self._adc_path,
    ]
    for p in paths:
      try:
        os.remove(p)
      except OSError:
        # file did not exist, so we're already done.
        pass

  def WriteTemplate(self):
    """Write the credential file."""

    # Remove all the credential files first. As per the comment in __init__
    # the BQ file (singlestore_bq) is created by the BQ CLI and not
    # regenerated. This file should be removed when the credentials are
    # created. If this file isn't removed, it will be stale, but BQ CLI will
    # try to use it anyways. The rest of the credential files should be
    # recreated here.
    self.Clean()

    # Generates credentials used by bq and gsutil.
    if self._cred_type == c_creds.P12_SERVICE_ACCOUNT_CREDS_NAME:
      cred = self.credentials
      key = cred._private_key_pkcs12  # pylint: disable=protected-access
      password = cred._private_key_password  # pylint: disable=protected-access
      files.WriteBinaryFileContents(self._p12_key_path, key, private=True)

      # the .boto file gets some different fields
      self._WriteFileContents(
          self._gsutil_path, '\n'.join([
              '[Credentials]',
              'gs_service_client_id = {account}',
              'gs_service_key_file = {key_file}',
              'gs_service_key_file_password = {key_password}',
          ]).format(account=self.credentials.service_account_email,
                    key_file=self._p12_key_path,
                    key_password=password))
      return
    c_creds.ADC(self.credentials).DumpADCToFile(file_path=self._adc_path)

    if self._cred_type == c_creds.USER_ACCOUNT_CREDS_NAME:
      # We create a small .boto file for gsutil, to be put in BOTO_PATH.
      # Our client_id and client_secret should accompany our refresh token;
      # if a user loaded any other .boto files that specified a different
      # id and secret, those would override our id and secret, causing any
      # attempts to obtain an access token with our refresh token to fail.
      self._WriteFileContents(
          self._gsutil_path, '\n'.join([
              '[OAuth2]',
              'client_id = {cid}',
              'client_secret = {secret}',
              '',
              '[Credentials]',
              'gs_oauth2_refresh_token = {token}',
          ]).format(cid=config.CLOUDSDK_CLIENT_ID,
                    secret=config.CLOUDSDK_CLIENT_NOTSOSECRET,
                    token=self.credentials.refresh_token))
    else:
      self._WriteFileContents(
          self._gsutil_path, '\n'.join([
              '[Credentials]',
              'gs_service_key_file = {key_file}',
          ]).format(key_file=self._adc_path))

  def _WriteFileContents(self, filepath, contents):
    """Writes contents to a path, ensuring mkdirs.

    Args:
      filepath: str, The path of the file to write.
      contents: str, The contents to write to the file.
    """

    full_path = os.path.realpath(files.ExpandHomeDir(filepath))
    files.WriteFileContents(full_path, contents, private=True)
