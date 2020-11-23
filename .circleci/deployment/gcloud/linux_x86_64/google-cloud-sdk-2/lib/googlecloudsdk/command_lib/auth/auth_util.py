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

"""Support library for the auth command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import os
import textwrap

from googlecloudsdk.api_lib.cloudresourcemanager import projects_api
from googlecloudsdk.api_lib.iamcredentials import util as impersonation_util
from googlecloudsdk.calliope import exceptions as c_exc
from googlecloudsdk.command_lib.projects import util as project_util
from googlecloudsdk.core import config
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.credentials import creds as c_creds
from googlecloudsdk.core.credentials import store as c_store
from googlecloudsdk.core.util import encoding
from googlecloudsdk.core.util import files

from oauth2client import client
from oauth2client import service_account
from oauth2client.contrib import gce as oauth2client_gce


_IS_GOOGLE_DOMAIN_ENV_VAR = 'CLOUDSDK_GOOGLE_AUTH_IS_GOOGLE_DOMAIN'
SERVICEUSAGE_PERMISSION = 'serviceusage.services.use'

ACCOUNT_TABLE_FORMAT = ("""\
    table[title='Credentialed Accounts'](
        status.yesno(yes='*', no=''):label=ACTIVE,
        account
    )""")


class MissingPermissionOnQuotaProjectError(c_creds.ADCError):
  """An error when ADC does not have permission to bill a quota project."""


class _AcctInfo(object):
  """An auth command resource list item.

  Attributes:
    account: The account name.
    status: The account status, one of ['ACTIVE', ''].
  """

  def __init__(self, account, active):
    self.account = account
    self.status = 'ACTIVE' if active else ''


def AllAccounts():
  """The resource list return value for the auth command Run() method."""
  active_account = properties.VALUES.core.account.Get()
  return [_AcctInfo(account, account == active_account)
          for account in c_store.AvailableAccounts()]


def IsGceAccountCredentials(cred):
  """Checks if the credential is a Compute Engine service account credential."""
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  import google.auth.compute_engine as google_auth_gce

  return (isinstance(cred, oauth2client_gce.AppAssertionCredentials) or
          isinstance(cred, google_auth_gce.credentials.Credentials))


def IsServiceAccountCredential(cred):
  """Checks if the credential is a service account credential."""
  # Import only when necessary to decrease the startup time. Move it to
  # global once google-auth is ready to replace oauth2client.
  # pylint: disable=g-import-not-at-top
  import google.oauth2.service_account as google_auth_service_account

  return (isinstance(cred, service_account.ServiceAccountCredentials) or
          isinstance(cred, google_auth_service_account.Credentials))


def IsImpersonationCredential(cred):
  """Checks if the credential is an impersonated service account credential."""
  return (impersonation_util.
          ImpersonationAccessTokenProvider.IsImpersonationCredential(cred))


def ValidIdTokenCredential(cred):
  return (IsImpersonationCredential(cred) or
          IsServiceAccountCredential(cred) or
          IsGceAccountCredentials(cred))


def PromptIfADCEnvVarIsSet():
  """Warns users if ADC environment variable is set."""
  override_file = config.ADCEnvVariable()
  if override_file:
    message = textwrap.dedent("""
          The environment variable [{envvar}] is set to:
            [{override_file}]
          Credentials will still be generated to the default location:
            [{default_file}]
          To use these credentials, unset this environment variable before
          running your application.
          """.format(
              envvar=client.GOOGLE_APPLICATION_CREDENTIALS,
              override_file=override_file,
              default_file=config.ADCFilePath()))
    console_io.PromptContinue(
        message=message, throw_if_unattended=True, cancel_on_no=True)


def WriteGcloudCredentialsToADC(creds, add_quota_project=False):
  """Writes gclouds's credential from auth login to ADC json."""
  if not c_creds.IsUserAccountCredentials(creds):
    log.warning('Credentials cannot be written to application default '
                'credentials because it is not a user credential.')
    return

  PromptIfADCEnvVarIsSet()
  if add_quota_project:
    c_creds.ADC(creds).DumpExtendedADCToFile()
  else:
    c_creds.ADC(creds).DumpADCToFile()


def GetADCAsJson():
  """Reads ADC from disk and converts it to a json object."""
  if not os.path.isfile(config.ADCFilePath()):
    return None
  with files.FileReader(config.ADCFilePath()) as f:
    return json.load(f)


def GetQuotaProjectFromADC():
  """Reads the quota project ID from ADC json file and return it."""
  adc_json = GetADCAsJson()
  try:
    return adc_json['quota_project_id']
  except (KeyError, TypeError):
    return None


def AssertADCExists():
  adc_path = config.ADCFilePath()
  if not os.path.isfile(adc_path):
    raise c_exc.BadFileException(
        'Application default credentials have not been set up. '
        'Run $ gcloud auth application-default login to set it up first.')


def ADCIsUserAccount():
  cred_file = config.ADCFilePath()
  creds = client.GoogleCredentials.from_stream(cred_file)
  return creds.serialization_data['type'] == 'authorized_user'


def AdcHasGivenPermissionOnProject(project_id, permissions):
  AssertADCExists()
  project_ref = project_util.ParseProject(project_id)
  return _AdcHasGivenPermissionOnProjectHelper(project_ref, permissions)


def _AdcHasGivenPermissionOnProjectHelper(project_ref, permissions):
  cred_file_override_old = properties.VALUES.auth.credential_file_override.Get()
  try:
    properties.VALUES.auth.credential_file_override.Set(config.ADCFilePath())
    granted_permissions = projects_api.TestIamPermissions(
        project_ref, permissions).permissions
    return set(permissions) == set(granted_permissions)
  finally:
    properties.VALUES.auth.credential_file_override.Set(cred_file_override_old)


def LogADCIsWritten(adc_path):
  log.status.Print('\nCredentials saved to file: [{}]'.format(adc_path))
  log.status.Print(
      '\nThese credentials will be used by any library that requests '
      'Application Default Credentials (ADC).')


def LogQuotaProjectAdded(quota_project):
  log.status.Print(
      '\nQuota project "{}" was added to ADC which can be used by Google '
      'client libraries for billing and quota. Note that some services may '
      'still bill the project owning the resource.'.format(quota_project))


def LogQuotaProjectNotFound():
  log.warning('\nCannot find a quota project to add to ADC. You might receive '
              'a "quota exceeded" or "API not enabled" error. Run $ gcloud '
              'auth application-default set-quota-project to add '
              'a quota project.')


def LogMissingPermissionOnQuotaProject(quota_project):
  log.warning(
      '\nCannot add the project "{}" to ADC as the quota project because the '
      'account in ADC does not have the "{}" permission on this project. '
      'You might receive a "quota_exceeded" or "API not enabled" error. '
      'Run $ gcloud auth application-default set-quota-project to add a quota '
      'project.'.format(quota_project, SERVICEUSAGE_PERMISSION))


def LogQuotaProjectDisabled():
  log.warning(
      '\nQuota project is disabled. You might receive a "quota exceeded" or '
      '"API not enabled" error. Run $ gcloud auth application-default '
      'set-quota-project to add a quota project.')


# TODO(b/150405706): For rolling out risky changes of google-auth migration
# to Googlers first. Remove this function once the migration is done.
def IsHostGoogleDomain():
  """Whether the host on which gcloud runs is on Google domain."""
  return encoding.GetEncodedValue(os.environ,
                                  _IS_GOOGLE_DOMAIN_ENV_VAR) == 'true'


def DumpADC(credentials, quota_project_disabled=False):
  """Dumps the given credentials to ADC file.

  Args:
     credentials: a credentials from oauth2client or google-auth libraries, the
       credentials to dump.
     quota_project_disabled: bool, If quota project is explicitly disabled.
  """
  adc_path = c_creds.ADC(credentials).DumpADCToFile()
  LogADCIsWritten(adc_path)
  if quota_project_disabled:
    LogQuotaProjectDisabled()


def DumpADCOptionalQuotaProject(credentials):
  """Dumps the given credentials to ADC file with an optional quota project.

  Loads quota project from gcloud's context and writes it to application default
  credentials file if the credentials has the "serviceusage.services.use"
  permission on the quota project..

  Args:
     credentials: a credentials from oauth2client or google-auth libraries, the
       credentials to dump.
  """
  adc_path = c_creds.ADC(credentials).DumpADCToFile()
  LogADCIsWritten(adc_path)

  quota_project = c_creds.GetQuotaProject(
      credentials, force_resource_quota=True)
  if not quota_project:
    LogQuotaProjectNotFound()
  elif AdcHasGivenPermissionOnProject(
      quota_project, permissions=[SERVICEUSAGE_PERMISSION]):
    c_creds.ADC(credentials).DumpExtendedADCToFile(quota_project=quota_project)
    LogQuotaProjectAdded(quota_project)
  else:
    LogMissingPermissionOnQuotaProject(quota_project)


def AddQuotaProjectToADC(quota_project):
  """Adds the quota project to the existing ADC file.

  Quota project is only added to ADC when the credentials have the
  "serviceusage.services.use" permission on the project.

  Args:
    quota_project: str, The project id of a valid GCP project to add to ADC.

  Raises:
    MissingPermissionOnQuotaProjectError: If the credentials do not have the
      "serviceusage.services.use" permission.
  """
  AssertADCExists()
  if not ADCIsUserAccount():
    raise c_exc.BadFileException(
        'The application default credentials are not user credentials, quota '
        'project cannot be added.')
  if not AdcHasGivenPermissionOnProject(
      quota_project, permissions=[SERVICEUSAGE_PERMISSION]):
    raise MissingPermissionOnQuotaProjectError(
        'Cannot add the project "{}" to application default credentials (ADC) '
        'as a quota project because the account in ADC does not have the '
        '"{}" permission on this project.'.format(quota_project,
                                                  SERVICEUSAGE_PERMISSION))
  credentials = client.GoogleCredentials.from_stream(config.ADCFilePath())
  adc_path = c_creds.ADC(credentials).DumpExtendedADCToFile(
      quota_project=quota_project)
  LogADCIsWritten(adc_path)
  LogQuotaProjectAdded(quota_project)
