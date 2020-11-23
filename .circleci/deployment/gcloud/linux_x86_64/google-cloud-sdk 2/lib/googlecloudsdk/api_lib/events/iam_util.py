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
"""Provides util methods for iam operations."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from apitools.base.py import exceptions
from googlecloudsdk.api_lib.cloudresourcemanager import projects_api
from googlecloudsdk.api_lib.iam import util as iam_api_util
from googlecloudsdk.command_lib.iam import iam_util
from googlecloudsdk.command_lib.projects import util as projects_util
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io

# As an alternative to more fine-grained permissions, we allow service accounts
# with this role which should give it all necessary current and future
# permissions.
_OWNER_ROLE = 'roles/owner'

EVENTS_CONTROL_PLANE_SERVICE_ACCOUNT = 'cloud-run-events'
EVENTS_BROKER_SERVICE_ACCOUNT = 'cloud-run-events-broker'
EVENTS_SOURCES_SERVICE_ACCOUNT = 'cloud-run-events-sources'


def CreateServiceAccountKey(service_account_ref):
  """Creates and returns a new service account key."""
  iam_client, iam_messages = iam_api_util.GetClientAndMessages()
  key_request = iam_messages.CreateServiceAccountKeyRequest(
      privateKeyType=(
          iam_messages.CreateServiceAccountKeyRequest
          .PrivateKeyTypeValueValuesEnum.TYPE_GOOGLE_CREDENTIALS_FILE))
  return iam_client.projects_serviceAccounts_keys.Create(
      iam_messages.IamProjectsServiceAccountsKeysCreateRequest(
          name=service_account_ref.RelativeName(),
          createServiceAccountKeyRequest=key_request))


def _GetProjectRolesForServiceAccount(service_account_ref):
  """Returns the project roles the given service account is a member of."""
  project_ref = projects_util.ParseProject(properties.VALUES.core.project.Get())
  iam_policy = projects_api.GetIamPolicy(project_ref)

  roles = set()
  # iam_policy.bindings looks like:
  # list[<Binding
  #       members=['serviceAccount:member@thing.iam.gserviceaccount.com',...]
  #       role='roles/somerole'>...]
  for binding in iam_policy.bindings:
    if any(
        m.endswith(':' + service_account_ref.Name()) for m in binding.members):
      roles.add(binding.role)
  return roles


def _BindProjectRolesForServiceAccount(service_account_ref, roles):
  project_ref = projects_util.ParseProject(properties.VALUES.core.project.Get())
  member_str = 'serviceAccount:{}'.format(service_account_ref.Name())
  member_roles = [(member_str, role) for role in roles]
  projects_api.AddIamPolicyBindings(project_ref, member_roles)


def _CreateServiceAccount(account_name, display_name, description):
  """Creates a service account with the provided name and display name."""
  project_ref = projects_util.ParseProject(properties.VALUES.core.project.Get())

  client, messages = iam_api_util.GetClientAndMessages()
  result = client.projects_serviceAccounts.Create(
      messages.IamProjectsServiceAccountsCreateRequest(
          name=iam_util.ProjectToProjectResourceName(project_ref.Name()),
          createServiceAccountRequest=messages.CreateServiceAccountRequest(
              accountId=account_name,
              serviceAccount=messages.ServiceAccount(
                  displayName=display_name,
                  description=description))))
  log.CreatedResource(account_name, kind='service account')
  return result


def _GetServiceAccount(account_name):
  """Returns the service account with the specified name or None."""
  project_ref = projects_util.ParseProject(properties.VALUES.core.project.Get())
  resource_name = _ProjectAndAccountNameToResource(
      project_ref.Name(), account_name)

  client, messages = iam_api_util.GetClientAndMessages()
  try:
    return client.projects_serviceAccounts.Get(
        messages.IamProjectsServiceAccountsGetRequest(name=resource_name))
  except exceptions.HttpNotFoundError:
    return None


def _ProjectAndAccountNameToResource(project, account_name):
  # NOTE: It's important that this ref includes the project name rather than
  # a "-" even though the email includes the project. Using a "-" for project
  # results in an HTTP status code of 403 rather than 404 when getting a
  # non-existing service account ref.
  return 'projects/{}/serviceAccounts/{}'.format(
      project, _ProjectAndAccountNameToEmail(project, account_name))


def _ProjectAndAccountNameToEmail(project, account_name):
  return '{}@{}.iam.gserviceaccount.com'.format(account_name, project)


def GetOrCreateServiceAccountWithPrompt(account_name, display_name,
                                        description):
  """Returns or creates specified service account.

  Args:
    account_name: Name of the service account (portion before @)
    display_name: The display name to use when creating the account
    description: The description to use when creating the account
  """

  project_ref = projects_util.ParseProject(properties.VALUES.core.project.Get())
  sa_email = _ProjectAndAccountNameToEmail(project_ref.Name(), account_name)
  account = _GetServiceAccount(account_name)
  if account is not None:
    return account.email

  if console_io.CanPrompt():
    message = 'This will create service account [{}]'.format(sa_email)
    console_io.PromptContinue(message=message, cancel_on_no=True)

  return _CreateServiceAccount(account_name, display_name, description).email


def PrintOrBindMissingRolesWithPrompt(service_account_ref, recommended_roles,
                                      bind):
  """Binds any recommended project roles to the provided service account.

  If the service account has the owner role, no roles will be bound. If the bind
  argument is False, this function will only print out the missing recommended
  roles.

  This will prompt the user should any roles be missing before binding.

  Args:
    service_account_ref: The service account to add roles to.
    recommended_roles: The roles which will be added if they are missing.
    bind: A boolean indicating if the roles should be bound or not.
  """
  existing_roles = _GetProjectRolesForServiceAccount(service_account_ref)
  if _OWNER_ROLE in existing_roles:
    return

  # This prevents us from binding both roles to the same service account.
  # Events init requires admin, while broker create requires editor.
  if 'roles/pubsub.admin' in existing_roles or 'roles/editor' in existing_roles:
    existing_roles.add('roles/pubsub.editor')

  missing_roles = set(recommended_roles) - existing_roles
  if not missing_roles:
    return

  formatted_roles = '\n'.join(
      ['- {}'.format(role) for role in sorted(missing_roles)])

  log.status.Print(
      'Service account [{}] is missing the following recommended roles:\n'
      '{}'.format(service_account_ref.Name(), formatted_roles))

  if bind and console_io.CanPrompt():
    bind = console_io.PromptContinue(
        prompt_string='\nWould you like to bind these roles?',
        cancel_on_no=False)
  if bind:
    _BindProjectRolesForServiceAccount(service_account_ref, missing_roles)
    log.status.Print('Roles successfully bound.')
  else:
    log.warning('Manual binding of above roles may be necessary.')

