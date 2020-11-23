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

"""Command to set properties."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions

from googlecloudsdk.api_lib.cloudresourcemanager import projects_api
from googlecloudsdk.api_lib.util import exceptions as api_lib_util_exceptions
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as c_exc
from googlecloudsdk.command_lib.config import completers
from googlecloudsdk.command_lib.config import flags
from googlecloudsdk.command_lib.projects import util as command_lib_util
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.credentials import store as c_store


def _WarnIfSettingProjectWithNoAccess(scope, project):
  """Warn if setting 'core/project' config to inaccessible project."""

  # Only display a warning if the following conditions are true:
  #
  # * The current scope is USER (not occurring in the context of installation).
  # * The 'core/account' value is set (a user has authed).
  #
  # If the above conditions are met, check that the project being set exists
  # and is accessible to the current user, otherwise show a warning.
  if (scope == properties.Scope.USER and
      properties.VALUES.core.account.Get()):
    project_ref = command_lib_util.ParseProject(project)
    base.DisableUserProjectQuota()
    try:
      projects_api.Get(project_ref, disable_api_enablement_check=True)
    except (apitools_exceptions.HttpError,
            c_store.NoCredentialsForAccountException,
            api_lib_util_exceptions.HttpException):
      log.warning(
          'You do not appear to have access to project [{}] or'
          ' it does not exist.'.format(project))
    finally:
      base.EnableUserProjectQuota()


def _WarnIfActivateUseClientCertificate(prop):
  """Warns if setting context_aware/use_client_certificate to True."""
  if not prop.GetBool():
    return
  mtls_not_supported_msg = (
      'Some services may not support client certificate authorization in '
      'this version of gcloud. When a command sends requests to such services, '
      'the requests will be executed without using a client certificate.\n\n'
      'Please run $ gcloud topic client-certificate for more information.')
  log.warning(mtls_not_supported_msg)


class Set(base.Command):
  """Set a Cloud SDK property.

  {command} sets the specified property in your active configuration only. A
  property governs the behavior of a specific aspect of Cloud SDK such as
  the service account to use or the verbosity level of logs. To
  set the property across all configurations, use the `--installation` flag. For
  more information regarding creating and using configurations, see
  `gcloud topic configurations`.

  To view a list of properties currently in use, run `gcloud config list`.

  To unset properties, use `gcloud config unset`.

  Note, Cloud SDK comes with a `default` configuration. To create multiple
  configurations, use `gcloud config configurations create`, and
  `gcloud config configurations activate` to switch between them.

  ## AVAILABLE PROPERTIES

  {properties}

  ## EXAMPLES

  To set the `project` property in the core section, run:

    $ {command} project myProject

  To set the `zone` property in the `compute` section, run:

    $ {command} compute/zone asia-east1-b

  To disable prompting for scripting, run:

    $ {command} disable_prompts true

  To set a proxy with the appropriate type, and specify the address and port on
  which to reach it, run:

    $ {command} proxy/type http
    $ {command} proxy/address 1.234.56.78
    $ {command} proxy/port 8080

  For a full list of accepted values, see the Cloud SDK properties
  page: https://cloud.google.com/sdk/docs/properties
  """

  detailed_help = {'properties': properties.VALUES.GetHelpString()}

  @staticmethod
  def Args(parser):
    """Adds args for this command."""
    parser.add_argument(
        'property',
        metavar='SECTION/PROPERTY',
        completer=completers.PropertiesCompleter,
        help='Property to be set. Note that SECTION/ is optional while '
        'referring to properties in the core section, i.e., using either '
        '`core/project` or `project` is a valid way of setting a project. '
        'Using section names is required for setting other properties like '
        '`compute/region`. Consult the Available Properties section below '
        'for a comprehensive list of properties.')
    parser.add_argument(
        'value',
        completer=completers.PropertyValueCompleter,
        help='Value to be set.')

    flags.INSTALLATION_FLAG.AddToParser(parser)

  def Run(self, args):
    scope = (properties.Scope.INSTALLATION if args.installation
             else properties.Scope.USER)

    prop = properties.FromString(args.property)
    if not prop:
      raise c_exc.InvalidArgumentException(
          'property', 'Must be in the form: [SECTION/]PROPERTY')
    properties.PersistProperty(prop, args.value, scope=scope)

    scope_msg = ''
    if args.installation:
      scope_msg = 'installation '
    log.status.Print('Updated {0}property [{1}].'.format(scope_msg, prop))

    if prop == properties.VALUES.core.project:
      _WarnIfSettingProjectWithNoAccess(scope, prop.Get())
    if prop == properties.VALUES.context_aware.use_client_certificate:
      _WarnIfActivateUseClientCertificate(prop)
