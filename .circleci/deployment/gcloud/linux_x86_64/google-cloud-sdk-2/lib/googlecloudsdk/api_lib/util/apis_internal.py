# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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

"""Library for obtaining API clients and messages.

This should only be called by api_lib.util.apis, core.resources, gcloud meta
commands, and module tests.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis_util
from googlecloudsdk.api_lib.util import resource as resource_util
from googlecloudsdk.core import properties
from googlecloudsdk.core import transport
from googlecloudsdk.third_party.apis import apis_map

import six


def _GetApiNameAndAlias(api_name):
  # pylint:disable=protected-access
  return (apis_util._API_NAME_ALIASES.get(api_name, api_name), api_name)


def _GetDefaultVersion(api_name):
  api_name, _ = _GetApiNameAndAlias(api_name)
  api_vers = apis_map.MAP.get(api_name, {})
  for ver, api_def in six.iteritems(api_vers):
    if api_def.default_version:
      return ver
  return None


def _GetApiNames():
  """Returns list of avaibleable apis, ignoring the version."""
  return sorted(apis_map.MAP.keys())


def _GetVersions(api_name):
  """Return available versions for given api.

  Args:
    api_name: str, The API name (or the command surface name, if different).

  Raises:
    apis_util.UnknownAPIError: If api_name does not exist in the APIs map.

  Returns:
    list, of version names.
  """
  api_name, _ = _GetApiNameAndAlias(api_name)
  version_map = apis_map.MAP.get(api_name, None)
  if version_map is None:
    raise apis_util.UnknownAPIError(api_name)
  return list(version_map.keys())


def _GetApiDef(api_name, api_version):
  """Returns the APIDef for the specified API and version.

  Args:
    api_name: str, The API name (or the command surface name, if different).
    api_version: str, The version of the API.

  Raises:
    apis_util.UnknownAPIError: If api_name does not exist in the APIs map.
    apis_util.UnknownVersionError: If api_version does not exist for given
      api_name in the APIs map.

  Returns:
    APIDef, The APIDef for the specified API and version.
  """
  api_name, api_name_alias = _GetApiNameAndAlias(api_name)
  if api_name not in apis_map.MAP:
    raise apis_util.UnknownAPIError(api_name)

  version_overrides = properties.VALUES.api_client_overrides.AllValues()

  # First attempt to get api specific override, then full surface override.
  version_override = version_overrides.get('{}/{}'.format(
      api_name, api_version))
  if not version_override:
    version_override = version_overrides.get(api_name_alias, None)

  api_version = version_override or api_version

  api_versions = apis_map.MAP[api_name]
  if api_version is None or api_version not in api_versions:
    raise apis_util.UnknownVersionError(api_name, api_version)
  else:
    api_def = api_versions[api_version]

  return api_def


def _GetClientClass(api_name, api_version):
  """Returns the client class for the API specified in the args.

  Args:
    api_name: str, The API name (or the command surface name, if different).
    api_version: str, The version of the API.

  Returns:
    base_api.BaseApiClient, Client class for the specified API.
  """
  api_def = _GetApiDef(api_name, api_version)
  return _GetClientClassFromDef(api_def)


def _GetClientClassFromDef(api_def):
  """Returns the client class for the API definition specified in the args.

  Args:
    api_def: apis_map.APIDef, The definition of the API.

  Returns:
    base_api.BaseApiClient, Client class for the specified API.
  """
  module_path, client_class_name = api_def.client_full_classpath.rsplit('.', 1)
  module_obj = __import__(module_path, fromlist=[client_class_name])
  return getattr(module_obj, client_class_name)


def _GetClientInstance(api_name,
                       api_version,
                       no_http=False,
                       http_client=None,
                       check_response_func=None):
  """Returns an instance of the API client specified in the args.

  Args:
    api_name: str, The API name (or the command surface name, if different).
    api_version: str, The version of the API.
    no_http: bool, True to not create an http object for this client.
    http_client: bring your own http client to use.
      Incompatible with no_http=True.
    check_response_func: error handling callback to give to apitools.

  Returns:
    base_api.BaseApiClient, An instance of the specified API client.
  """

  # pylint: disable=g-import-not-at-top
  if no_http:
    assert http_client is None
  elif http_client is None:
    # Normal gcloud authentication
    # Import http only when needed, as it depends on credential infrastructure
    # which is not needed in all cases.
    from googlecloudsdk.core.credentials import transports
    http_client = transports.GetApitoolsTransport(
        response_encoding=transport.ENCODING)

  client_class = _GetClientClass(api_name, api_version)
  client_instance = client_class(
      url=_GetEffectiveApiEndpoint(api_name, api_version, client_class),
      get_credentials=False,
      http=http_client)
  if check_response_func is not None:
    client_instance.check_response_func = check_response_func
  api_key = properties.VALUES.core.api_key.Get()
  if api_key:
    client_instance.AddGlobalParam('key', api_key)
    header = 'X-Google-Project-Override'
    client_instance.additional_http_headers[header] = 'apikey'
  return client_instance


def _GetMtlsEndpointIfEnabled(api_name, api_version, client_class=None):
  """Returns mtls endpoint if mtls is enabled for the API."""
  client_class = client_class or _GetClientClass(api_name, api_version)
  api_def = _GetApiDef(api_name, api_version)
  if api_def.enable_mtls:
    # Services with mTLS enabled should have the mTLS endpoint either in
    # mtls_endpoint_override in the API map or in the generated client.
    # We have tests to guarantee that.
    return api_def.mtls_endpoint_override or client_class.MTLS_BASE_URL


def _GetEffectiveApiEndpoint(api_name, api_version, client_class=None):
  """Returns effective endpoint for given api."""
  endpoint_overrides = properties.VALUES.api_endpoint_overrides.AllValues()
  endpoint_override = endpoint_overrides.get(api_name, '')
  if endpoint_override:
    return endpoint_override
  client_class = client_class or _GetClientClass(api_name, api_version)
  if properties.VALUES.context_aware.use_client_certificate.GetBool():
    mtls_endpoint = _GetMtlsEndpointIfEnabled(api_name, api_version,
                                              client_class)
    if mtls_endpoint:
      return mtls_endpoint
  return client_class.BASE_URL


def _GetDefaultEndpointUrl(url):
  """Looks up default endpoint based on overridden endpoint value."""
  endpoint_overrides = properties.VALUES.api_endpoint_overrides.AllValues()
  for api_name, overridden_url in six.iteritems(endpoint_overrides):
    if url.startswith(overridden_url):
      api_version = _GetDefaultVersion(api_name)
      return (_GetClientClass(api_name, api_version).BASE_URL +
              url[len(overridden_url):])
  return url


def _GetMessagesModule(api_name, api_version):
  """Returns the messages module for the API specified in the args.

  Args:
    api_name: str, The API name (or the command surface name, if different).
    api_version: str, The version of the API.

  Returns:
    Module containing the definitions of messages for the specified API.
  """
  api_def = _GetApiDef(api_name, api_version)
  # fromlist below must not be empty, see:
  # http://stackoverflow.com/questions/2724260/why-does-pythons-import-require-fromlist.
  return __import__(api_def.messages_full_modulepath, fromlist=['something'])


def _GetResourceModule(api_name, api_version):
  """Imports and returns given api resources module."""

  api_def = _GetApiDef(api_name, api_version)
  # fromlist below must not be empty, see:
  # http://stackoverflow.com/questions/2724260/why-does-pythons-import-require-fromlist.
  return __import__(api_def.class_path + '.' + 'resources',
                    fromlist=['something'])


def _GetApiCollections(api_name, api_version):
  """Yields all collections for for given api."""

  try:
    resources_module = _GetResourceModule(api_name, api_version)
  except ImportError:
    pass
  else:
    for collection in resources_module.Collections:
      yield resource_util.CollectionInfo(
          api_name,
          api_version,
          resources_module.BASE_URL,
          resources_module.DOCS_URL,
          collection.collection_name,
          collection.path,
          collection.flat_paths,
          collection.params,
          collection.enable_uri_parsing,
      )
