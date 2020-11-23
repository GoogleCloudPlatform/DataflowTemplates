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

"""A module to get an unauthenticated requests.Session object."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from googlecloudsdk.core import context_aware
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import transport
from googlecloudsdk.core.util import http_proxy_types

import requests
import six
from six.moves import urllib
import socks
from urllib3.util.ssl_ import create_urllib3_context


def GetSession(timeout='unset', response_encoding=None, ca_certs=None,
               session=None):
  """Get a requests.Session that is properly configured for use by gcloud.

  This method does not add credentials to the client. For a requests.Session
  that has been authenticated, use core.credentials.requests.GetSession().

  Args:
    timeout: double, The timeout in seconds. This is the
        socket level timeout. If timeout is None, timeout is infinite. If
        default argument 'unset' is given, a sensible default is selected using
        transport.GetDefaultTimeout().
    response_encoding: str, the encoding to decode with when accessing
        response.text. If none, then the encoding will be inferred from the
        response.
    ca_certs: str, absolute filename of a ca_certs file that overrides the
        default. The gcloud config property for ca_certs, in turn, overrides
        this argument.
    session: requests.Session instance

  Returns:
    A requests.Session object configured with all the required settings
    for gcloud.
  """
  http_client = _CreateRawSession(timeout, ca_certs, session)
  http_client = RequestWrapper().WrapWithDefaults(http_client,
                                                  response_encoding)
  return http_client


class ClientSideCertificate(
    collections.namedtuple('ClientSideCertificate',
                           ['certfile', 'keyfile', 'password'])):
  """Holds information about a client side certificate.

  Attributes:
    certfile: str, path to a cert file.
    keyfile: str, path to a key file.
    password: str, password to the private key.
  """

  def __new__(cls, certfile, keyfile, password=None):
    return super(ClientSideCertificate, cls).__new__(
        cls, certfile, keyfile, password)


def CreateSSLContext():
  """Returns a urrlib3 SSL context."""
  return create_urllib3_context()


class HTTPAdapter(requests.adapters.HTTPAdapter):
  """Transport adapter for requests.

  Transport adapters provide an interface to extend the default behavior of the
  requests library using the full power of the underlying urrlib3 library.

  See https://requests.readthedocs.io/en/master/user/advanced/
      #transport-adapters for more information about adapters.
  """

  def __init__(self, client_side_certificate, *args, **kwargs):
    self._cert_info = client_side_certificate
    super(HTTPAdapter, self).__init__(*args, **kwargs)

  def init_poolmanager(self, *args, **kwargs):
    self._add_ssl_context(kwargs)
    return super(HTTPAdapter, self).init_poolmanager(*args, **kwargs)

  def proxy_manager_for(self, *args, **kwargs):
    self._add_ssl_context(kwargs)
    return super(HTTPAdapter, self).proxy_manager_for(*args, **kwargs)

  def _add_ssl_context(self, kwargs):
    if not self._cert_info:
      return

    context = CreateSSLContext()

    cert_chain_kwargs = {}
    if self._cert_info.keyfile:
      cert_chain_kwargs['keyfile'] = self._cert_info.keyfile
    if self._cert_info.password:
      cert_chain_kwargs['password'] = self._cert_info.password

    context.load_cert_chain(self._cert_info.certfile, **cert_chain_kwargs)

    kwargs['ssl_context'] = context


def GetProxyInfo():
  """Returns the proxy string for use by requests from gcloud properties.

  See https://requests.readthedocs.io/en/master/user/advanced/#proxies.
  """
  proxy_type = properties.VALUES.proxy.proxy_type.Get()
  proxy_address = properties.VALUES.proxy.address.Get()
  proxy_port = properties.VALUES.proxy.port.GetInt()

  proxy_prop_set = len(
      [f for f in (proxy_type, proxy_address, proxy_port) if f])
  if proxy_prop_set > 0 and proxy_prop_set != 3:
    raise properties.InvalidValueError(
        'Please set all or none of the following properties: '
        'proxy/type, proxy/address and proxy/port')

  if not proxy_prop_set:
    return

  proxy_rdns = properties.VALUES.proxy.rdns.GetBool()
  proxy_user = properties.VALUES.proxy.username.Get()
  proxy_pass = properties.VALUES.proxy.password.Get()

  proxy_type = http_proxy_types.PROXY_TYPE_MAP[proxy_type]
  if proxy_type == socks.PROXY_TYPE_SOCKS4:
    proxy_scheme = 'socks4a' if proxy_rdns else 'socks4'
  elif proxy_type == socks.PROXY_TYPE_SOCKS5:
    proxy_scheme = 'socks5h' if proxy_rdns else 'socks5'
  elif proxy_type == socks.PROXY_TYPE_HTTP:
    proxy_scheme = 'http'
  else:
    raise ValueError('Unsupported proxy type: {}'.format(proxy_type))

  if proxy_user or proxy_pass:
    proxy_auth = ':'.join(x or '' for x in (proxy_user, proxy_pass))
    proxy_auth += '@'
  else:
    proxy_auth = ''
  return '{}://{}{}:{}'.format(proxy_scheme, proxy_auth, proxy_address,
                               proxy_port)


def Session(
    timeout=None,
    ca_certs=None,
    disable_ssl_certificate_validation=False,
    session=None):
  """Returns a requests.Session subclass.

  Args:
    timeout: float, Request timeout, in seconds.
    ca_certs: str, absolute filename of a ca_certs file
    disable_ssl_certificate_validation: bool, If true, disable ssl certificate
        validation.
    session: requests.Session instance. Otherwise, a new requests.Session will
        be initialized.

  Returns: A requests.Session subclass.
  """
  session = session or requests.Session()

  orig_request_method = session.request
  def WrappedRequest(*args, **kwargs):
    if 'timeout' not in kwargs:
      kwargs['timeout'] = timeout
    return orig_request_method(*args, **kwargs)
  session.request = WrappedRequest

  proxy_info = GetProxyInfo()
  if proxy_info:
    session.proxies = {
        'http': proxy_info,
        'https': proxy_info,
    }

  client_side_certificate = None
  if properties.VALUES.context_aware.use_client_certificate.Get():
    ca_config = context_aware.Config()
    log.debug('Using client certificate %s', ca_config.client_cert_path)
    client_side_certificate = ClientSideCertificate(
        ca_config.client_cert_path,
        ca_config.client_cert_path,
        ca_config.client_cert_password)
  else:
    client_side_certificate = None

  adapter = HTTPAdapter(client_side_certificate)

  if disable_ssl_certificate_validation:
    session.verify = False
  elif ca_certs:
    session.verify = ca_certs

  session.mount('https://', adapter)
  return session


def _CreateRawSession(timeout='unset', ca_certs=None, session=None):
  """Create a requests.Session matching the appropriate gcloud properties."""
  # Compared with setting the default timeout in the function signature (i.e.
  # timeout=300), this lets you test with short default timeouts by mocking
  # GetDefaultTimeout.
  if timeout != 'unset':
    effective_timeout = timeout
  else:
    effective_timeout = transport.GetDefaultTimeout()

  no_validate = properties.VALUES.auth.disable_ssl_validation.GetBool() or False
  ca_certs_property = properties.VALUES.core.custom_ca_certs_file.Get()
  # Believe an explicitly-set ca_certs property over anything we added.
  if ca_certs_property:
    ca_certs = ca_certs_property
  if no_validate:
    ca_certs = None
  return Session(timeout=effective_timeout,
                 ca_certs=ca_certs,
                 disable_ssl_certificate_validation=no_validate,
                 session=session)


def _GetURIFromRequestArgs(url, params):
  """Gets the complete URI by merging url and params from the request args."""
  url_parts = urllib.parse.urlsplit(url)
  query_params = urllib.parse.parse_qs(url_parts.query)
  for param, value in six.iteritems(params or {}):
    query_params[param] = value
  # Need to do this to convert a SplitResult into a list so it can be modified.
  url_parts = list(url_parts)
  # pylint:disable=redundant-keyword-arg, this is valid syntax for this lib
  url_parts[3] = urllib.parse.urlencode(query_params, doseq=True)

  # pylint:disable=too-many-function-args, This is just bogus.
  return urllib.parse.urlunsplit(url_parts)


class Request(transport.Request):
  """Encapsulates parameters for making a general HTTP request.

  This implementation does additional manipulation to ensure that the request
  parameters are specified in the same way as they were specified by the
  caller. That is, if the user calls:
      request('URI', 'GET', None, {'header': '1'})

  After modifying the request, we will call request using positional
  parameters, instead of transforming the request into:
      request('URI', method='GET', body=None, headers={'header': '1'})
  """

  @classmethod
  def FromRequestArgs(cls, *args, **kwargs):
    return cls(*args, **kwargs)

  def __init__(self, method, url, params=None, data=None, headers=None,
               **kwargs):
    self._kwargs = kwargs
    uri = _GetURIFromRequestArgs(url, params)
    super(Request, self).__init__(uri, method, headers or {}, data)

  def ToRequestArgs(self):
    args = [self.method, self.uri]
    kwargs = dict(self._kwargs)
    kwargs['headers'] = self.headers
    if self.body:
      kwargs['data'] = self.body
    return args, kwargs


class Response(transport.Response):
  """Encapsulates responses from making a general HTTP request."""

  @classmethod
  def FromResponse(cls, response):
    return cls(response.status_code, response.headers, response.content)


class RequestWrapper(transport.RequestWrapper):
  """Class for wrapping request.Session requests."""

  request_class = Request
  response_class = Response

  def DecodeResponse(self, response, response_encoding):
    response.encoding = response_encoding
    return response
