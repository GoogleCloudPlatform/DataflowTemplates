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

"""Run a web flow for oauth2.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import contextlib
import json
import socket
import webbrowser
import wsgiref
from google_auth_oauthlib import flow as google_auth_flow

from googlecloudsdk.core import exceptions as c_exceptions
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import requests
from googlecloudsdk.core.util import files
from googlecloudsdk.core.util import pkg_resources

from oauth2client import client
from oauth2client import tools
from oauthlib.oauth2.rfc6749 import errors as rfc6749_errors

from requests import exceptions as requests_exceptions
import six
from six.moves import input  # pylint: disable=redefined-builtin
from six.moves.http_client import ResponseNotReady
from six.moves.urllib import parse

_PORT_SEARCH_ERROR_MSG = (
    'Failed to start a local webserver listening on any port '
    'between {start_port} and {end_port}. Please check your '
    'firewall settings or locally running programs that may be '
    'blocking or using those ports.')

_PORT_SEARCH_START = 8085
_PORT_SEARCH_END = _PORT_SEARCH_START + 100


class Error(c_exceptions.Error):
  """Exceptions for the flow module."""


class AuthRequestRejectedError(Error):
  """Exception for when the authentication request was rejected."""


class AuthRequestFailedError(Error):
  """Exception for when the authentication request failed."""


class LocalServerCreationError(Error):
  """Exception for when a local server cannot be created."""


class LocalServerTimeoutError(Error):
  """Exception for when the local server timeout before receiving request."""


def RaiseProxyError(source_exc):
  six.raise_from(AuthRequestFailedError(
      'Could not reach the login server. A potential cause of this could be '
      'because you are behind a proxy. Please set the environment variables '
      'HTTPS_PROXY and HTTP_PROXY to the address of the proxy in the format '
      '"protocol://address:port" (without quotes) and try again.\n'
      'Example: HTTPS_PROXY=https://192.168.0.1:8080'), source_exc)


class ClientRedirectHandler(tools.ClientRedirectHandler):
  """A handler for OAuth 2.0 redirects back to localhost.

  Waits for a single request and parses the query parameters
  into the servers query_params and then stops serving.
  """

  # pylint:disable=invalid-name, This method is overriden from the base class.
  def do_GET(self):
    """Handle a GET request.

    Parses the query parameters and prints a message
    if the flow has completed. Note that we can't detect
    if an error occurred.
    """
    self.send_response(200)
    self.send_header('Content-type', 'text/html')
    self.end_headers()
    query = self.path.split('?', 1)[-1]
    query = dict(parse.parse_qsl(query))
    self.server.query_params = query

    if 'code' in query:
      page = 'oauth2_landing.html'
    else:
      page = 'oauth2_landing_error.html'

    self.wfile.write(pkg_resources.GetResource(__name__, page))


def PromptForAuthCode(message, authorize_url):
  log.err.Print(message.format(url=authorize_url))
  return input('Enter verification code: ').strip()


def Run(flow, launch_browser=True, http=None,
        auth_host_name='localhost', auth_host_port_start=8085):
  """Run a web flow to get oauth2 credentials.

  Args:
    flow: oauth2client.OAuth2WebServerFlow, A flow that is ready to run.
    launch_browser: bool, If False, give the user a URL to copy into
        a browser. Requires that they paste the refresh token back into the
        terminal. If True, opens a web browser in a new window.
    http: httplib2.Http, The http transport to use for authentication.
    auth_host_name: str, Host name for the redirect server.
    auth_host_port_start: int, First port to try for serving the redirect. If
        this port is taken, it will keep trying incrementing ports until 100
        have been tried, then fail.

  Returns:
    oauth2client.Credential, A ready-to-go credential that has already been
    put in the storage.

  Raises:
    AuthRequestRejectedError: If the request was rejected.
    AuthRequestFailedError: If the request fails.
  """

  if launch_browser:
    success = False
    port_number = auth_host_port_start

    while True:
      try:
        httpd = tools.ClientRedirectServer((auth_host_name, port_number),
                                           ClientRedirectHandler)
      except socket.error as e:
        if port_number > auth_host_port_start + 100:
          success = False
          break
        port_number += 1
      else:
        success = True
        break

    if success:
      flow.redirect_uri = ('http://%s:%s/' % (auth_host_name, port_number))

      authorize_url = flow.step1_get_authorize_url()
      webbrowser.open(authorize_url, new=1, autoraise=True)
      message = 'Your browser has been opened to visit:'
      log.err.Print('{message}\n\n    {url}\n\n'.format(
          message=message,
          url=authorize_url,))

      httpd.handle_request()
      if 'error' in httpd.query_params:
        raise AuthRequestRejectedError('Unable to authenticate.')
      if 'code' in httpd.query_params:
        code = httpd.query_params['code']
      else:
        raise AuthRequestFailedError(
            'Failed to find "code" in the query parameters of the redirect.')
    else:
      message = ('Failed to start a local webserver listening on any port '
                 'between {start_port} and {end_port}. Please check your '
                 'firewall settings or locally running programs that may be '
                 'blocking or using those ports.')
      log.warning(message.format(
          start_port=auth_host_port_start,
          end_port=port_number,
      ))

      launch_browser = False
      log.warning('Defaulting to URL copy/paste mode.')

  if not launch_browser:
    flow.redirect_uri = client.OOB_CALLBACK_URN
    authorize_url = flow.step1_get_authorize_url()
    message = 'Go to the following link in your browser:\n\n    {url}\n\n'
    try:
      code = PromptForAuthCode(message, authorize_url)
    except EOFError as e:
      raise AuthRequestRejectedError(e)

  try:
    credential = flow.step2_exchange(code, http=http)
  except client.FlowExchangeError as e:
    raise AuthRequestFailedError(e)
  except ResponseNotReady as e:
    RaiseProxyError(e)

  return credential


def CreateGoogleAuthFlow(scopes, client_id_file=None):
  """Creates a Google auth oauthlib browser flow."""
  client_config = _CreateGoogleAuthClientConfig(client_id_file)
  return InstalledAppFlow.from_client_config(
      client_config, scopes, autogenerate_code_verifier=True)


def _CreateGoogleAuthClientConfig(client_id_file=None):
  """Creates a client config from a client id file or gcloud's properties."""
  if client_id_file:
    with files.FileReader(client_id_file) as f:
      return json.load(f)
  return _CreateGoogleAuthClientConfigFromProperties()


def _CreateGoogleAuthClientConfigFromProperties():
  auth_uri = properties.VALUES.auth.auth_host.Get(required=True)
  token_uri = properties.VALUES.auth.token_host.Get(required=True)
  client_id = properties.VALUES.auth.client_id.Get(required=True)
  client_secret = properties.VALUES.auth.client_secret.Get(required=True)
  return {
      'installed': {
          'client_id': client_id,
          'client_secret': client_secret,
          'auth_uri': auth_uri,
          'token_uri': token_uri
      }
  }


@contextlib.contextmanager
def HandleOauth2FlowErrors():
  try:
    yield
  except requests_exceptions.ConnectionError as e:
    RaiseProxyError(e)
  except rfc6749_errors.AccessDeniedError as e:
    six.raise_from(AuthRequestRejectedError(e), e)
  except ValueError as e:
    raise six.raise_from(AuthRequestFailedError(e), e)


def _RunGoogleAuthFlowLaunchBrowser(flow):
  """Runs oauth2 3LO flow and auto launch the browser."""
  authorization_prompt_msg_launch_browser = (
      'Your browser has been opened to visit:\n\n    {url}\n')
  with HandleOauth2FlowErrors():
    return flow.run_local_server(
        authorization_prompt_message=authorization_prompt_msg_launch_browser)


def _RunGoogleAuthFlowNoLaunchBrowser(flow):
  """Runs oauth2 3LO flow without auto-launching the browser."""
  authorization_prompt_msg_no_launch_browser = (
      'Go to the following link in your browser:\n\n    {url}\n')
  with HandleOauth2FlowErrors():
    return flow.run_console(
        authorization_prompt_message=authorization_prompt_msg_no_launch_browser)


def RunGoogleAuthFlow(flow, launch_browser=False):
  """Runs a Google auth oauthlib web flow.

  Args:
    flow: InstalledAppFlow, A web flow to run.
    launch_browser: bool, True to launch the web browser automatically and and
      use local server to handle the redirect. False to ask users to paste the
      url in a browser.

  Returns:
    google.auth.credentials.Credentials, The credentials obtained from the flow.
  """
  if launch_browser:
    try:
      return _RunGoogleAuthFlowLaunchBrowser(flow)
    except LocalServerCreationError as e:
      log.warning(e)
      log.warning('Defaulting to URL copy/paste mode.')
  return _RunGoogleAuthFlowNoLaunchBrowser(flow)


class InstalledAppFlow(google_auth_flow.InstalledAppFlow):
  """Installed app flow.

  This class overrides base class's run_local_server() method to provide
  customized behaviors for gcloud auth login:
    1. Try to find an available port for the local server which handles the
       redirect.
    2. A WSGI app on the local server which can direct browser to
       Google's confirmation pages for authentication.

  This class overrides base class's run_console() method so that the auth code
  fetching step can be easily mocked in login integration testing.
  """

  def __init__(
      self, oauth2session, client_type, client_config,
      redirect_uri=None, code_verifier=None,
      autogenerate_code_verifier=False):
    """Initializes a google_auth_flow.InstalledAppFlow.

    Args:
        oauth2session (requests_oauthlib.OAuth2Session):
            The OAuth 2.0 session from ``requests-oauthlib``.
        client_type (str): The client type, either ``web`` or
            ``installed``.
        client_config (Mapping[str, Any]): The client
            configuration in the Google `client secrets`_ format.
        redirect_uri (str): The OAuth 2.0 redirect URI if known at flow
            creation time. Otherwise, it will need to be set using
            :attr:`redirect_uri`.
        code_verifier (str): random string of 43-128 chars used to verify
            the key exchange.using PKCE.
        autogenerate_code_verifier (bool): If true, auto-generate a
            code_verifier.
    .. _client secrets:
        https://developers.google.com/api-client-library/python/guide
        /aaa_client_secrets
    """
    session = requests.GetSession(session=oauth2session)
    super(InstalledAppFlow, self).__init__(
        session, client_type, client_config,
        redirect_uri, code_verifier,
        autogenerate_code_verifier)

  def run_local_server(self,
                       host='localhost',
                       authorization_prompt_message=google_auth_flow
                       .InstalledAppFlow._DEFAULT_AUTH_PROMPT_MESSAGE,
                       open_browser=True,
                       **kwargs):
    """Run the flow using the server strategy.

    The server strategy instructs the user to open the authorization URL in
    their browser and will attempt to automatically open the URL for them.
    It will start a local web server to listen for the authorization
    response. Once authorization is complete the authorization server will
    redirect the user's browser to the local web server. The web server
    will get the authorization code from the response and shutdown. The
    code is then exchanged for a token.

    Args:
        host: str, The hostname for the local redirect server. This will
          be served over http, not https.
        authorization_prompt_message: str, The message to display to tell
          the user to navigate to the authorization URL.
        open_browser: bool, Whether or not to open the authorization URL
          in the user's browser.
        **kwargs: Additional keyword arguments passed through to
          authorization_url`.

    Returns:
        google.oauth2.credentials.Credentials: The OAuth 2.0 credentials
          for the user.

    Raises:
      LocalServerTimeoutError: If the local server handling redirection timeout
        before receiving the request.
    """

    wsgi_app = _RedirectWSGIApp()
    local_server = CreateLocalServer(wsgi_app, _PORT_SEARCH_START,
                                     _PORT_SEARCH_END)

    self.redirect_uri = 'http://{}:{}/'.format(host, local_server.server_port)
    auth_url, _ = self.authorization_url(**kwargs)

    if open_browser:
      webbrowser.open(auth_url, new=1, autoraise=True)

    log.err.Print(authorization_prompt_message.format(url=auth_url))

    local_server.handle_request()

    if not wsgi_app.last_request_uri:
      raise LocalServerTimeoutError(
          'Local server timed out before receiving the redirection request.')
    # Note: using https here because oauthlib requires that
    # OAuth 2.0 should only occur over https.
    authorization_response = wsgi_app.last_request_uri.replace(
        'http:', 'https:')
    self.fetch_token(authorization_response=authorization_response)

    return self.credentials

  def run_console(self,
                  authorization_prompt_message=google_auth_flow.InstalledAppFlow
                  ._DEFAULT_AUTH_PROMPT_MESSAGE,
                  **kwargs):
    """Run the flow using the console strategy.

    The console strategy instructs the user to open the authorization URL
    in their browser. Once the authorization is complete the authorization
    server will give the user a code. The user then must copy & paste this
    code into the application. The code is then exchanged for a token.

    Args:
        authorization_prompt_message: str, The message to display to tell the
          user to navigate to the authorization URL.
        **kwargs: Additional keyword arguments passed through to
          'authorization_url'.

    Returns:
        google.oauth2.credentials.Credentials: The OAuth 2.0 credentials
          for the user.
    """
    kwargs.setdefault('prompt', 'consent')

    self.redirect_uri = self._OOB_REDIRECT_URI

    auth_url, _ = self.authorization_url(**kwargs)

    code = PromptForAuthCode(authorization_prompt_message, auth_url)

    self.fetch_token(code=code)

    return self.credentials


def CreateLocalServer(wsgi_app, search_start_port, search_end_port):
  """Creates a local wsgi server.

  Finds an available port in the range of [search_start_port, search_end_point)
  for the local server.

  Args:
    wsgi_app: A wsgi app running on the local server.
    search_start_port: int, the port where the search starts.
    search_end_port: int, the port where the search ends.

  Raises:
    LocalServerCreationError: If it cannot find an available port for
      the local server.

  Returns:
    wsgiref.simple_server.WSGISever, a wsgi server.
  """
  port = search_start_port
  local_server = None
  while not local_server and port < search_end_port:
    try:
      local_server = wsgiref.simple_server.make_server(
          'localhost',
          port,
          wsgi_app,
          handler_class=google_auth_flow._WSGIRequestHandler)  # pylint:disable=protected-access
    except (socket.error, OSError):
      port += 1
  if local_server:
    return local_server
  raise LocalServerCreationError(
      _PORT_SEARCH_ERROR_MSG.format(
          start_port=search_start_port, end_port=search_end_port - 1))


class _RedirectWSGIApp(object):
  """WSGI app to handle the authorization redirect.

  Stores the request URI and responds with a confirmation page.
  """

  def __init__(self):
    self.last_request_uri = None

  def __call__(self, environ, start_response):
    """WSGI Callable.

    Args:
        environ (Mapping[str, Any]): The WSGI environment.
        start_response (Callable[str, list]): The WSGI start_response callable.

    Returns:
        Iterable[bytes]: The response body.
    """
    start_response(
        six.ensure_str('200 OK'),
        [(six.ensure_str('Content-type'), six.ensure_str('text/html'))])
    self.last_request_uri = wsgiref.util.request_uri(environ)
    query = self.last_request_uri.split('?', 1)[-1]
    query = dict(parse.parse_qsl(query))
    if 'code' in query:
      page = 'oauth2_landing.html'
    else:
      page = 'oauth2_landing_error.html'
    return [pkg_resources.GetResource(__name__, page)]
