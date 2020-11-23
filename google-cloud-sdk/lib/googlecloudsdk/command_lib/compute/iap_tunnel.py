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

"""Tunnel TCP traffic over Cloud IAP WebSocket connection."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import ctypes
import errno
import functools
import io
import os
import select
import socket
import sys
import threading
import time

from googlecloudsdk.api_lib.compute import iap_tunnel_websocket
from googlecloudsdk.api_lib.compute import iap_tunnel_websocket_utils as utils
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import execution_utils
from googlecloudsdk.core import http_proxy
from googlecloudsdk.core import log
from googlecloudsdk.core.credentials import creds
from googlecloudsdk.core.credentials import store
from googlecloudsdk.core.util import files
from googlecloudsdk.core.util import platforms
import portpicker
import six


class LocalPortUnavailableError(exceptions.Error):
  pass


class UnableToOpenPortError(exceptions.Error):
  pass


def _AddBaseArgs(parser):
  parser.add_argument(
      '--iap-tunnel-url-override',
      hidden=True,
      help=('Allows for overriding the connection endpoint for integration '
            'testing.'))
  parser.add_argument(
      '--iap-tunnel-insecure-disable-websocket-cert-check',
      default=False,
      action='store_true',
      hidden=True,
      help='Disables checking certificates on the WebSocket connection.')


def AddSshTunnelArgs(parser, tunnel_through_iap_scope):
  _AddBaseArgs(parser)
  tunnel_through_iap_scope.add_argument(
      '--tunnel-through-iap',
      action='store_true',
      help="""\
      Tunnel the ssh connection through Cloud Identity-Aware Proxy for TCP
      forwarding.

      To learn more, see the
      [IAP for TCP forwarding documentation](https://cloud.google.com/iap/docs/tcp-forwarding-overview).
      """)


def AddProxyServerHelperArgs(parser):
  _AddBaseArgs(parser)


class SshTunnelArgs(object):
  """A class to hold some options for IAP Tunnel SSH/SCP.

  Attributes:
    track: str/None, the prefix of the track for the inner gcloud.
    project: str, the project id (string with dashes).
    zone: str, the zone name.
    instance: str, the instance name.
    pass_through_args: [str], additional args to be passed to the inner gcloud.
  """

  def __init__(self):
    self.track = None
    self.project = ''
    self.zone = ''
    self.instance = ''
    self.pass_through_args = []

  @staticmethod
  def FromArgs(args, track, instance_ref, external_interface):
    """Construct an SshTunnelArgs from command line args and values.

    Args:
      args: The parsed commandline arguments. May or may not have had
        AddSshTunnelArgs called.
      track: ReleaseTrack, The currently running release track.
      instance_ref: The target instance reference object.
      external_interface: The external interface of target resource object, if
        available, otherwise None.
    Returns:
      SshTunnelArgs or None if IAP Tunnel is disabled.
    """
    # If tunneling through IAP is not available, then abort.
    if not hasattr(args, 'tunnel_through_iap'):
      return None

    # If set to connect directly to private IP address, then abort.
    if getattr(args, 'internal_ip', False):
      return None

    if args.IsSpecified('tunnel_through_iap'):
      # If IAP tunneling is explicitly disabled, then abort.
      if not args.tunnel_through_iap:
        return None
    else:
      # If no external interface is available, then default to using IAP
      # tunneling and continue with code below.  Otherwise, abort.
      if external_interface:
        return None
      log.status.Print('External IP address was not found; defaulting to using '
                       'IAP tunneling.')

    res = SshTunnelArgs()

    res.track = track.prefix
    res.project = instance_ref.project
    res.zone = instance_ref.zone
    res.instance = instance_ref.instance

    # The tunnel_through_iap attribute existed, so these must too.
    if args.IsSpecified('iap_tunnel_url_override'):
      res.pass_through_args.append(
          '--iap-tunnel-url-override=' + args.iap_tunnel_url_override)
    if args.iap_tunnel_insecure_disable_websocket_cert_check:
      res.pass_through_args.append(
          '--iap-tunnel-insecure-disable-websocket-cert-check')
    return res

  def _Members(self):
    return (
        self.track,
        self.project,
        self.zone,
        self.instance,
        self.pass_through_args,
    )

  def __eq__(self, other):
    # pylint: disable=protected-access
    return self._Members() == other._Members()

  def __ne__(self, other):
    return not self == other

  def __repr__(self):
    return 'SshTunnelArgs<%r>' % (self._Members(),)


def DetermineLocalPort(port_arg=0):
  if not port_arg:
    port_arg = portpicker.pick_unused_port()
  if not portpicker.is_port_free(port_arg):
    raise LocalPortUnavailableError('Local port [%d] is not available.' %
                                    port_arg)
  return port_arg


def _CloseLocalConnectionCallback(local_conn):
  """Callback function to close the local connection, if any."""
  # For test WebSocket connections, there is not a local socket connection.
  if local_conn:
    try:
      # Calling shutdown() first is needed to promptly notify the process on
      # the other side of the connection that it is closing. This allows that
      # other process, whether over TCP or stdin, to promptly terminate rather
      # that waiting for the next time that the process tries to send data.
      local_conn.shutdown(socket.SHUT_RDWR)
    except EnvironmentError:
      pass
    try:
      local_conn.close()
    except EnvironmentError:
      pass


def _GetAccessTokenCallback(credentials):
  """Callback function to refresh credentials and return access token."""
  if not credentials:
    return None
  log.debug('credentials type for _GetAccessTokenCallback is [%s].',
            six.text_type(credentials))
  store.Refresh(credentials)

  if creds.IsGoogleAuthCredentials(credentials):
    return credentials.token
  else:
    return credentials.access_token


def _SendLocalDataCallback(local_conn, data):
  # For test WebSocket connections, there is not a local socket connection.
  if local_conn:
    local_conn.send(data)


def _OpenLocalTcpSockets(local_host, local_port):
  """Attempt to open a local socket(s) listening on specified host and port."""
  open_sockets = []
  for res in socket.getaddrinfo(
      local_host, local_port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0,
      socket.AI_PASSIVE):
    af, socktype, proto, unused_canonname, sock_addr = res
    try:
      s = socket.socket(af, socktype, proto)
    except socket.error:
      continue
    try:
      s.bind(sock_addr)
      s.listen(1)
      open_sockets.append(s)
    except EnvironmentError:
      try:
        s.close()
      except socket.error:
        pass

  if open_sockets:
    return open_sockets

  raise UnableToOpenPortError('Unable to open socket on port [%d].' %
                              local_port)


class _StdinSocket(object):
  """A wrapper around stdin/out that allows it to be treated like a socket.

  Does not implement all socket functions. And of the ones implemented, not all
  arguments/flags are supported. Once created, stdin should never be accessed by
  anything else.
  """

  def __init__(self):
    # This is only used in Unix.
    self._stdin_closed = False

  def send(self, data):  # pylint: disable=invalid-name
    files.WriteStreamBytes(sys.stdout, data)
    if not six.PY2:
      # WriteStreamBytes flushes python2 but not python3. Perhaps it should
      # be modified to also flush python3.
      sys.stdout.buffer.flush()
    return len(data)

  def recv(self, bufsize):  # pylint: disable=invalid-name
    """Receives data from stdin.

    Blocks until at least 1 byte is available.
    On Unix (but not Windows) this is unblocked by close() and shutdown(RD).
    On all platforms a signal handler triggering an exception will unblock this.
    This cannot be called by multiple threads at the same time.
    This function performs cleanups before returning, so killing gcloud while
    this is running should be avoided. Specifically RaisesKeyboardInterrupt
    should be in effect so that ctrl-c causes a clean exit with an exception
    instead of triggering gcloud's default os.kill().

    Args:
      bufsize: The maximum number of bytes to receive. Must be positive.
    Returns:
      The bytes received. EOF is indicated by b''.
    Raises:
      IOError: On low level errors.
    """
    if platforms.OperatingSystem.IsWindows():
      return self._RecvWindows(bufsize)
    else:
      return self._RecvUnix(bufsize)

  def close(self):  # pylint: disable=invalid-name
    # Closing stdin doesn't help, because it doesn't unblock read() calls.
    # Also it causes problems, such as segfaulting in python2 and blocking in
    # python3.
    self.shutdown(socket.SHUT_RD)

  def shutdown(self, how):  # pylint: disable=invalid-name
    # Shutting down read only (SHUT_RD) on Unix only (no change/effect on
    # Windows)
    if how in (socket.SHUT_RDWR, socket.SHUT_RD):
      self._stdin_closed = True

  def _RecvWindows(self, bufsize):
    """Reads data from std in Windows.

    Args:
      bufsize: The maximum number of bytes to receive. Must be positive.
    Returns:
      The bytes received. EOF is indicated by b''.
    Raises:
      socket.error: On low level errors.
    """
    # On Windows the way to quickly read without unnecessary blocking is
    # to directly call ReadFile().
    from ctypes import wintypes  # pylint: disable=g-import-not-at-top
    # STD_INPUT_HANDLE is -10
    h = ctypes.windll.kernel32.GetStdHandle(-10)
    buf = ctypes.create_string_buffer(bufsize)
    number_of_bytes_read = wintypes.DWORD()
    ok = ctypes.windll.kernel32.ReadFile(
        h, buf, bufsize, ctypes.byref(number_of_bytes_read), None)
    if not ok:
      raise socket.error(errno.EIO, 'stdin ReadFile failed')
    return buf.raw[:number_of_bytes_read.value]

  class _EOFError(Exception):
    pass

  def _RecvUnix(self, bufsize):
    """Reads data from stdin on Unix.

    Args:
      bufsize: The maximum number of bytes to receive. Must be positive.
    Returns:
      The bytes received. EOF is indicated by b''. Once EOF has been indicated,
      will always indicate EOF.
    Raises:
      IOError: On low level errors.
    """
    # On Unix, the way to quickly read bytes without unnecessary blocking
    # is to make stdin non-blocking. To ensure at least 1 byte is received, we
    # read the first byte blocking.
    b = b''
    try:
      while not self._stdin_closed:
        b = self._ReadUnixNonBlocking(bufsize)
        if b:
          break
        time.sleep(0.001)
    except _StdinSocket._EOFError:
      self._stdin_closed = True
    return b

  def _ReadUnixNonBlocking(self, bufsize):
    """Reads from stdin on Unix in a nonblocking manner.

    Args:
      bufsize: The maximum number of bytes to receive. Must be positive.
    Returns:
      The bytes read. b'' means no data is available.
    Raises:
      _StdinSocket._EOFError: to indicate EOF.
      IOError: On low level errors.
    """
    # In python 3, we need to read stdin in a binary way, not a text way to
    # read bytes instead of str. In python 2, binary mode vs text mode only
    # matters on Windows.
    import fcntl  # pylint: disable=g-import-not-at-top
    old_flags = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
    try:
      fcntl.fcntl(sys.stdin, fcntl.F_SETFL, old_flags | os.O_NONBLOCK)
      if six.PY2:
        b = sys.stdin.read(bufsize)
      else:
        b = sys.stdin.buffer.read(bufsize)
    except IOError as e:
      if e.errno == errno.EAGAIN or isinstance(e, io.BlockingIOError):
        # In python2, no nonblocking data available is indicated by raising
        # IOError with EAGAIN.
        # The online python3 documentation says BlockingIOError is raised when
        # no nonblocking data available. We handle that case in case it is ever
        # correct. BlockingIOError is a subclass of OSError which is identical
        # to IOError.
        return b''
      raise
    finally:
      # We need to restore the flags no matter what when exiting this function.
      # Other code assumes it's blocking. Also even when gcloud exits,
      # nonblocking stdin causes weird problems in that terminal, such as cat
      # stops working.
      # There's actually still a small chance of that happening if gcloud is
      # running like this directly, then killed in the small time between the
      # set nonblocking and the restore. The user could fix that by running bash
      # and exiting it, or just closing the terminal.
      # If gcloud is running as an ssh ProxyCommand this problem doesn't happen.
      fcntl.fcntl(sys.stdin, fcntl.F_SETFL, old_flags)
    if b == b'':  # pylint: disable=g-explicit-bool-comparison
      # In python 2 and 3, EOF is indicated by returning b''.
      raise _StdinSocket._EOFError
    if b is None:
      # Regardless of what the online python3 documentation says, it actually
      # returns None to indicate no nonblocking data available.
      b = b''
    return b


class _BaseIapTunnelHelper(object):
  """Base helper class for starting IAP tunnel."""

  def __init__(self, args, project, zone, instance, interface, port):
    self._project = project
    self._zone = zone
    self._instance = instance
    self._interface = interface
    self._port = port
    self._iap_tunnel_url_override = args.iap_tunnel_url_override
    self._ignore_certs = args.iap_tunnel_insecure_disable_websocket_cert_check
    # Means that a ctrl-c was seen in server mode (never true in Stdin mode).
    self._shutdown = False

  def _InitiateWebSocketConnection(self, local_conn, get_access_token_callback):
    tunnel_target = self._GetTunnelTargetInfo()
    new_websocket = iap_tunnel_websocket.IapTunnelWebSocket(
        tunnel_target, get_access_token_callback,
        functools.partial(_SendLocalDataCallback, local_conn),
        functools.partial(_CloseLocalConnectionCallback, local_conn),
        ignore_certs=self._ignore_certs)
    new_websocket.InitiateConnection()
    return new_websocket

  def _GetTunnelTargetInfo(self):
    proxy_info = http_proxy.GetHttpProxyInfo()
    if callable(proxy_info):
      proxy_info = proxy_info(method='https')
    return utils.IapTunnelTargetInfo(project=self._project,
                                     zone=self._zone,
                                     instance=self._instance,
                                     interface=self._interface,
                                     port=self._port,
                                     url_override=self._iap_tunnel_url_override,
                                     proxy_info=proxy_info)

  def _RunReceiveLocalData(self, conn, socket_address):
    """Receive data from provided local connection and send over WebSocket.

    Args:
      conn: A socket or _StdinSocket representing the local connection.
      socket_address: A verbose loggable string describing where conn is
        connected to.
    """

    websocket_conn = None
    try:
      websocket_conn = self._InitiateWebSocketConnection(
          conn,
          functools.partial(_GetAccessTokenCallback,
                            store.LoadIfEnabled(use_google_auth=True)))
      while not self._shutdown:
        data = conn.recv(utils.SUBPROTOCOL_MAX_DATA_FRAME_SIZE)
        if not data:
          # When we recv an EOF, we notify the websocket_conn of it, then we
          # wait for all data to send before returning.
          websocket_conn.LocalEOF()
          if not websocket_conn.WaitForAllSent():
            log.warning('Failed to send all data from [%s].', socket_address)
          break
        websocket_conn.Send(data)
    finally:
      if self._shutdown:
        log.info('Terminating connection to [%s].', socket_address)
      else:
        log.info('Client closed connection from [%s].', socket_address)
      try:
        conn.close()
      except EnvironmentError:
        pass
      try:
        if websocket_conn:
          websocket_conn.Close()
      except (EnvironmentError, exceptions.Error):
        pass


class IapTunnelProxyServerHelper(_BaseIapTunnelHelper):
  """Proxy server helper listens on a port for new local connections."""

  def __init__(self, args, project, zone, instance, interface, port, local_host,
               local_port):
    super(IapTunnelProxyServerHelper, self).__init__(
        args, project, zone, instance, interface, port)
    self._local_host = local_host
    self._local_port = local_port
    self._server_sockets = []
    self._connections = []

  def __del__(self):
    self._CloseServerSockets()

  def StartProxyServer(self):
    """Start accepting connections."""
    self._TestConnection()
    self._server_sockets = _OpenLocalTcpSockets(self._local_host,
                                                self._local_port)
    log.out.Print('Listening on port [%d].' % self._local_port)

    try:
      with execution_utils.RaisesKeyboardInterrupt():
        while True:
          self._connections.append(self._AcceptNewConnection())
    except KeyboardInterrupt:
      log.info('Keyboard interrupt received.')
    finally:
      self._CloseServerSockets()

    self._shutdown = True
    self._CloseClientConnections()
    log.status.Print('Server shutdown complete.')

  def _TestConnection(self):
    log.status.Print('Testing if tunnel connection works.')
    websocket_conn = self._InitiateWebSocketConnection(
        None,
        functools.partial(_GetAccessTokenCallback,
                          store.LoadIfEnabled(use_google_auth=True)))
    websocket_conn.Close()

  def _AcceptNewConnection(self):
    """Accept a new socket connection and start a new WebSocket tunnel."""
    # Python socket accept() on Windows does not get interrupted by ctrl-c
    # To work around that, use select() with a timeout before the accept()
    # which allows for the ctrl-c to be noticed and abort the process as
    # expected.
    ready_sockets = [()]
    while not ready_sockets[0]:
      # 0.2 second timeout
      ready_sockets = select.select(self._server_sockets, (), (), 0.2)

    ready_read_sockets = ready_sockets[0]
    conn, socket_address = ready_read_sockets[0].accept()
    new_thread = threading.Thread(target=self._HandleNewConnection,
                                  args=(conn, socket_address))
    new_thread.daemon = True
    new_thread.start()
    return new_thread, conn

  def _CloseServerSockets(self):
    log.debug('Stopping server.')
    try:
      for server_socket in self._server_sockets:
        server_socket.close()
    except EnvironmentError:
      pass

  def _CloseClientConnections(self):
    """Close client connections that seem to still be open."""
    if self._connections:
      close_count = 0
      for client_thread, conn in self._connections:
        if client_thread.isAlive():
          close_count += 1
          try:
            conn.close()
          except EnvironmentError:
            pass
      if close_count:
        log.status.Print('Closed [%d] local connection(s).' % close_count)

  def _HandleNewConnection(self, conn, socket_address):
    try:
      self._RunReceiveLocalData(conn, repr(socket_address))
    except EnvironmentError as e:
      log.info('Socket error [%s] while receiving from client.',
               six.text_type(e))
    except:  # pylint: disable=bare-except
      log.exception('Error while receiving from client.')


class IapTunnelStdinHelper(_BaseIapTunnelHelper):
  """Facilitates a connection that gets local data from stdin."""

  def Run(self):
    """Executes the tunneling of data."""
    try:
      with execution_utils.RaisesKeyboardInterrupt():
        self._RunReceiveLocalData(_StdinSocket(), 'stdin')
    except KeyboardInterrupt:
      log.info('Keyboard interrupt received.')
