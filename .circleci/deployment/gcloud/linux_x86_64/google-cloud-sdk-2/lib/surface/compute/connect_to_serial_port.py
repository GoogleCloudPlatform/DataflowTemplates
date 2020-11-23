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

"""Connects to a serial port gateway using SSH."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import sys

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute import ssh_utils
from googlecloudsdk.command_lib.compute.instances import flags as instance_flags
from googlecloudsdk.command_lib.util.ssh import ssh
from googlecloudsdk.core import http
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import encoding
from six.moves import http_client as httplib


SERIAL_PORT_GATEWAY = 'ssh-serialport.googleapis.com'
CONNECTION_PORT = '9600'
HOST_KEY_URL = ('https://cloud-certs.storage.googleapis.com/'
                'google-cloud-serialport-host-key.pub')
DEFAULT_HOST_KEY = ('ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDkOOCaBZVTxzvjJ7+7'
                    'YonnZOwIZ2Z7azwPC+oHbBCbWNBZAwzs63JQlHLibHG6NiNunFwP/lWs'
                    '5SpLx5eEdxGL+WQmvtldnBdqJzNE1UHrxPDegysCXxn1fT7KELpLozLh'
                    'vlfSnWJXbFbDrGB0bTv2U373Zo3BL9XTRf3qthdDEMF3GouUH8pGvitH'
                    'lwcwO1ulpVB0sTIdB7Bu+YPuo1XSuL2n3tXA9n9S+7kQCoyuXodeBpJo'
                    'JxzdJeoQXAepLrLA7nl6jRiYZyic0WJeSJm7vmvl1VDAGkyXloNEhBnv'
                    'oQFQl5aCwcS8UQnzzwMDflQ+JgsynYN08dLIRGcwkJe9')
SERIAL_PORT_HELP = ('https://cloud.google.com/compute/docs/'
                    'instances/interacting-with-serial-console')


class ConnectToSerialPort(base.Command):
  """Connect to the serial port of an instance.

  *{command}* allows users to connect to, and interact with, a VM's
  virtual serial port using ssh as the secure, authenticated transport
  protocol.

  The user must first enable serial port access to a given VM by setting
  the 'serial-port-enable=true' metadata key-value pair. Setting
  'serial-port-enable' on the project-level metadata enables serial port
  access to all VMs in the project.

  This command uses the same SSH key pair as the `gcloud compute ssh`
  command and also ensures that the user's public SSH key is present in
  the project's metadata. If the user does not have a public SSH key,
  one is generated using ssh-keygen.

  ## EXAMPLES
  To connect to the serial port of the instance 'my-instance' in zone
  'us-central1-f', run:

    $ {command} my-instance --zone=us-central1-f
  """

  category = base.TOOLS_CATEGORY

  @staticmethod
  def Args(parser):
    """Set up arguments for this command.

    Args:
      parser: An argparse.ArgumentParser.
    """
    ssh_utils.BaseSSHHelper.Args(parser)

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help=('If provided, the ssh command is printed to standard out '
              'rather than being executed.'))

    parser.add_argument(
        'user_host',
        completer=completers.InstancesCompleter,
        metavar='[USER@]INSTANCE',
        help="""\
        Specifies the user/instance for the serial port connection.

        ``USER'' specifies the username to authenticate as. If omitted,
        the current OS user is selected.
        """)

    parser.add_argument(
        '--port',
        default=1,
        help="""\
        The number of the requested serial port. Can be 1-4, default is 1.

        Instances can support up to four serial ports. By default, this
        command will connect to the first serial port. Setting this flag
        will connect to the requested serial port.
        """)

    parser.add_argument(
        '--extra-args',
        type=arg_parsers.ArgDict(min_length=1),
        default={},
        metavar='KEY=VALUE',
        help="""\
        Optional arguments can be passed to the serial port connection by
        passing key-value pairs to this flag, such as max-connections=N or
        replay-lines=N. See {0} for additional options.
        """.format(SERIAL_PORT_HELP))

    parser.add_argument(
        '--serial-port-gateway',
        hidden=True,
        help='THIS ARGUMENT NEEDS HELP TEXT.',
        default=SERIAL_PORT_GATEWAY)

    flags.AddZoneFlag(
        parser,
        resource_type='instance',
        operation_type='connect to')

    ssh_utils.AddSSHKeyExpirationArgs(parser)

  def Run(self, args):
    """See ssh_utils.BaseSSHCommand.Run."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    ssh_helper = ssh_utils.BaseSSHHelper()
    ssh_helper.Run(args)
    ssh_helper.keys.EnsureKeysExist(args.force_key_file_overwrite,
                                    allow_passphrase=True)

    remote = ssh.Remote.FromArg(args.user_host)
    if not remote:
      raise ssh_utils.ArgumentError(
          'Expected argument of the form [USER@]INSTANCE. Received [{0}].'
          .format(args.user_host))
    if not remote.user:
      remote.user = ssh.GetDefaultSshUsername()
    public_key = ssh_helper.keys.GetPublicKey().ToEntry(include_comment=True)

    hostname = '[{0}]:{1}'.format(args.serial_port_gateway, CONNECTION_PORT)
    # Update google_compute_known_hosts file with published host key
    if args.serial_port_gateway == SERIAL_PORT_GATEWAY:
      http_client = http.Http()
      http_response = http_client.request(HOST_KEY_URL)
      known_hosts = ssh.KnownHosts.FromDefaultFile()
      if int(http_response[0]['status']) == httplib.OK:
        host_key = encoding.Decode(http_response[1]).strip()
        known_hosts.Add(hostname, host_key, overwrite=True)
        known_hosts.Write()
      elif known_hosts.ContainsAlias(hostname):
        log.warning(
            'Unable to download and update Host Key for [{0}] from [{1}]. '
            'Attempting to connect using existing Host Key in [{2}]. If '
            'the connection fails, please try again to update the Host '
            'Key.'.format(SERIAL_PORT_GATEWAY, HOST_KEY_URL,
                          known_hosts.file_path))
      else:
        known_hosts.Add(hostname, DEFAULT_HOST_KEY)
        known_hosts.Write()
        log.warning(
            'Unable to download Host Key for [{0}] from [{1}]. To ensure '
            'the security of the SSH connection, gcloud will attempt to '
            'connect using a hard-coded Host Key value. If the connection '
            'fails, please try again. If the problem persists, try '
            'updating gcloud and connecting again.'
            .format(SERIAL_PORT_GATEWAY, HOST_KEY_URL))
    instance_ref = instance_flags.SSH_INSTANCE_RESOLVER.ResolveResources(
        [remote.host],
        compute_scope.ScopeEnum.ZONE,
        args.zone,
        holder.resources,
        scope_lister=instance_flags.GetInstanceZoneScopeLister(client))[0]
    instance = ssh_helper.GetInstance(client, instance_ref)
    project = ssh_helper.GetProject(client, instance_ref.project)
    expiration, expiration_micros = ssh_utils.GetSSHKeyExpirationFromArgs(args)

    remote.user, use_os_login = ssh.CheckForOsloginAndGetUser(
        instance, project, remote.user, public_key, expiration_micros,
        self.ReleaseTrack())

    # Determine the serial user, host tuple (remote)
    port = 'port={0}'.format(args.port)
    constructed_username_list = [instance_ref.project, instance_ref.zone,
                                 instance_ref.Name(), remote.user, port]
    if args.extra_args:
      for k, v in args.extra_args.items():
        constructed_username_list.append('{0}={1}'.format(k, v))
    serial_user = '.'.join(constructed_username_list)
    serial_remote = ssh.Remote(args.serial_port_gateway, user=serial_user)

    identity_file = ssh_helper.keys.key_file
    options = ssh_helper.GetConfig(hostname, strict_host_key_checking='yes')
    del options['HostKeyAlias']
    options['ControlPath'] = 'none'
    cmd = ssh.SSHCommand(serial_remote, identity_file=identity_file,
                         port=CONNECTION_PORT,
                         options=options)
    if args.dry_run:
      log.out.Print(' '.join(cmd.Build(ssh_helper.env)))
      return
    if not use_os_login:
      ssh_helper.EnsureSSHKeyExists(
          client, remote.user, instance, project, expiration)

    # Don't wait for the instance to become SSHable. We are not connecting to
    # the instance itself through SSH, so the instance doesn't need to have
    # fully booted to connect to the serial port. Also, ignore exit code 255,
    # since the normal way to terminate the serial port connection is ~. and
    # that causes ssh to exit with 255.
    try:
      return_code = cmd.Run(
          ssh_helper.env,
          force_connect=properties.VALUES.ssh.putty_force_connect.GetBool())
    except ssh.CommandError:
      return_code = 255
    if return_code:
      sys.exit(return_code)
