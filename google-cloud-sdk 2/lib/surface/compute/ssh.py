# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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

"""Implements the command for SSHing into an instance."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import argparse
import sys

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute import iap_tunnel
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute import ssh_utils
from googlecloudsdk.command_lib.compute.instances import flags as instance_flags
from googlecloudsdk.command_lib.util.ssh import containers
from googlecloudsdk.command_lib.util.ssh import ssh
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import retry


def AddCommandArg(parser):
  parser.add_argument(
      '--command',
      help="""\
      A command to run on the virtual machine.

      Runs the command on the target instance and then exits.
      """)


def AddSSHArgs(parser):
  """Additional flags and positional args to be passed to *ssh(1)*."""
  parser.add_argument(
      '--ssh-flag',
      action='append',
      help="""\
      Additional flags to be passed to *ssh(1)*. It is recommended that flags
      be passed using an assignment operator and quotes. Example:

        $ {command} example-instance --zone=us-central1-a --ssh-flag="-vvv" --ssh-flag="-L 80:localhost:80"

      This flag will replace occurences of ``%USER%'' and ``%INSTANCE%'' with
      their dereferenced values. For example, passing ``80:%INSTANCE%:80`` into
      the flag is equivalent to passing ``80:162.222.181.197:80'' to *ssh(1)*
      if the external IP address of 'example-instance' is 162.222.181.197.

      If connecting to the instance's external IP, then %INSTANCE% is replaced
      with that, otherwise it is replaced with the internal IP.
      """)

  parser.add_argument(
      'user_host',
      completer=completers.InstancesCompleter,
      metavar='[USER@]INSTANCE',
      help="""\
      Specifies the instance to SSH into.

      ``USER'' specifies the username with which to SSH. If omitted,
      the user login name is used. If using OS Login, USER will be replaced
      by the OS Login user.

      ``INSTANCE'' specifies the name of the virtual machine instance to SSH
      into.
      """)

  parser.add_argument(
      'ssh_args',
      nargs=argparse.REMAINDER,
      help="""\
          Flags and positionals passed to the underlying ssh implementation.
          """,
      example="""\
        $ {command} example-instance --zone=us-central1-a -- -vvv -L 80:%INSTANCE%:80
      """)


def AddContainerArg(parser):
  parser.add_argument(
      '--container',
      help="""\
          The name or ID of a container inside of the virtual machine instance
          to connect to. This only applies to virtual machines that are using
          a Google Container-Optimized virtual machine image. For more
          information, see [](https://cloud.google.com/compute/docs/containers).
          """)


def AddInternalIPArg(group):
  group.add_argument(
      '--internal-ip',
      default=False,
      action='store_true',
      help="""\
        Connect to instances using their internal IP addresses rather than their
        external IP addresses. Use this to connect from one instance to another
        on the same VPC network, over a VPN connection, or between two peered
        VPC networks.

        For this connection to work, you must configure your networks and
        firewall to allow SSH connections to the internal IP address of
        the instance to which you want to connect.

        To learn how to use this flag, see
        [](https://cloud.google.com/compute/docs/instances/connecting-advanced#sshbetweeninstances).
        """)


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Ssh(base.Command):
  """SSH into a virtual machine instance."""

  category = base.TOOLS_CATEGORY

  @staticmethod
  def Args(parser):
    """Set up arguments for this command.

    Args:
      parser: An argparse.ArgumentParser.
    """
    ssh_utils.BaseSSHCLIHelper.Args(parser)
    AddCommandArg(parser)
    AddSSHArgs(parser)
    AddContainerArg(parser)
    flags.AddZoneFlag(
        parser, resource_type='instance', operation_type='connect to')
    ssh_utils.AddVerifyInternalIpArg(parser)

    routing_group = parser.add_mutually_exclusive_group()
    AddInternalIPArg(routing_group)
    iap_tunnel.AddSshTunnelArgs(parser, routing_group)

  def Run(self, args):
    """See ssh_utils.BaseSSHCLICommand.Run."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    ssh_helper = ssh_utils.BaseSSHCLIHelper()
    ssh_helper.Run(args)
    user, instance_name = ssh_utils.GetUserAndInstance(args.user_host)
    instance_ref = instance_flags.SSH_INSTANCE_RESOLVER.ResolveResources(
        [instance_name], compute_scope.ScopeEnum.ZONE, args.zone,
        holder.resources,
        scope_lister=instance_flags.GetInstanceZoneScopeLister(client))[0]
    instance = ssh_helper.GetInstance(client, instance_ref)
    project = ssh_helper.GetProject(client, instance_ref.project)
    host_keys = ssh_helper.GetHostKeysFromGuestAttributes(client, instance_ref,
                                                          instance, project)
    if not host_keys and host_keys is not None:
      # Only display this message if there was an attempt to retrieve
      # host keys but it was unsuccessful. If Guest Attributes is disabled,
      # there is no attempt to retrieve host keys.
      log.status.Print('Unable to retrieve host keys from instance metadata. '
                       'Continuing.')
    expiration, expiration_micros = ssh_utils.GetSSHKeyExpirationFromArgs(args)
    if args.plain:
      use_oslogin = False
    else:
      public_key = ssh_helper.keys.GetPublicKey().ToEntry(include_comment=True)
      # If there is an '@' symbol in the user_host arg, the user is requesting
      # to connect as a specific user. This may get overridden by OS Login.
      username_requested = '@' in args.user_host
      user, use_oslogin = ssh.CheckForOsloginAndGetUser(
          instance, project, user, public_key, expiration_micros,
          self.ReleaseTrack(), username_requested=username_requested)

    iap_tunnel_args = iap_tunnel.SshTunnelArgs.FromArgs(
        args, self.ReleaseTrack(), instance_ref,
        ssh_utils.GetExternalInterface(instance, no_raise=True))

    internal_address = ssh_utils.GetInternalIPAddress(instance)

    if iap_tunnel_args:
      # IAP Tunnel only uses instance_address for the purpose of --ssh-flag
      # substitution. In this case, dest_addr doesn't do much, it just matches
      # against entries in the user's ssh_config file. It's best to use
      # something unique to avoid false positive matches, thus we use
      # HostKeyAlias.
      instance_address = internal_address
      dest_addr = ssh_utils.HostKeyAlias(instance)
    elif args.internal_ip:
      instance_address = internal_address
      dest_addr = instance_address
    else:
      instance_address = ssh_utils.GetExternalIPAddress(instance)
      dest_addr = instance_address
    remote = ssh.Remote(dest_addr, user)

    identity_file = None
    options = None
    if not args.plain:
      identity_file = ssh_helper.keys.key_file
      options = ssh_helper.GetConfig(ssh_utils.HostKeyAlias(instance),
                                     args.strict_host_key_checking,
                                     host_keys_to_add=host_keys)

    extra_flags = ssh.ParseAndSubstituteSSHFlags(args, remote, instance_address,
                                                 internal_address)
    remainder = []

    if args.ssh_args:
      remainder.extend(args.ssh_args)

    # Transform args.command into arg list or None if no command
    command_list = args.command.split(' ') if args.command else None
    tty = containers.GetTty(args.container, command_list)
    remote_command = containers.GetRemoteCommand(args.container, command_list)

    # Do not include default port since that will prevent users from
    # specifying a custom port (b/121998342).
    ssh_cmd_args = {'remote': remote,
                    'identity_file': identity_file,
                    'options': options,
                    'extra_flags': extra_flags,
                    'remote_command': remote_command,
                    'tty': tty,
                    'iap_tunnel_args': iap_tunnel_args,
                    'remainder': remainder}

    cmd = ssh.SSHCommand(**ssh_cmd_args)

    if args.dry_run:
      log.out.Print(' '.join(cmd.Build(ssh_helper.env)))
      return

    if args.plain or use_oslogin:
      keys_newly_added = False
    else:
      keys_newly_added = ssh_helper.EnsureSSHKeyExists(
          client, remote.user, instance, project, expiration=expiration)

    if keys_newly_added:
      poller = ssh_utils.CreateSSHPoller(remote, identity_file, options,
                                         iap_tunnel_args,
                                         extra_flags=extra_flags)
      log.status.Print('Waiting for SSH key to propagate.')
      # TODO(b/35355795): Don't force_connect
      try:
        poller.Poll(
            ssh_helper.env,
            force_connect=properties.VALUES.ssh.putty_force_connect.GetBool())
      except retry.WaitException:
        raise ssh_utils.NetworkError()

    if args.internal_ip:
      ssh_helper.PreliminarilyVerifyInstance(instance.id, remote, identity_file,
                                             options)

    # Errors from SSH itself result in an ssh.CommandError being raised
    return_code = cmd.Run(
        ssh_helper.env,
        force_connect=properties.VALUES.ssh.putty_force_connect.GetBool())
    if return_code:
      # This is the return code of the remote command.  Problems with SSH itself
      # will result in ssh.CommandError being raised above.
      sys.exit(return_code)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class SshBeta(Ssh):
  """SSH into a virtual machine instance (Beta)."""


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class SshAlpha(SshBeta):
  """SSH into a virtual machine instance (Alpha)."""


def DetailedHelp():
  """Construct help text based on the command release track."""
  detailed_help = {
      'brief': 'SSH into a virtual machine instance',
      'DESCRIPTION': """\
        *{command}* is a thin wrapper around the *ssh(1)* command that
        takes care of authentication and the translation of the
        instance name into an IP address.

        Note, this command does not work when connecting to Windows VMs. To
        connect to a Windows instance using a command-line method, refer to this
        guide: https://cloud.google.com/compute/docs/instances/connecting-to-instance#windows_cli

        The default network comes preconfigured to allow ssh access to
        all VMs. If the default network was edited, or if not using the
        default network, you may need to explicitly enable ssh access by adding
        a firewall-rule:

          $ gcloud compute firewall-rules create --network=NETWORK \
            default-allow-ssh --allow=tcp:22

        {command} ensures that the user's public SSH key is present
        in the project's metadata. If the user does not have a public
        SSH key, one is generated using *ssh-keygen(1)* (if the `--quiet`
        flag is given, the generated key will have an empty passphrase).
        """,
      'EXAMPLES': """\
        To SSH into 'example-instance' in zone ``us-central1-a'', run:

          $ {command} example-instance --zone=us-central1-a

        You can also run a command on the virtual machine. For
        example, to get a snapshot of the guest's process tree, run:

          $ {command} example-instance --zone=us-central1-a --command="ps -ejH"

        If you are using the Google Container-Optimized virtual machine image,
        you can SSH into one of your containers with:

          $ {command} example-instance --zone=us-central1-a --container=CONTAINER

        You can limit the allowed time to ssh. For example, to allow a key to be
        used through 2019:

          $ {command} example-instance --zone=us-central1-a --ssh-key-expiration="2020-01-01T00:00:00:00Z"

        Or alternatively, allow access for the next two minutes:

          $ {command} example-instance --zone=us-central1-a --ssh-key-expire-after=2m
        """,
  }
  return detailed_help


Ssh.detailed_help = DetailedHelp()
