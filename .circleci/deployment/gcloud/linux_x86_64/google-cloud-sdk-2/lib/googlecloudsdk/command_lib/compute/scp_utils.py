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

"""Base class for commands copying files from and to virtual machines."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from argcomplete.completers import FilesCompleter

from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute import iap_tunnel
from googlecloudsdk.command_lib.compute import scope as compute_scope
from googlecloudsdk.command_lib.compute import ssh_utils
from googlecloudsdk.command_lib.compute.instances import flags as instance_flags
from googlecloudsdk.command_lib.util.ssh import ip
from googlecloudsdk.command_lib.util.ssh import ssh
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import retry


class BaseScpHelper(ssh_utils.BaseSSHCLIHelper):
  """Copy files to and from Google Compute Engine virtual machines."""

  @staticmethod
  def Args(parser):
    """Set up arguments for this command.

    Args:
      parser: An argparse.ArgumentParser.
    """
    super(BaseScpHelper, BaseScpHelper).Args(parser)

    parser.add_argument(
        'sources',
        completer=FilesCompleter,
        help='Specifies the files to copy.',
        metavar='[[USER@]INSTANCE:]SRC',
        nargs='+')

    parser.add_argument(
        'destination',
        help='Specifies a destination for the source files.',
        metavar='[[USER@]INSTANCE:]DEST')

    # TODO(b/21515936): Use flags.AddZoneFlag when copy_files supports URIs.
    parser.add_argument(
        '--zone',
        action=actions.StoreProperty(properties.VALUES.compute.zone),
        help=('The zone of the instance to copy files to/from.\n\n' +
              flags.ZONE_PROPERTY_EXPLANATION))

  def RunScp(self,
             compute_holder,
             args,
             port=None,
             recursive=False,
             compress=False,
             extra_flags=None,
             release_track=None,
             ip_type=ip.IpTypeEnum.EXTERNAL):
    """SCP files between local and remote GCE instance.

    Run this method from subclasses' Run methods.

    Args:
      compute_holder: The ComputeApiHolder.
      args: argparse.Namespace, the args the command was invoked with.
      port: str or None, Port number to use for SSH connection.
      recursive: bool, Whether to use recursive copying using -R flag.
      compress: bool, Whether to use compression.
      extra_flags: [str] or None, extra flags to add to command invocation.
      release_track: obj, The current release track.
      ip_type: IpTypeEnum, Specify using internal ip or external ip address.

    Raises:
      ssh_utils.NetworkError: Network issue which likely is due to failure
        of SSH key propagation.
      ssh.CommandError: The SSH command exited with SSH exit code, which
        usually implies that a connection problem occurred.
    """
    if release_track is None:
      release_track = base.ReleaseTrack.GA
    super(BaseScpHelper, self).Run(args)

    dst = ssh.FileReference.FromPath(args.destination)
    srcs = [ssh.FileReference.FromPath(src) for src in args.sources]

    # Make sure we have a unique remote
    ssh.SCPCommand.Verify(srcs, dst, single_remote=True)

    remote = dst.remote or srcs[0].remote
    if not dst.remote:  # Make sure all remotes point to the same ref
      for src in srcs:
        src.remote = remote

    instance_ref = instance_flags.SSH_INSTANCE_RESOLVER.ResolveResources(
        [remote.host],
        compute_scope.ScopeEnum.ZONE,
        args.zone,
        compute_holder.resources,
        scope_lister=instance_flags.GetInstanceZoneScopeLister(
            compute_holder.client))[0]
    instance = self.GetInstance(compute_holder.client, instance_ref)
    project = self.GetProject(compute_holder.client, instance_ref.project)
    expiration, expiration_micros = ssh_utils.GetSSHKeyExpirationFromArgs(args)

    if remote.user:
      username_requested = True
    else:
      username_requested = False
      remote.user = ssh.GetDefaultSshUsername(warn_on_account_user=True)
    if args.plain:
      use_oslogin = False
    else:
      public_key = self.keys.GetPublicKey().ToEntry(include_comment=True)
      remote.user, use_oslogin = ssh.CheckForOsloginAndGetUser(
          instance, project, remote.user, public_key, expiration_micros,
          release_track, username_requested=username_requested)

    identity_file = None
    options = None
    if not args.plain:
      identity_file = self.keys.key_file
      options = self.GetConfig(ssh_utils.HostKeyAlias(instance),
                               args.strict_host_key_checking)

    iap_tunnel_args = iap_tunnel.SshTunnelArgs.FromArgs(
        args, release_track, instance_ref,
        ssh_utils.GetExternalInterface(instance, no_raise=True))

    if iap_tunnel_args:
      remote.host = ssh_utils.HostKeyAlias(instance)
    elif ip_type is ip.IpTypeEnum.INTERNAL:
      remote.host = ssh_utils.GetInternalIPAddress(instance)
    else:
      remote.host = ssh_utils.GetExternalIPAddress(instance)

    cmd = ssh.SCPCommand(
        srcs, dst, identity_file=identity_file, options=options,
        recursive=recursive, compress=compress, port=port,
        extra_flags=extra_flags, iap_tunnel_args=iap_tunnel_args)

    if args.dry_run:
      log.out.Print(' '.join(cmd.Build(self.env)))
      return

    if args.plain or use_oslogin:
      keys_newly_added = False
    else:
      keys_newly_added = self.EnsureSSHKeyExists(
          compute_holder.client,
          remote.user,
          instance,
          project,
          expiration=expiration)

    if keys_newly_added:
      poller = ssh_utils.CreateSSHPoller(remote, identity_file, options,
                                         iap_tunnel_args, port=port)

      log.status.Print('Waiting for SSH key to propagate.')
      # TODO(b/35355795): Don't force_connect
      try:
        poller.Poll(
            self.env,
            force_connect=properties.VALUES.ssh.putty_force_connect.GetBool())
      except retry.WaitException:
        raise ssh_utils.NetworkError()

    if ip_type is ip.IpTypeEnum.INTERNAL:
      # This will never happen when IAP Tunnel is enabled, because ip_type is
      # always EXTERNAL when IAP Tunnel is enabled, even if the instance has no
      # external IP. IAP Tunnel doesn't need verification because it uses
      # unambiguous identifiers for the instance.
      self.PreliminarilyVerifyInstance(instance.id, remote, identity_file,
                                       options)

    # Errors from the SCP command result in an ssh.CommandError being raised
    cmd.Run(
        self.env,
        force_connect=properties.VALUES.ssh.putty_force_connect.GetBool())
