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
"""Command to run an Airflow CLI sub-command in an environment."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import argparse
import re

from googlecloudsdk.api_lib.composer import environments_util as environments_api_util
from googlecloudsdk.api_lib.composer import util as api_util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.composer import image_versions_util as image_versions_command_util
from googlecloudsdk.command_lib.composer import resource_args
from googlecloudsdk.command_lib.composer import util as command_util
from googlecloudsdk.core import log
from googlecloudsdk.core.console import console_io

WORKER_POD_SUBSTR = 'worker'
WORKER_CONTAINER = 'airflow-worker'
DEPRECATION_WARNING = ('Because Cloud Composer manages the Airflow metadata '
                       'database for your environment, support for the Airflow '
                       '`{}` subcommand is being deprecated. '
                       'To avoid issues related to Airflow metadata, we '
                       'recommend that you do not use this subcommand unless '
                       'you understand the outcome.')


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Run(base.Command):
  """Run an Airflow sub-command remotely in a Cloud Composer environment.

  Executes an Airflow CLI sub-command remotely in an environment. If the
  sub-command takes flags, separate the environment name from the sub-command
  and its flags with ``--''. This command waits for the sub-command to
  complete; its exit code will match the sub-command's exit code.

  ## EXAMPLES

    The following command:

    {command} myenv trigger_dag -- some_dag --run_id=foo

  is equivalent to running the following command from a shell inside the
  *my-environment* environment:

    airflow trigger_dag some_dag --run_id=foo
  """

  @staticmethod
  def Args(parser):
    resource_args.AddEnvironmentResourceArg(
        parser, 'in which to run an Airflow command')

    parser.add_argument(
        'subcommand',
        metavar='SUBCOMMAND',
        choices=command_util.SUBCOMMAND_WHITELIST,
        help=('The Airflow CLI subcommand to run. Available subcommands '
              'include: {} (see https://airflow.apache.org/cli.html for more '
              'info). Note that delete_dag is available from Airflow 1.10.1, '
              'and list_dag_runs, next_execution are available from Airflow '
              '1.10.2.').format(', '.join(command_util.SUBCOMMAND_WHITELIST)))
    parser.add_argument(
        'cmd_args',
        metavar='CMD_ARGS',
        nargs=argparse.REMAINDER,
        help='Command line arguments to the subcommand.',
        example='{command} myenv trigger_dag -- some_dag --run_id=foo')

  def BypassConfirmationPrompt(self, args, airflow_version):
    """Bypasses confirmations with "yes" responses.

    Prevents certain Airflow CLI subcommands from presenting a confirmation
    prompting (which can hang the gcloud CLI). When necessary, bypass
    confirmations with a "yes" response.

    Args:
      args: argparse.Namespace, An object that contains the values for the
        arguments specified in the .Args() method.
      airflow_version: String, an Airflow semantic version.
    """
    # Value is the lowest Airflow version for which this command needs to bypass
    # the confirmation prompt.
    prompting_subcommands = {'delete_dag': None, 'backfill': '1.10.6'}
    if args.subcommand in prompting_subcommands \
        and (prompting_subcommands[args.subcommand] is None \
             or image_versions_command_util.CompareVersions(
                 airflow_version,
                 prompting_subcommands[args.subcommand]) >= 0) \
        and set(args.cmd_args).isdisjoint({'-y', '--yes'}):
      args.cmd_args.append('--yes')

  def DeprecationWarningPrompt(self, args):
    response = True
    if args.subcommand in command_util.SUBCOMMAND_DEPRECATION:
      response = console_io.PromptContinue(
          message=DEPRECATION_WARNING.format(args.subcommand),
          default=False, cancel_on_no=True)
    return response

  def ConvertKubectlError(self, error, env_obj):
    del env_obj  # Unused argument.
    return error

  def _ExtractAirflowVersion(self, image_version):
    return re.findall(r'-airflow-([\d\.]+)', image_version)[0]

  def Run(self, args):
    self.DeprecationWarningPrompt(args)

    running_state = (
        api_util.GetMessagesModule(release_track=self.ReleaseTrack())
        .Environment.StateValueValuesEnum.RUNNING)

    env_ref = args.CONCEPTS.environment.Parse()
    env_obj = environments_api_util.Get(
        env_ref, release_track=self.ReleaseTrack())

    if env_obj.state != running_state:
      raise command_util.Error(
          'Cannot execute subcommand for environment in state {}. '
          'Must be RUNNING.'.format(env_obj.state))

    cluster_id = env_obj.config.gkeCluster
    cluster_location_id = command_util.ExtractGkeClusterLocationId(env_obj)

    tty = 'no-tty' not in args

    with command_util.TemporaryKubeconfig(cluster_location_id, cluster_id):
      try:
        image_version = env_obj.config.softwareConfig.imageVersion
        airflow_version = self._ExtractAirflowVersion(image_version)
        kubectl_ns = command_util.FetchKubectlNamespace(image_version)
        pod = command_util.GetGkePod(
            pod_substr=WORKER_POD_SUBSTR, kubectl_namespace=kubectl_ns)

        log.status.Print(
            'Executing within the following Kubernetes cluster namespace: '
            '{}'.format(kubectl_ns))

        self.BypassConfirmationPrompt(args, airflow_version)
        kubectl_args = ['exec', pod, '--stdin']
        if tty:
          kubectl_args.append('--tty')
        kubectl_args.extend(
            ['--container', WORKER_CONTAINER, '--', 'airflow', args.subcommand])
        if args.cmd_args:
          kubectl_args.extend(args.cmd_args)

        command_util.RunKubectlCommand(
            command_util.AddKubectlNamespace(kubectl_ns, kubectl_args),
            out_func=log.out.Print)
      except command_util.KubectlError as e:
        raise self.ConvertKubectlError(e, env_obj)


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class RunBeta(Run):
  """Run an Airflow sub-command remotely in a Cloud Composer environment.

  Executes an Airflow CLI sub-command remotely in an environment. If the
  sub-command takes flags, separate the environment name from the sub-command
  and its flags with ``--''. This command waits for the sub-command to
  complete; its exit code will match the sub-command's exit code.

  ## EXAMPLES

    The following command:

    {command} myenv trigger_dag -- some_dag --run_id=foo

  is equivalent to running the following command from a shell inside the
  *my-environment* environment:

    airflow trigger_dag some_dag --run_id=foo
  """

  def ConvertKubectlError(self, error, env_obj):
    is_private = (
        env_obj.config.privateEnvironmentConfig and
        env_obj.config.privateEnvironmentConfig.enablePrivateEnvironment)
    if is_private:
      return command_util.Error(
          str(error) +
          ' Make sure you have followed https://cloud.google.com/composer/docs/how-to/accessing/airflow-cli#running_commands_on_a_private_ip_environment '
          'to enable access to your private Cloud Composer environment from '
          'your machine.')
    return error
