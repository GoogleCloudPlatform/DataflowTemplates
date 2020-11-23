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
"""Command for updating env vars and other configuration info."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.run import traffic
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.run import config_changes
from googlecloudsdk.command_lib.run import connection_context
from googlecloudsdk.command_lib.run import exceptions
from googlecloudsdk.command_lib.run import flags
from googlecloudsdk.command_lib.run import messages_util
from googlecloudsdk.command_lib.run import pretty_print
from googlecloudsdk.command_lib.run import resource_args
from googlecloudsdk.command_lib.run import resource_change_validators
from googlecloudsdk.command_lib.run import serverless_operations
from googlecloudsdk.command_lib.run import stages
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core.console import progress_tracker


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Update(base.Command):
  """Update Cloud Run environment variables and other configuration settings."""

  detailed_help = {
      'DESCRIPTION':
          """\
          {description}
          """,
      'EXAMPLES':
          """\
          To update one or more env vars:

              $ {command} myservice --update-env-vars=KEY1=VALUE1,KEY2=VALUE2
         """,
  }

  @staticmethod
  def CommonArgs(parser):
    # Flags specific to managed CR
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddCloudSQLFlags(managed_group)
    flags.AddRevisionSuffixArg(managed_group)
    flags.AddVpcConnectorArg(managed_group)

    # Flags specific to connecting to a cluster
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddSecretsFlags(cluster_group)
    flags.AddConfigMapsFlags(cluster_group)
    flags.AddHttp2Flag(cluster_group)

    # Flags not specific to any platform
    service_presentation = presentation_specs.ResourcePresentationSpec(
        'SERVICE',
        resource_args.GetServiceResourceSpec(prompt=True),
        'Service to update the configuration of.',
        required=True,
        prefixes=False)
    flags.AddMutexEnvVarsFlags(parser)
    flags.AddMemoryFlag(parser)
    flags.AddConcurrencyFlag(parser)
    flags.AddTimeoutFlag(parser)
    flags.AddAsyncFlag(parser)
    flags.AddLabelsFlags(parser)
    flags.AddMaxInstancesFlag(parser)
    flags.AddCommandFlag(parser)
    flags.AddArgsFlag(parser)
    flags.AddPortFlag(parser)
    flags.AddCpuFlag(parser)
    flags.AddNoTrafficFlag(parser)
    flags.AddServiceAccountFlag(parser)
    flags.AddImageArg(parser, required=False)
    flags.AddClientNameAndVersionFlags(parser)
    concept_parsers.ConceptParser([service_presentation]).AddToParser(parser)
    # No output by default, can be overridden by --format
    parser.display_info.AddFormat('none')

  @staticmethod
  def Args(parser):
    Update.CommonArgs(parser)

    # Flags only supported on GKE and Knative
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddMinInstancesFlag(cluster_group)
    flags.AddEndpointVisibilityEnum(cluster_group)

  def Run(self, args):
    """Update the service resource.

       Different from `deploy` in that it can only update the service spec but
       no IAM or Cloud build changes.

    Args:
      args: Args!
    Returns:
      googlecloudsdk.api_lib.run.Service, the updated service
    """
    changes = flags.GetConfigurationChanges(args)
    if not changes or (len(changes) == 1 and isinstance(
        changes[0], config_changes.SetClientNameAndVersionAnnotationChange)):
      raise exceptions.NoConfigurationChangeError(
          'No configuration change requested. '
          'Did you mean to include the flags `--update-env-vars`, '
          '`--memory`, `--concurrency`, `--timeout`, `--connectivity`, '
          '`--image`?')
    changes.append(
        config_changes.SetLaunchStageAnnotationChange(self.ReleaseTrack()))

    conn_context = connection_context.GetConnectionContext(
        args, flags.Product.RUN, self.ReleaseTrack())
    service_ref = flags.GetService(args)

    with serverless_operations.Connect(conn_context) as client:
      service = client.GetService(service_ref)
      resource_change_validators.ValidateClearVpcConnector(service, args)
      has_latest = (
          service is None or
          traffic.LATEST_REVISION_KEY in service.spec_traffic)
      deployment_stages = stages.ServiceStages(
          include_iam_policy_set=False, include_route=has_latest)
      with progress_tracker.StagedProgressTracker(
          'Deploying...',
          deployment_stages,
          failure_message='Deployment failed',
          suppress_output=args.async_) as tracker:
        service = client.ReleaseService(
            service_ref, changes, tracker, asyn=args.async_, prefetch=service)

      if args.async_:
        pretty_print.Success('Service [{{bold}}{serv}{{reset}}] is deploying '
                             'asynchronously.'.format(serv=service.name))
      else:
        service = client.GetService(service_ref)
        pretty_print.Success(
            messages_util.GetSuccessMessageForSynchronousDeploy(service))
      return service


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class BetaUpdate(Update):
  """Update Cloud Run environment variables and other configuration settings."""

  @staticmethod
  def Args(parser):
    Update.CommonArgs(parser)

    # Flags specific to managed CR
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddEgressSettingsFlag(managed_group)

    # Flags only supported on GKE and Knative
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddEndpointVisibilityEnum(cluster_group)

    # Flags not specific to any platform
    flags.AddDeployTagFlag(parser)
    flags.AddMinInstancesFlag(parser)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class AlphaUpdate(Update):
  """Update Cloud Run environment variables and other configuration settings."""

  @staticmethod
  def Args(parser):
    Update.CommonArgs(parser)

    # Flags specific to managed CR
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddEgressSettingsFlag(managed_group)

    # Flags only supported on GKE and Knative
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddEndpointVisibilityEnum(cluster_group, deprecated=True)

    # Flags not specific to any platform
    flags.AddMinInstancesFlag(parser)
    flags.AddDeployTagFlag(parser)
    flags.AddIngressFlag(parser)


AlphaUpdate.__doc__ = Update.__doc__
