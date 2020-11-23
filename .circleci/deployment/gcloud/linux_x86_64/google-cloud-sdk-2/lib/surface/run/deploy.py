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
"""Deploy a container to Cloud Run."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import enum
import os.path
import uuid

from googlecloudsdk.api_lib.run import traffic
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as c_exceptions
from googlecloudsdk.command_lib.builds import flags as build_flags
from googlecloudsdk.command_lib.run import config_changes
from googlecloudsdk.command_lib.run import connection_context
from googlecloudsdk.command_lib.run import flags
from googlecloudsdk.command_lib.run import messages_util
from googlecloudsdk.command_lib.run import pretty_print
from googlecloudsdk.command_lib.run import resource_args
from googlecloudsdk.command_lib.run import resource_change_validators
from googlecloudsdk.command_lib.run import serverless_operations
from googlecloudsdk.command_lib.run import stages
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import progress_tracker


def GetAllowUnauth(args, operations, service_ref, service_exists):
  """Returns allow_unauth value for a service change.

  Args:
    args: argparse.Namespace, Command line arguments
    operations: serverless_operations.ServerlessOperations, Serverless client.
    service_ref: protorpc.messages.Message, A resource reference object for the
      service See googlecloudsdk.core.resources.Registry.ParseResourceId for
      details.
    service_exists: True if the service being changed already exists.

  Returns:
    allow_unauth value where
     True means to enable unauthenticated acess for the service.
     False means to disable unauthenticated access for the service.
     None means to retain the current value for the service.
  """
  allow_unauth = None
  if flags.GetPlatform() == flags.PLATFORM_MANAGED:
    allow_unauth = flags.GetAllowUnauthenticated(args, operations, service_ref,
                                                 not service_exists)
    # Avoid failure removing a policy binding for a service that
    # doesn't exist.
    if not service_exists and not allow_unauth:
      allow_unauth = None
  return allow_unauth


class BuildType(enum.Enum):
  DOCKERFILE = 'Dockerfile'
  BUILDPACKS = 'Buildpacks'


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Deploy(base.Command):
  """Deploy a container to Cloud Run."""

  detailed_help = {
      'DESCRIPTION':
          """\
          Deploys container images to Google Cloud Run.
          """,
      'EXAMPLES':
          """\
          To deploy a container to the service `my-backend` on Cloud Run:

              $ {command} my-backend --image=gcr.io/my/image

          You may also omit the service name. Then a prompt will be displayed
          with a suggested default value:

              $ {command} --image=gcr.io/my/image

          To deploy to Cloud Run on Kubernetes Engine, you need to specify a cluster:

              $ {command} --image=gcr.io/my/image --cluster=my-cluster
          """,
  }

  @staticmethod
  def CommonArgs(parser):
    # Flags specific to managed CR
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddAllowUnauthenticatedFlag(managed_group)
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
        'Service to deploy to.',
        required=True,
        prefixes=False)
    flags.AddFunctionArg(parser)
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
    flags.AddClientNameAndVersionFlags(parser)
    concept_parsers.ConceptParser([service_presentation]).AddToParser(parser)
    # No output by default, can be overridden by --format
    parser.display_info.AddFormat('none')

  @staticmethod
  def Args(parser):
    Deploy.CommonArgs(parser)
    flags.AddImageArg(parser)

    # Flags only supported on GKE and Knative
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddMinInstancesFlag(cluster_group)
    flags.AddEndpointVisibilityEnum(cluster_group)

  def Run(self, args):
    """Deploy a container to Cloud Run."""
    service_ref = flags.GetService(args)
    build_type = None
    image = None
    pack = None
    source = None
    include_build = flags.FlagIsExplicitlySet(args, 'source')
    operation_message = 'Deploying container'
    # Build an image from source if source specified
    if include_build:
      # Create a tag for the image creation
      source = args.source
      if not args.IsSpecified('image'):
        args.image = 'gcr.io/{projectID}/cloud-run-source-deploy/{service}:{tag}'.format(
            projectID=properties.VALUES.core.project.Get(required=True),
            service=service_ref.servicesId,
            tag=uuid.uuid4().hex)
      # Use GCP Buildpacks if Dockerfile doesn't exist
      docker_file = args.source + '/Dockerfile'
      if os.path.exists(docker_file):
        build_type = BuildType.DOCKERFILE
      else:
        pack = [{'image': args.image}]
        build_type = BuildType.BUILDPACKS
      image = None if pack else args.image
      operation_message = 'Building using {build_type} and deploying container'.format(
          build_type=build_type.value)
    elif not args.IsSpecified('image'):
      raise c_exceptions.RequiredArgumentException(
          '--image', 'Requires a container image to deploy (e.g. '
          '`gcr.io/cloudrun/hello:latest`) if no build source is provided.')
    # Deploy a container with an image
    conn_context = connection_context.GetConnectionContext(
        args, flags.Product.RUN, self.ReleaseTrack())
    changes = flags.GetConfigurationChanges(args)
    changes.append(
        config_changes.SetLaunchStageAnnotationChange(self.ReleaseTrack()))

    with serverless_operations.Connect(conn_context) as operations:
      service = operations.GetService(service_ref)
      allow_unauth = GetAllowUnauth(args, operations, service_ref, service)
      resource_change_validators.ValidateClearVpcConnector(service, args)

      pretty_print.Info(
          messages_util.GetStartDeployMessage(conn_context, service_ref,
                                              operation_message))
      has_latest = (
          service is None or
          traffic.LATEST_REVISION_KEY in service.spec_traffic)
      deployment_stages = stages.ServiceStages(
          include_iam_policy_set=allow_unauth is not None,
          include_route=has_latest,
          include_build=include_build)
      header = None
      if include_build:
        header = 'Building and deploying'
      else:
        header = 'Deploying'
      if service is None:
        header += ' new service'
      header += '...'
      with progress_tracker.StagedProgressTracker(
          header,
          deployment_stages,
          failure_message='Deployment failed',
          suppress_output=args.async_) as tracker:
        service = operations.ReleaseService(
            service_ref,
            changes,
            tracker,
            asyn=args.async_,
            allow_unauthenticated=allow_unauth,
            prefetch=service,
            build_image=image,
            build_pack=pack,
            build_source=source)

      if args.async_:
        pretty_print.Success('Service [{{bold}}{serv}{{reset}}] is deploying '
                             'asynchronously.'.format(serv=service.name))
      else:
        service = operations.GetService(service_ref)
        pretty_print.Success(
            messages_util.GetSuccessMessageForSynchronousDeploy(service))
      return service


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class BetaDeploy(Deploy):
  """Deploy a container to Cloud Run."""

  @staticmethod
  def Args(parser):
    Deploy.CommonArgs(parser)
    flags.AddImageArg(parser)

    # Flags specific to managed CR
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddEgressSettingsFlag(managed_group)

    # Flags only supported on GKE and Knative
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddEndpointVisibilityEnum(cluster_group)

    # Flags not specific to any platform
    flags.AddMinInstancesFlag(parser)
    flags.AddDeployTagFlag(parser)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class AlphaDeploy(Deploy):
  """Deploy a container to Cloud Run."""

  @staticmethod
  def Args(parser):
    Deploy.CommonArgs(parser)

    # Flags specific to VPCAccess
    managed_group = flags.GetManagedArgGroup(parser)
    flags.AddEgressSettingsFlag(managed_group)

    # Flags specific to connecting to a cluster
    cluster_group = flags.GetClusterArgGroup(parser)
    flags.AddEndpointVisibilityEnum(cluster_group, deprecated=True)

    # Flags not specific to any platform
    flags.AddMinInstancesFlag(parser)
    flags.AddDeployTagFlag(parser)
    flags.AddIngressFlag(parser)

    # Flags inherited from gcloud builds submit
    flags.AddConfigFlags(parser)
    flags.AddSourceFlag(parser)
    flags.AddBuildTimeoutFlag(parser)
    # TODO(b/165145546): Remove advanced build flags for 'gcloud run deploy'
    build_flags.AddGcsSourceStagingDirFlag(parser, True)
    build_flags.AddGcsLogDirFlag(parser, True)
    build_flags.AddMachineTypeFlag(parser, True)
    build_flags.AddDiskSizeFlag(parser, True)
    build_flags.AddSubstitutionsFlag(parser, True)
    build_flags.AddWorkerPoolFlag(parser, True)
    build_flags.AddNoCacheFlag(parser, True)
    build_flags.AddIgnoreFileFlag(parser, True)


AlphaDeploy.__doc__ = Deploy.__doc__
