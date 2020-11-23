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
"""The gcloud app deploy command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import hashlib
import os

from googlecloudsdk.api_lib.app import appengine_api_client
from googlecloudsdk.api_lib.app import runtime_builders
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.app import deploy_util
from googlecloudsdk.core import log
from googlecloudsdk.core import properties

_DETAILED_HELP = {
    'brief': ('Deploy the local code and/or configuration of your app to App '
              'Engine.'),
    'DESCRIPTION':
        """\
        This command is used to deploy both code and configuration to the App
        Engine server. As an input it takes one or more ``DEPLOYABLES'' that
        should be uploaded.  A ``DEPLOYABLE'' can be a service's .yaml file or a
        configuration's .yaml file (for more information about configuration
        files specific to your App Engine environment, refer to
        [](https://cloud.google.com/appengine/docs/standard/python/configuration-files)
        or [](https://cloud.google.com/appengine/docs/flexible/python/configuration-files)).
        Note, for Java8 Standard apps, you must add the path to the
        `appengine-web.xml` file inside the WEB-INF directory. {command}
        skips files specified in the .gcloudignore file (see `gcloud topic
        gcloudignore` for more information).
        For Java11 Standard, you can either use the yaml file, a Maven pom.xml, or a Gradle build.gradle. Alternatively, if the
        application is a single self-contained jar, you can give the path to the
        jar and a simple service configuration will be generated. You can deploy
        Java11 Maven source projects by specifying the location of your
        project's pom.xml file, and it will be built and deployed using
        App Engine Buildpacks.
        """,
    'EXAMPLES':
        """\
        To deploy a single service, run:

          $ {command} ~/my_app/app.yaml

        To deploy an App Engine Standard Java8 service, run:

          $ {command} ~/my_app/WEB-INF/appengine-web.xml

        To deploy an App Engine Standard Java11 single jar, run:

          $ {command} ~/my_app/my_jar.jar

        To deploy an App Engine Standard Java11 Maven source project, run:

          $ {command} ~/my_app/pom.xml

        To deploy an App Engine Standard Java11 Gradle source project, run:

          $ {command} ~/my_app/build.gradle

        By default, the service is deployed the current project configured via:

          $ gcloud config set core/project PROJECT

        To override this value for a single deployment, use the ``--project''
        flag:

          $ {command} ~/my_app/app.yaml --project=PROJECT

        To deploy multiple services, run:

          $ {command} ~/my_app/app.yaml ~/my_app/another_service.yaml

        To change the default --promote behavior for your current
        environment, run:

          $ gcloud config set app/promote_by_default false
        """,
}


@base.ReleaseTracks(base.ReleaseTrack.GA)
class DeployGA(base.SilentCommand):
  """Deploy the local code and/or configuration of your app to App Engine."""

  @staticmethod
  def Args(parser):
    """Get arguments for this command."""
    deploy_util.ArgsDeploy(parser)

  def Run(self, args):
    runtime_builder_strategy = deploy_util.GetRuntimeBuilderStrategy(
        base.ReleaseTrack.GA)
    api_client = appengine_api_client.GetApiClientForTrack(self.ReleaseTrack())
    if (runtime_builder_strategy !=
        runtime_builders.RuntimeBuilderStrategy.NEVER and
        self._ServerSideExperimentEnabled()):
      flex_image_build_option_default = deploy_util.FlexImageBuildOptions.ON_SERVER
    else:
      flex_image_build_option_default = deploy_util.FlexImageBuildOptions.ON_CLIENT
    return deploy_util.RunDeploy(
        args,
        api_client,
        runtime_builder_strategy=runtime_builder_strategy,
        parallel_build=False,
        flex_image_build_option=deploy_util.GetFlexImageBuildOption(
            default_strategy=flex_image_build_option_default))

  def _ServerSideExperimentEnabled(self):
    """Evaluates whether the build on server-side experiment is enabled for the project.

      The experiment is enabled for a project if the sha256 hash of the
      projectID mod 100 is smaller than the current experiment rollout percent.

    Returns:
      false if the experiment is not enabled for this project or the
      experiment config cannot be read due to an error
    """
    runtimes_builder_root = properties.VALUES.app.runtime_builders_root.Get(
        required=True)
    try:
      experiment_config = runtime_builders.Experiments.LoadFromURI(
          runtimes_builder_root)
      experiment_percent = experiment_config.GetExperimentPercentWithDefault(
          runtime_builders.Experiments.TRIGGER_BUILD_SERVER_SIDE, 0)
      project_hash = int(
          hashlib.sha256(
              properties.VALUES.core.project.Get().encode('utf-8')).hexdigest(),
          16) % 100
      return project_hash < experiment_percent
    except runtime_builders.ExperimentsError as e:
      log.debug(
          'Experiment config file could not be read. This error is '
          'informational, and does not cause a deployment to fail. '
          'Reason: %s' % e, exc_info=True)
      return False


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class DeployBeta(base.SilentCommand):
  """Deploy the local code and/or configuration of your app to App Engine."""

  @staticmethod
  def Args(parser):
    """Get arguments for this command."""
    deploy_util.ArgsDeploy(parser)
    parser.add_argument(
        '--use-ct-apis',
        action='store_true',
        default=False,
        hidden=True,
        help=('Use Cloud Tasks APIs instead of admin-console-hr for only '
              'queue.yaml uploads.'))

  def Run(self, args):
    runtime_builder_strategy = deploy_util.GetRuntimeBuilderStrategy(
        base.ReleaseTrack.BETA)
    api_client = appengine_api_client.GetApiClientForTrack(self.ReleaseTrack())

    # If the hidden flag `--use-ct-apis` is not set, then continue to use old
    # implementation which uses admin-console-hr.
    if not args.use_ct_apis:
      return deploy_util.RunDeploy(
          args,
          api_client,
          use_beta_stager=True,
          runtime_builder_strategy=runtime_builder_strategy,
          parallel_build=True,
          flex_image_build_option=deploy_util.GetFlexImageBuildOption(
              default_strategy=deploy_util.FlexImageBuildOptions.ON_SERVER),
          dispatch_admin_api=True)

    app_engine_legacy_deployables = []
    cloud_tasks_api_deployables = []
    cloud_scheduler_api_deployables = []
    for deployable in args.deployables:
      if os.path.basename(deployable) == 'queue.yaml':
        cloud_tasks_api_deployables.append(deployable)
      elif os.path.basename(deployable) == 'cron.yaml':
        cloud_scheduler_api_deployables.append(deployable)
      else:
        app_engine_legacy_deployables.append(deployable)

    resources = {'versions': [], 'configs': []}
    if (
        app_engine_legacy_deployables or
        not (cloud_tasks_api_deployables or cloud_scheduler_api_deployables)
    ):
      args.deployables = app_engine_legacy_deployables
      resources = deploy_util.RunDeploy(
          args,
          api_client,
          use_beta_stager=True,
          runtime_builder_strategy=runtime_builder_strategy,
          parallel_build=True,
          flex_image_build_option=deploy_util.GetFlexImageBuildOption(
              default_strategy=deploy_util.FlexImageBuildOptions.ON_SERVER),
          dispatch_admin_api=True)
    if cloud_tasks_api_deployables:
      args.deployables = cloud_tasks_api_deployables
      deploy_util.RunDeployCloudTasks(args)
      resources['configs'].append('queue')
    if cloud_scheduler_api_deployables:
      args.deployables = cloud_scheduler_api_deployables
      deploy_util.RunDeployCloudScheduler(args)
      resources['configs'].append('cron')
    return resources


DeployGA.detailed_help = _DETAILED_HELP
DeployBeta.detailed_help = _DETAILED_HELP
