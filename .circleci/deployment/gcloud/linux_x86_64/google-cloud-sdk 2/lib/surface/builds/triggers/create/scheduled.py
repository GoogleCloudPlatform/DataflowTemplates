# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Create Scheduled trigger command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.cloudbuild import cloudbuild_util
from googlecloudsdk.api_lib.cloudbuild import trigger_config as trigger_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateScheduled(base.CreateCommand):
  """Create a build trigger that fires on a schedule."""

  detailed_help = {
      'EXAMPLES':
          """\
            To create a scheduled trigger for a CSR repository:

              $ {command} --name="my-trigger" --repo-uri="https://source.developers.google.com/p/projectid/r/repo" --repo-type="CLOUD_SOURCE_REPOSITORIES" --revision="refs/heads/master" --build-config="cloudbuild.yaml" --schedule="0 9 * * *" --time-zone="America/New_York"

            To create a scheduled trigger for a GitHub repository:

              $ {command} --name="my-trigger" --repo-uri="https://github.com/owner/repo" --repo-type="GITHUB" --revision="refs/heads/master" --build-config="cloudbuild.yaml" --schedule="0 9 * * *" --time-zone="America/New_York"
          """,
  }

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """

    parser.display_info.AddFormat("""
          table(
            name,
            createTime.date('%Y-%m-%dT%H:%M:%S%Oz', undefined='-'),
            status
          )
        """)

    trigger_config = parser.add_mutually_exclusive_group(required=True)

    # Allow trigger config to be specified on the command line or by file.
    trigger_config.add_argument(
        '--trigger-config',
        metavar='PATH',
        help="""\
Path to a YAML or JSON file containing the trigger configuration.

For more details, see: https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.triggers
""")

    # Trigger configuration
    flag_config = trigger_config.add_argument_group(
        help='Flag based trigger configuration')
    flag_config.add_argument(
        '--repo-uri', help='URI of the repository.', required=True)
    flag_config.add_argument(
        '--repo-type', help="""
Type of repository (CLOUD_SOURCE_REPOSITORIES or GITHUB)
""", required=True)
    flag_config.add_argument(
        '--revision', help="""
Revision to build (e.g. refs/heads/master).
For more details, see: https://git-scm.com/book/en/v2/Git-Tools-Revision-Selection
""", required=True)
    flag_config.add_argument(
        '--schedule', help="""
Cron unix format schedule. For more details, see: https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules?#defining_the_job_schedule.'
""", required=True)
    flag_config.add_argument(
        '--time-zone', help="""
Specifies the time zone to be used in interpreting the schedule.
The value of this field must be a time
zone name (e.g. America/New_York) from the TZ Database (http://en.wikipedia.org/wiki/Tz_database).

Note that some time zones include a provision for
daylight savings time. The rules for daylight saving time are
determined by the chosen tz. For UTC use the string "utc". If a
time zone is not specified, the default will be in UTC (also known
as GMT).
""")
    flag_config.add_argument(
        '--description', help='Build trigger description.')
    flag_config.add_argument('--name', help='Build trigger name.')

    trigger_utils.AddBuildFileConfigArgs(flag_config)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      Some value that we want to have printed later.
    """

    client = cloudbuild_util.GetClientInstance()
    messages = cloudbuild_util.GetMessagesModule()

    # Convert 'repo type' input string to proto.
    git_file_source_type = messages.GitFileSource.RepoTypeValueValuesEnum.UNKNOWN
    git_repo_source_type = messages.GitRepoSource.RepoTypeValueValuesEnum.UNKNOWN
    if args.repo_type == 'CLOUD_SOURCE_REPOSITORIES':
      git_file_source_type = messages.GitFileSource.RepoTypeValueValuesEnum.CLOUD_SOURCE_REPOSITORIES
      git_repo_source_type = messages.GitRepoSource.RepoTypeValueValuesEnum.CLOUD_SOURCE_REPOSITORIES
    elif args.repo_type == 'GITHUB':
      git_file_source_type = messages.GitFileSource.RepoTypeValueValuesEnum.GITHUB
      git_repo_source_type = messages.GitRepoSource.RepoTypeValueValuesEnum.GITHUB

    trigger = messages.BuildTrigger()
    if args.trigger_config:
      trigger = cloudbuild_util.LoadMessageFromPath(
          path=args.trigger_config,
          msg_type=messages.BuildTrigger,
          msg_friendly_name='build trigger config',
          skip_camel_case=['substitutions'])
    else:
      trigger = messages.BuildTrigger(
          name=args.name,
          description=args.description,
          cron=messages.CronConfig(
              schedule=args.schedule,
              timeZone=args.time_zone,
          ),
      )

      # Source To Build
      trigger.sourceToBuild = messages.GitRepoSource(
          uri=args.repo_uri,
          ref=args.revision,
          repoType=git_repo_source_type,
      )

      # Build Config
      if args.build_config:
        trigger.gitFileSource = messages.GitFileSource(
            path=args.build_config,
            uri=args.repo_uri,
            repoType=git_file_source_type,
            revision=args.revision,
        )

        trigger.substitutions = cloudbuild_util.EncodeTriggerSubstitutions(
            args.substitutions, messages)

    # Send the Create request
    project = properties.VALUES.core.project.Get(required=True)

    created_trigger = client.projects_triggers.Create(
        messages.CloudbuildProjectsTriggersCreateRequest(
            buildTrigger=trigger, projectId=project))

    trigger_resource = resources.REGISTRY.Parse(
        None,
        collection='cloudbuild.projects.triggers',
        api_version='v1',
        params={
            'projectId': project,
            'triggerId': created_trigger.id,
        })
    log.CreatedResource(trigger_resource)

    return created_trigger
