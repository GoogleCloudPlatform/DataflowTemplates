# -*- coding: utf-8 -*- #
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Command for deleting a service."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from googlecloudsdk.api_lib.events import iam_util
from googlecloudsdk.api_lib.services import services_util
from googlecloudsdk.api_lib.services import serviceusage
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.events import eventflow_operations
from googlecloudsdk.command_lib.events import exceptions
from googlecloudsdk.command_lib.events import flags
from googlecloudsdk.command_lib.events import stages
from googlecloudsdk.command_lib.iam import iam_util as core_iam_util
from googlecloudsdk.command_lib.run import connection_context
from googlecloudsdk.command_lib.run import flags as serverless_flags
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.console import progress_tracker


_CONTROL_PLANE_NAMESPACE = 'cloud-run-events'
_CONTROL_PLANE_REQUIRED_SERVICES = [
    # cloudresourcemanager isn't required for eventing itself, but is required
    # for this command to perform the IAM bindings necessary.
    'cloudresourcemanager.googleapis.com',
    'cloudscheduler.googleapis.com',
    'logging.googleapis.com',
    'pubsub.googleapis.com',
    'stackdriver.googleapis.com',
    'storage-api.googleapis.com',
    'storage-component.googleapis.com',
]

ServiceAccountConfig = collections.namedtuple(
        'ServiceAccountConfig',
        ['arg_name', 'display_name', 'description', 'default_service_account',
         'recommended_roles', 'additional_wi_roles', 'secret_name'])

_CONTROL_PLANE_SERVICE_ACCOUNT_CONFIG = ServiceAccountConfig(
    arg_name='service_account',
    display_name='Cloud Run Events',
    description='Cloud Run Events on-cluster Infrastructure',
    default_service_account=iam_util.EVENTS_CONTROL_PLANE_SERVICE_ACCOUNT,
    recommended_roles=[
        # CloudSchedulerSource
        'roles/cloudscheduler.admin',
        # CloudAuditLogsSource
        'roles/logging.configWriter',
        # CloudAuditLogsSource
        'roles/logging.privateLogViewer',
        # All Sources
        'roles/pubsub.admin',
        # CloudStorageSource
        'roles/storage.admin',
    ],
    additional_wi_roles=[],
    secret_name='google-cloud-key',
)

_BROKER_SERVICE_ACCOUNT_CONFIG = ServiceAccountConfig(
    arg_name='broker_service_account',
    display_name='Cloud Run Events Broker',
    description='Cloud Run Events on-cluster Broker',
    default_service_account=iam_util.EVENTS_BROKER_SERVICE_ACCOUNT,
    recommended_roles=[
        'roles/pubsub.editor',
    ],
    additional_wi_roles=[
        'roles/monitoring.metricWriter',
        'roles/cloudtrace.agent',
    ],
    secret_name='google-broker-key',
)

_SOURCES_SERVICE_ACCOUNT_CONFIG = ServiceAccountConfig(
    arg_name='sources_service_account',
    display_name='Cloud Run Events Sources',
    description='Cloud Run Events on-cluster Sources',
    default_service_account=iam_util.EVENTS_SOURCES_SERVICE_ACCOUNT,
    recommended_roles=[
        'roles/pubsub.editor',
    ],
    additional_wi_roles=[
        'roles/monitoring.metricWriter',
        'roles/cloudtrace.agent',
    ],
    secret_name='google-cloud-sources-key',
)


class Init(base.Command):
  """Initialize a cluster for eventing."""

  detailed_help = {
      'DESCRIPTION': """
          {description}
          Enables necessary services for the project, adds necessary IAM policy
          bindings to the provided service account, and creates a new key for
          the provided service account.
          This command is only available with Cloud Run for Anthos.
          """,
      'EXAMPLES': """
          To initialize a cluster:

              $ {command}
          """,
  }

  @staticmethod
  def CommonArgs(parser):
    """Defines arguments common to all release tracks."""
    flags.AddControlPlaneServiceAccountFlag(parser)
    flags.AddBrokerServiceAccountFlag(parser)
    flags.AddSourcesServiceAccountFlag(parser)

  @staticmethod
  def Args(parser):
    Init.CommonArgs(parser)

  def Run(self, args):
    """Executes when the user runs the delete command."""
    if serverless_flags.GetPlatform() == serverless_flags.PLATFORM_MANAGED:
      raise exceptions.UnsupportedArgumentError(
          'This command is only available with Cloud Run for Anthos.')
    project = properties.VALUES.core.project.Get(required=True)
    conn_context = connection_context.GetConnectionContext(
        args, serverless_flags.Product.EVENTS, self.ReleaseTrack())

    with eventflow_operations.Connect(conn_context) as client:
      cloud_run_obj = client.GetCloudRun()
      if cloud_run_obj is None:
        pass
      elif cloud_run_obj.eventing_enabled:
        log.status.Print('Eventing already enabled.')
      else:
        tracker_stages = stages.EventingStages()

        # Enable eventing
        with progress_tracker.StagedProgressTracker(
            'Enabling eventing...',
            tracker_stages,
            failure_message='Failed to enable eventing') as tracker:
          response = client.UpdateCloudRunWithEventingEnabled()

          # Wait for Operator to enable eventing
          client.PollCloudRunResource(tracker)
          log.status.Print('Enabled eventing successfully.')

      if client.IsClusterInitialized():
        console_io.PromptContinue(
            message='This cluster has already been initialized.',
            prompt_string='Would you like to re-run initialization?',
            cancel_on_no=True)

      _EnableMissingServices(project)

      for sa_config in [_CONTROL_PLANE_SERVICE_ACCOUNT_CONFIG,
                        _BROKER_SERVICE_ACCOUNT_CONFIG,
                        _SOURCES_SERVICE_ACCOUNT_CONFIG]:
        _ConfigureServiceAccount(sa_config, client, args)

      client.MarkClusterInitialized()

    log.status.Print(_InitializedMessage(
        self.ReleaseTrack(), conn_context.cluster_name))


def _EnableMissingServices(project):
  """Enables any required services for the project."""
  enabled_services = set(
      service.config.name for service in
      serviceusage.ListServices(project, True, 100, None))
  missing_services = list(sorted(
      set(_CONTROL_PLANE_REQUIRED_SERVICES) - enabled_services))
  if not missing_services:
    return

  formatted_services = '\n'.join(
      ['- {}'.format(s) for s in missing_services])
  _PromptIfCanPrompt('\nThis will enable the following services:\n'
                     '{}'.format(formatted_services))
  if len(missing_services) == 1:
    op = serviceusage.EnableApiCall(project, missing_services[0])
  else:
    op = serviceusage.BatchEnableApiCall(project, missing_services)
  if not op.done:
    op = services_util.WaitOperation(op.name, serviceusage.GetOperation)
  log.status.Print('Services successfully enabled.')


def _ConfigureServiceAccount(sa_config, client, args):
  """Configures a service account for eventing."""

  log.status.Print('Configuring service account for {}.'.format(
      sa_config.description))
  if not args.IsSpecified(sa_config.arg_name):
    sa_email = iam_util.GetOrCreateServiceAccountWithPrompt(
        sa_config.default_service_account,
        sa_config.display_name,
        sa_config.description)
  else:
    sa_email = getattr(args, sa_config.arg_name)

  # We use projectsId of '-' to handle the case where a user-provided service
  # account may belong to a different project and we need to obtain a key for
  # that service account.
  #
  # The IAM utils used below which print or bind roles are implemented to
  # specifically operate on the current project and are not impeded by this
  # projectless ref.
  service_account_ref = resources.REGISTRY.Parse(
      sa_email,
      params={'projectsId': '-'},
      collection=core_iam_util.SERVICE_ACCOUNTS_COLLECTION)

  should_bind_roles = not args.IsSpecified(sa_config.arg_name)
  iam_util.PrintOrBindMissingRolesWithPrompt(
      service_account_ref, sa_config.recommended_roles, should_bind_roles)

  secret_ref = resources.REGISTRY.Parse(
      sa_config.secret_name,
      params={'namespacesId': _CONTROL_PLANE_NAMESPACE},
      collection='run.api.v1.namespaces.secrets',
      api_version='v1')

  _PromptIfCanPrompt(
      'This will create a new key for the service account [{}].'
      .format(sa_email))
  _, key_ref = client.CreateOrReplaceServiceAccountSecret(
      secret_ref, service_account_ref)
  log.status.Print('Added key [{}] to cluster for [{}].'.format(
      key_ref.Name(), sa_email))

  log.status.Print('Finished configuring service account for {}.\n'.format(
      sa_config.description))


def _InitializedMessage(release_track, cluster_name):
  command_prefix = 'gcloud '
  if release_track != base.ReleaseTrack.GA:
    command_prefix += release_track.prefix + ' '
  ns_init_command = command_prefix + ('events namespaces init '
                                      '--copy-default-secret')
  brokers_create_command = command_prefix + 'events brokers create default'
  return ('Initialized cluster [{}] for Cloud Run eventing. '
          'Next, initialize the namespace(s) you plan to use and '
          'create a broker via `{}` and `{}`.'.format(
              cluster_name,
              ns_init_command,
              brokers_create_command,
          ))


def _PromptIfCanPrompt(message):
  if console_io.CanPrompt():
    console_io.PromptContinue(message=message, cancel_on_no=True)
