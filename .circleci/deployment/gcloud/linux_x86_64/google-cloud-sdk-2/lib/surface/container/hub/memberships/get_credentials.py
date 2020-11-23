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
"""Fetch Hub-registered cluster credentials for Connect Gateway."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.cloudresourcemanager import projects_api
from googlecloudsdk.api_lib.container import util
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.container.hub import api_util as hubapi_util
from googlecloudsdk.command_lib.container.hub import gwkubeconfig_util as kconfig
from googlecloudsdk.command_lib.projects import util as project_util
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.core import log
from googlecloudsdk.core import properties

KUBECONTEXT_FORMAT = 'connectgateway_{project}_{membership}'
SERVER_FORMAT = 'https://{service_name}/{version}/projects/{project_number}/memberships/{membership}'
REQUIRED_PERMISSIONS = [
    'gkehub.memberships.get',
    'gkehub.gateway.get',
]


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class GetCredentials(base.Command):
  """Fetch credentials for a Hub-registered cluster to be used in Connect Gateway.

  {command} updates the `kubeconfig` file with the appropriate credentials and
  endpoint information to send `kubectl` commands to a Hub-registered and
  connected cluster through Connect Gateway Service.

  It takes a project, passed through by set defaults or flags. By default,
  credentials are written to `$HOME/.kube/config`. You can provide an alternate
  path by setting the `KUBECONFIG` environment variable. If `KUBECONFIG`
  contains multiple paths, the first one is used.

  Upon success, this command will switch current context to the target cluster,
  when working with multiple clusters.
  """

  @classmethod
  def Args(cls, parser):
    parser.add_argument(
        'MEMBERSHIP',
        type=str,
        help=textwrap.dedent("""\
          The membership name used to locate a cluster in your project. """),
    )

  def Run(self, args):
    util.CheckKubectlInstalled()
    log.status.Print('Starting to build Gateway kubeconfig...')
    # Parse Args: get project_id from flag or default
    project_id = arg_utils.GetFromNamespace(
        args, '--project', use_defaults=True)
    log.status.Print('Current project_id: ' + project_id)

    self.RunIamCheck(project_id)
    self.ReadClusterMembership(project_id, args.MEMBERSHIP)
    self.GenerateKubeconfig(project_id, args.MEMBERSHIP)

  # Run IAM check, make sure caller has permission to use Gateway API.
  def RunIamCheck(self, project_id):
    project_ref = project_util.ParseProject(project_id)
    result = projects_api.TestIamPermissions(project_ref, REQUIRED_PERMISSIONS)
    granted_permissions = result.permissions

    if set(REQUIRED_PERMISSIONS) != set(granted_permissions):
      raise Exception("caller doesn't have sufficient permission.")

  def ReadClusterMembership(self, project_id, membership):
    resource_name = hubapi_util.MembershipRef(project_id, 'global', membership)
    # If membership doesn't exist, exception will be raised to caller.
    hubapi_util.GetMembership(resource_name)

  def GenerateKubeconfig(self, project_id, membership):
    project_number = project_util.GetProjectNumber(project_id)
    kwargs = {
        'membership':
            membership,
        'project_id':
            project_id,
        'server':
            SERVER_FORMAT.format(
                service_name=self.get_service_name(),
                version=self.GetVersion(),
                project_number=project_number,
                membership=membership),
        'auth_provider':
            'gcp',
    }
    user_kwargs = {
        'auth_provider': 'gcp',
    }

    cluster_kwargs = {}
    context = KUBECONTEXT_FORMAT.format(
        project=project_id, membership=membership)
    kubeconfig = kconfig.Kubeconfig.Default()
    # Use same key for context, cluster, and user.
    kubeconfig.contexts[context] = kconfig.Context(context, context, context)
    kubeconfig.users[context] = kconfig.User(context, **user_kwargs)
    kubeconfig.clusters[context] = kconfig.Cluster(context, kwargs['server'],
                                                   **cluster_kwargs)
    kubeconfig.SetCurrentContext(context)
    kubeconfig.SaveToFile()
    return kubeconfig

  def get_service_name(self):
    # This function checks environment endpoint overidden configuration for
    # gkehub. The overridden value will be like this:
    # https://autopush-gkehub.sandbox.googleapis.com/.

    # When there is no overridden set, this command will run against Hub prod
    # endpoint and return an empty string. When the
    # overrideen value is  https://autopush-gkehub.sandbox.googleapis.com/,
    # the Gateway's server address in generated kubeconfig will be
    # https://autopush-connectgateway.googleapis.com as a result.

    endpoint_overrides = properties.VALUES.api_endpoint_overrides.AllValues()
    hub_endpoint_override = endpoint_overrides.get('gkehub', '')
    if not hub_endpoint_override:
      # hub_endpoint_override will be empty string for Prod.
      return 'connectgateway.googleapis.com'
    elif 'autopush-gkehub' in hub_endpoint_override:
      return 'autopush-connectgateway.sandbox.googleapis.com'
    elif 'staging-gkehub' in hub_endpoint_override:
      return 'staging-connectgateway.sandbox.googleapis.com'
    else:
      raise Exception('Unknown api_endpoint_overrides for gkehub.')

  @classmethod
  def GetVersion(cls):
    if cls.ReleaseTrack() is base.ReleaseTrack.ALPHA:
      return 'v1alpha1'
    elif cls.ReleaseTrack() is base.ReleaseTrack.BETA:
      return 'v1beta1'
    elif cls.ReleaseTrack() is base.ReleaseTrack.GA:
      return 'v1'
    else:
      return ''
