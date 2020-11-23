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
"""The command to update Config Management Feature."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap
from googlecloudsdk.api_lib.util import apis as core_apis
from googlecloudsdk.command_lib.container.hub.features import base
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import properties
from googlecloudsdk.core import yaml
from googlecloudsdk.core.console import console_io

MEMBERSHIP_FLAG = '--membership'
CONFIG_YAML_FLAG = '--config'
membership = None


class Apply(base.UpdateCommand):
  r"""Update a Config Management Feature Spec.

  This command apply ConfigManagement CR with user-specified config yaml file.

  ## Examples

  Apply ConfigManagement yaml file:

    $ {command} --membership=CLUSTER_NAME \
    --config=/path/to/config-management.yaml
  """

  FEATURE_NAME = 'configmanagement'
  FEATURE_DISPLAY_NAME = 'Config Management'

  @classmethod
  def Args(cls, parser):
    parser.add_argument(
        MEMBERSHIP_FLAG,
        type=str,
        help=textwrap.dedent("""\
            The Membership name provided during registration.
            """),
    )
    parser.add_argument(
        CONFIG_YAML_FLAG,
        type=str,
        help=textwrap.dedent("""\
            The path to config-management.yaml.
            """),
        required=True)

  def Run(self, args):
    project = properties.VALUES.core.project.GetOrFail()
    memberships = base.ListMemberships(project)
    if not memberships:
      raise exceptions.Error('No Memberships available in Hub.')
    # User should choose an existing membership if not provide one
    global membership
    if not args.membership:
      index = console_io.PromptChoice(
          options=memberships,
          message='Please specify a membership to apply {}:\n'.format(
              args.config))
      membership = memberships[index]
    else:
      membership = args.membership
      if membership not in memberships:
        raise exceptions.Error(
            'Membership {} is not in Hub.'.format(membership))

    try:
      loaded_cm = yaml.load_path(args.config)
    except yaml.Error as e:
      raise exceptions.Error(
          'Invalid config yaml file {}'.format(args.config), e)
    _validate_meta(loaded_cm)

    client = core_apis.GetClientInstance('gkehub', 'v1alpha1')
    msg = client.MESSAGES_MODULE
    config_sync = _parse_config_sync(loaded_cm, msg)
    policy_controller = _parse_policy_controller(loaded_cm, msg)
    applied_config = msg.ConfigManagementFeatureSpec.MembershipConfigsValue.AdditionalProperty(
        key=membership,
        value=msg.MembershipConfig(
            configSync=config_sync,
            policyController=policy_controller))
    # UpdateFeature uses patch method to update membership_configs map,
    # there's no need to get the existing feature spec
    m_configs = msg.ConfigManagementFeatureSpec.MembershipConfigsValue(
        additionalProperties=[applied_config])
    self.RunCommand(
        'configmanagement_feature_spec.membership_configs',
        configmanagementFeatureSpec=msg.ConfigManagementFeatureSpec(
            membershipConfigs=m_configs))


def _validate_meta(configmanagement):
  """Validate the parsed configmanagement yaml.

  Args:
    configmanagement: The dict loaded from yaml.
  """
  if not isinstance(configmanagement, dict):
    raise exceptions.Error('Invalid ConfigManagement template.')
  if ('apiVersion' not in configmanagement or
      configmanagement['apiVersion'] != 'configmanagement.gke.io/v1'):
    raise exceptions.Error(
        'Only support "apiVersion: configmanagement.gke.io/v1"')
  if ('kind' not in configmanagement or
      configmanagement['kind'] != 'ConfigManagement'):
    raise exceptions.Error('Only support "kind: ConfigManagement"')
  if 'spec' not in configmanagement:
    raise exceptions.Error('Missing required field .spec')
  spec = configmanagement['spec']
  for field in spec:
    if field not in ['git', 'policyController', 'sourceFormat']:
      raise exceptions.Error(
          'Please remove illegal field .spec.{}'.format(field))
  if 'sourceFormat' in spec and configmanagement['spec'][
      'sourceFormat'] not in ['hierarchy', 'unstructured']:
    raise exceptions.Error('Please remove illegal value of .spec.sourceFormat')
  if 'git' in spec:
    for field in spec['git']:
      if field not in [
          'policyDir', 'proxy', 'secretType', 'syncBranch', 'syncRepo',
          'syncRev', 'syncWait'
      ]:
        raise exceptions.Error(
            'Please remove illegal field .spec.git.{}'.format(field))
    if 'proxy' in spec['git']:
      for field in spec['git']['proxy']:
        if field not in ['httpsProxy']:
          raise exceptions.Error(
              'Please remove illegal field .spec.git.proxy.{}'.format(field))


def _parse_config_sync(configmanagement, msg):
  """Load GitConfig with the parsed configmanagement yaml.

  Args:
    configmanagement: dict, The data loaded from the config-management.yaml
      given by user.
    msg: The empty message class for gkehub version v1alpha1
  Returns:
    config_sync: The ConfigSync configuration holds configmanagement.spec.git
    being used in MembershipConfigs
  Raises: Error, if required fields are missing from .spec.git
  """

  if('spec' not in configmanagement or 'git' not in configmanagement['spec']):
    return None
  spec_git = configmanagement['spec']['git']
  git_config = msg.GitConfig()
  config_sync = msg.ConfigSync(git=git_config)
  # https://cloud.google.com/anthos-config-management/docs/how-to/installing#configuring-git-repo
  # Required field
  for field in ['syncRepo', 'secretType']:
    if field not in spec_git:
      raise exceptions.Error('Missing required field [{}].'.format(field))
  for field in [
      'policyDir', 'secretType', 'syncBranch', 'syncRepo', 'syncRev'
  ]:
    if field in spec_git:
      setattr(git_config, field, spec_git[field])
  if 'syncWait' in spec_git:
    git_config.syncWaitSecs = spec_git['syncWait']
  if 'proxy' in spec_git and 'httpsProxy' in spec_git['proxy']:
    git_config.httpsProxy = spec_git['proxy']['httpsProxy']

  if 'sourceFormat' in configmanagement['spec']:
    config_sync.sourceFormat = configmanagement['spec']['sourceFormat']
  return config_sync


def _parse_policy_controller(configmanagement, msg):
  """Load PolicyController with the parsed config-management.yaml.

  Args:
    configmanagement: dict, The data loaded from the config-management.yaml
      given by user.
    msg: The empty message class for gkehub version v1alpha1
  Returns:
    policy_controller: The Policy Controller configuration for
    MembershipConfigs, filled in the data parsed from
    configmanagement.spec.policyController
  Raises: Error, if Policy Controller `enabled` set to false but also has
    other fields present in the config
  """

  if ('spec' not in configmanagement or
      'policyController' not in configmanagement['spec']):
    return None

  spec_policy_controller = configmanagement['spec']['policyController']
  # Required field
  if configmanagement['spec'][
      'policyController'] is None or 'enabled' not in spec_policy_controller:
    raise exceptions.Error(
        'Missing required field .spec.policyController.enabled')
  enabled = spec_policy_controller['enabled']
  if not isinstance(enabled, bool):
    raise exceptions.Error(
        'policyController.enabled should be `true` or `false`')
  if not enabled:
    if len(spec_policy_controller.items()) > 1:
      raise exceptions.Error('Policy Controller is disabled, '
                             'remove the other config fields.')
    return msg.PolicyController(enabled=False)

  policy_controller = msg.PolicyController(enabled=True)
  # When the policyController is set to be enabled, policy_controller will
  # be filled with the valid fields set in spec_policy_controller, which
  # were mapped from the config-management.yaml
  for field in spec_policy_controller:
    if field not in [
        'enabled', 'templateLibraryInstalled', 'auditIntervalSeconds',
        'referentialRulesEnabled', 'exemptableNamespaces'
    ]:
      raise exceptions.Error(
          'Please remove illegal field .spec.policyController.{}'.format(field))
    setattr(policy_controller, field, spec_policy_controller[field])

  return policy_controller
