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
"""A library that used to interact with CTD-IA backend services."""

from apitools.base.py import exceptions
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import exceptions as calliope_exceptions
from googlecloudsdk.command_lib.scc.settings import exceptions as scc_exceptions
from googlecloudsdk.core import properties

API_NAME = 'securitycenter'
DEFAULT_API_VERSION = 'v1beta2'

SERVICES_ENDPOINTS = {
    'container-threat-detection': 'containerThreatDetectionSettings',
    'event-threat-detection': 'eventThreatDetectionSettings',
    'security-health-analytics': 'securityHealthAnalyticsSettings',
    'web-security-scanner': 'webSecurityScannerSettings',
}

SERVICE_STATUS_MASK = 'service_enablement_state'
MODULE_STATUS_MASK = 'modules'


def GetMessages(version=DEFAULT_API_VERSION):
  """Import and return the securitycenter settings message_module module.

  Args:
    version: the API version

  Returns:
    securitycenter settings message module.
  """
  return apis.GetMessagesModule(API_NAME, version)


def GetClient(version=DEFAULT_API_VERSION):
  """Import and return the securitycenter settings client module.

  Args:
    version: the API version

  Returns:
    securitycenter settings API client module.
  """
  return apis.GetClientInstance(API_NAME, version)


def GenerateParent(args):
  if args.organization:
    return 'organizations/{}/'.format(args.organization)
  elif args.project:
    return 'projects/{}/'.format(args.project)
  elif args.folder:
    return 'folders/{}/'.format(args.folder)


def FallBackFlags(args):
  if (not args.organization and not args.folder and not args.project):
    args.organization = properties.VALUES.scc.organization.Get()
    if not args.organization:
      args.project = properties.VALUES.core.project.Get()
  if (not args.organization and not args.folder and not args.project):
    raise calliope_exceptions.MinimumArgumentException(
        ['--organization', '--folder', '--project'])


class SettingsClient(object):
  """Client for securitycenter settings service."""

  def __init__(self, api_version=DEFAULT_API_VERSION):
    self.message_module = GetMessages(api_version)
    self.service_client = GetClient(api_version)

  def DescribeExplicit(self, args):
    """Describe settings of organization."""

    path = GenerateParent(args) + 'securityCenterSettings'

    try:
      request_message = self.message_module.SecuritycenterOrganizationsGetSecurityCenterSettingsRequest(
          name=path)
      return self.service_client.organizations.GetSecurityCenterSettings(
          request_message)
    except exceptions.HttpNotFoundError:
      raise scc_exceptions.SecurityCenterSettingsException(
          'Invalid argument {}'.format(path))

  def DescribeServiceExplicit(self, args):
    """Describe effective service settings of organization/folder/project."""

    FallBackFlags(args)
    path = GenerateParent(args) + SERVICES_ENDPOINTS[args.service]

    try:
      if args.organization:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterOrganizationsGetWebSecurityScannerSettingsRequest(
              name=path)
          return self.service_client.organizations.GetWebSecurityScannerSettings(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterOrganizationsGetSecurityHealthAnalyticsSettingsRequest(
              name=path)
          return self.service_client.organizations.GetSecurityHealthAnalyticsSettings(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterOrganizationsGetContainerThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.organizations.GetContainerThreatDetectionSettings(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterOrganizationsGetEventThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.organizations.GetEventThreatDetectionSettings(
              request_message)
      elif args.project:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterProjectsGetWebSecurityScannerSettingsRequest(
              name=path)
          return self.service_client.projects.GetWebSecurityScannerSettings(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterProjectsGetSecurityHealthAnalyticsSettingsRequest(
              name=path)
          return self.service_client.projects.GetSecurityHealthAnalyticsSettings(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterProjectsGetContainerThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.projects.GetContainerThreatDetectionSettings(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterProjectsGetEventThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.projects.GetEventThreatDetectionSettings(
              request_message)
      elif args.folder:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterFoldersGetWebSecurityScannerSettingsRequest(
              name=path)
          return self.service_client.folders.GetWebSecurityScannerSettings(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterFoldersGetSecurityHealthAnalyticsSettingsRequest(
              name=path)
          return self.service_client.folders.GetSecurityHealthAnalyticsSettings(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterFoldersGetContainerThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.folders.GetContainerThreatDetectionSettings(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterFoldersGetEventThreatDetectionSettingsRequest(
              name=path)
          return self.service_client.folders.GetEventThreatDetectionSettings(
              request_message)
    except exceptions.HttpError:
      # TODO(b/152617502): handle 404 error instead of general HttpError
      raise scc_exceptions.SecurityCenterSettingsException(
          'Invalid argument {}'.format(path))

  def DescribeService(self, args):
    """Describe service settings of organization/folder/project."""

    FallBackFlags(args)
    path = GenerateParent(args) + SERVICES_ENDPOINTS[args.service]

    try:
      if args.organization:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterOrganizationsWebSecurityScannerSettingsCalculateRequest(
              name=path)
          return self.service_client.organizations_webSecurityScannerSettings.Calculate(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterOrganizationsSecurityHealthAnalyticsSettingsCalculateRequest(
              name=path)
          return self.service_client.organizations_securityHealthAnalyticsSettings.Calculate(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterOrganizationsContainerThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.organizations_containerThreatDetectionSettings.Calculate(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterOrganizationsEventThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.organizations_eventThreatDetectionSettings.Calculate(
              request_message)
      elif args.project:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterProjectsWebSecurityScannerSettingsCalculateRequest(
              name=path)
          return self.service_client.projects_webSecurityScannerSettings.Calculate(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterProjectsSecurityHealthAnalyticsSettingsCalculateRequest(
              name=path)
          return self.service_client.projects_securityHealthAnalyticsSettings.Calculate(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterProjectsContainerThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.projects_containerThreatDetectionSettings.Calculate(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterProjectsEventThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.projects_eventThreatDetectionSettings.Calculate(
              request_message)
      elif args.folder:
        if args.service == 'web-security-scanner':
          request_message = self.message_module.SecuritycenterFoldersWebSecurityScannerSettingsCalculateRequest(
              name=path)
          return self.service_client.folders_webSecurityScannerSettings.Calculate(
              request_message)
        elif args.service == 'security-health-analytics':
          request_message = self.message_module.SecuritycenterFoldersSecurityHealthAnalyticsSettingsCalculateRequest(
              name=path)
          return self.service_client.folders_securityHealthAnalyticsSettings.Calculate(
              request_message)
        elif args.service == 'container-threat-detection':
          request_message = self.message_module.SecuritycenterFoldersContainerThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.folders_containerThreatDetectionSettings.Calculate(
              request_message)
        elif args.service == 'event-threat-detection':
          request_message = self.message_module.SecuritycenterFoldersEventThreatDetectionSettingsCalculateRequest(
              name=path)
          return self.service_client.folders_eventThreatDetectionSettings.Calculate(
              request_message)
    except exceptions.HttpNotFoundError:
      raise scc_exceptions.SecurityCenterSettingsException(
          'Invalid argument {}'.format(path))

  def EnableService(self, args):
    """Enable service of organization/folder/project."""
    if args.service == 'web-security-scanner':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          serviceEnablementState=self.message_module.WebSecurityScannerSettings
          .ServiceEnablementStateValueValuesEnum.ENABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'security-health-analytics':
      security_health_analytics_settings = self.message_module.SecurityHealthAnalyticsSettings(
          serviceEnablementState=self.message_module
          .SecurityHealthAnalyticsSettings.ServiceEnablementStateValueValuesEnum
          .ENABLED)
      return self._UpdateService(args, security_health_analytics_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'container-threat-detection':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          serviceEnablementState=self.message_module.WebSecurityScannerSettings
          .ServiceEnablementStateValueValuesEnum.ENABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'event-threat-detection':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          serviceEnablementState=self.message_module.WebSecurityScannerSettings
          .ServiceEnablementStateValueValuesEnum.ENABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)

  def DisableService(self, args):
    """Disable service of organization/folder/project."""
    if args.service == 'web-security-scanner':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          serviceEnablementState=self.message_module.WebSecurityScannerSettings
          .ServiceEnablementStateValueValuesEnum.DISABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'security-health-analytics':
      security_health_analytics_settings = self.message_module.SecurityHealthAnalyticsSettings(
          serviceEnablementState=self.message_module
          .SecurityHealthAnalyticsSettings.ServiceEnablementStateValueValuesEnum
          .DISABLED)
      return self._UpdateService(args, security_health_analytics_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'container-threat-detection':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          serviceEnablementState=self.message_module.WebSecurityScannerSettings
          .ServiceEnablementStateValueValuesEnum.DISABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)
    elif args.service == 'event-threat-detection':
      web_security_center_settings = self.message_module.WebSecurityScannerSettings(
          service_enablement_state=self.message_module
          .WebSecurityScannerSettings.ServiceEnablementStateValueValuesEnum
          .DISABLED)
      return self._UpdateService(args, web_security_center_settings,
                                 SERVICE_STATUS_MASK)

  def _UpdateService(self, args, service_settings, update_mask):
    """Update service settings of organization/folder/project."""

    FallBackFlags(args)
    path = GenerateParent(args) + SERVICES_ENDPOINTS[args.service]

    if args.service == 'web-security-scanner':
      if args.organization:
        request_message = self.message_module.SecuritycenterOrganizationsUpdateWebSecurityScannerSettingsRequest(
            name=path,
            updateMask=update_mask,
            webSecurityScannerSettings=service_settings)
        return self.service_client.organizations.UpdateWebSecurityScannerSettings(
            request_message)
      elif args.folder:
        request_message = self.message_module.SecuritycenterFoldersUpdateWebSecurityScannerSettingsRequest(
            name=path,
            updateMask=update_mask,
            webSecurityScannerSettings=service_settings)
        return self.service_client.folders.UpdateWebSecurityScannerSettings(
            request_message)
      elif args.project:
        request_message = self.message_module.SecuritycenterProjectsUpdateWebSecurityScannerSettingsRequest(
            name=path,
            updateMask=update_mask,
            webSecurityScannerSettings=service_settings)
        return self.service_client.projects.UpdateWebSecurityScannerSettings(
            request_message)
    elif args.service == 'security-health-analytics':
      if args.organization:
        request_message = self.message_module.SecuritycenterOrganizationsUpdateSecurityHealthAnalyticsSettingsRequest(
            name=path,
            updateMask=update_mask,
            securityHealthAnalyticsSettings=service_settings)
        return self.service_client.organizations.UpdateSecurityHealthAnalyticsSettings(
            request_message)
      elif args.folder:
        request_message = self.message_module.SecuritycenterFoldersUpdateSecurityHealthAnalyticsSettingsRequest(
            name=path,
            updateMask=update_mask,
            securityHealthAnalyticsSettings=service_settings)
        return self.service_client.folders.UpdateSecurityHealthAnalyticsSettings(
            request_message)
      elif args.project:
        request_message = self.message_module.SecuritycenterProjectsUpdateSecurityHealthAnalyticsSettingsRequest(
            name=path,
            updateMask=update_mask,
            securityHealthAnalyticsSettings=service_settings)
        return self.service_client.projects.UpdateSecurityHealthAnalyticsSettings(
            request_message)
    elif args.service == 'container-threat-detection':
      if args.organization:
        request_message = self.message_module.SecuritycenterOrganizationsUpdateContainerThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            containerThreatDetectionSettings=service_settings)
        return self.service_client.organizations.UpdateContainerThreatDetectionSettings(
            request_message)
      if args.folder:
        request_message = self.message_module.SecuritycenterFoldersUpdateContainerThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            containerThreatDetectionSettings=service_settings)
        return self.service_client.folders.UpdateContainerThreatDetectionSettings(
            request_message)
      if args.project:
        request_message = self.message_module.SecuritycenterProjectsUpdateContainerThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            containerThreatDetectionSettings=service_settings)
        return self.service_client.projects.UpdateContainerThreatDetectionSettings(
            request_message)
    elif args.service == 'event-threat-detection':
      if args.organization:
        request_message = self.message_module.SecuritycenterOrganizationsUpdateEventThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            eventThreatDetectionSettings=service_settings)
        return self.service_client.organizations.UpdateEventThreatDetectionSettings(
            request_message)
      elif args.folder:
        request_message = self.message_module.SecuritycenterFoldersUpdateEventThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            eventThreatDetectionSettings=service_settings)
        return self.service_client.folders.UpdateEventThreatDetectionSettings(
            request_message)
      elif args.project:
        request_message = self.message_module.SecuritycenterProjectsUpdateEventThreatDetectionSettingsRequest(
            name=path,
            updateMask=update_mask,
            eventThreatDetectionSettings=service_settings)
        return self.service_client.projects.UpdateEventThreatDetectionSettings(
            request_message)

  def EnableModule(self, args):
    """Enable a module for a service of organization/folder/project."""
    return self._UpdateModules(args, True)

  def DisableModule(self, args):
    """Disable a module for a service of organization/folder/project."""
    return self._UpdateModules(args, False)

  def _UpdateModules(self, args, enabled):
    """Update modules within service settings."""
    state = self.message_module.Config.ModuleEnablementStateValueValuesEnum.ENABLED if enabled else self.message_module.Config.ModuleEnablementStateValueValuesEnum.DISABLED
    if args.service == 'web-security-scanner':
      settings = self.message_module.WebSecurityScannerSettings(
          modules=self.message_module.WebSecurityScannerSettings.ModulesValue(
              additionalProperties=[
                  self.message_module.WebSecurityScannerSettings.ModulesValue
                  .AdditionalProperty(
                      key=args.module,
                      value=self.message_module.Config(
                          moduleEnablementState=state))
              ]))
    elif args.service == 'security-health-analytics':
      settings = self.message_module.SecurityHealthAnalyticsSettings(
          modules=self.message_module.SecurityHealthAnalyticsSettings
          .ModulesValue(additionalProperties=[
              self.message_module.SecurityHealthAnalyticsSettings.ModulesValue
              .AdditionalProperty(
                  key=args.module,
                  value=self.message_module.Config(moduleEnablementState=state))
          ]))
    elif args.service == 'container-threat-detection':
      settings = self.message_module.ContainerThreatDetectionSettings(
          modules=self.message_module.ContainerThreatDetectionSettings
          .ModulesValue(additionalProperties=[
              self.message_module.ContainerThreatDetectionSettings.ModulesValue
              .AdditionalProperty(
                  key=args.module,
                  value=self.message_module.Config(moduleEnablementState=state))
          ]))
    elif args.service == 'event-threat-detection':
      settings = self.message_module.EventThreatDetectionSettings(
          modules=self.message_module.EventThreatDetectionSettings.ModulesValue(
              additionalProperties=[
                  self.message_module.EventThreatDetectionSettings.ModulesValue
                  .AdditionalProperty(
                      key=args.module,
                      value=self.message_module.Config(
                          moduleEnablementState=state))
              ]))

    return self._UpdateService(args, settings, MODULE_STATUS_MASK)
