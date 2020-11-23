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
"""Utilities for constructing Assured api messages."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.assured import client_util

BETA = 'BETA'


def GetMessages(release_track):
  return client_util.GetClientInstance(release_track).MESSAGES_MODULE


def GetBetaWorkloadMessage():
  return GetMessages(BETA).GoogleCloudAssuredworkloadsV1beta1Workload


def CreateAssuredParent(organization_id, location):
  return 'organizations/{}/locations/{}'.format(organization_id, location)


def CreateBetaAssuredWorkload(display_name=None,
                              compliance_regime=None,
                              billing_account=None,
                              next_rotation_time=None,
                              rotation_period=None,
                              labels=None,
                              etag=None,
                              provisioned_resources_parent=None):
  """Construct an Assured Workload message for Assured Workloads Beta API requests.

  Args:
    display_name: str, display name of the Assured Workloads environment.
    compliance_regime: str, the compliance regime, which is one of:
      FEDRAMP_MODERATE, FEDRAMP_HIGH, IL4 or CJIS.
    billing_account: str, the billing account of the Assured Workloads
      environment in the form: billingAccounts/{BILLING_ACCOUNT_ID}
    next_rotation_time: str, the next key rotation time for the Assured
      Workloads environment, for example: 2020-12-30T10:15:00.00Z
    rotation_period: str, the time between key rotations, for example: 172800s
    labels: dict, dictionary of label keys and values of the Assured Workloads
      environment.
    etag: str, the etag of the Assured Workloads environment.
    provisioned_resources_parent: str, parent of the provisioned projects,
      for example: folders/{FOLDER_ID}

  Returns:
    A populated Assured Workloads message for the Assured Workloads Beta API.
  """

  workloads_messages = GetMessages(BETA)
  workload_message = GetBetaWorkloadMessage()
  workload = workload_message()
  if etag:
    workload.etag = etag
  if billing_account:
    workload.billingAccount = billing_account
  if display_name:
    workload.displayName = display_name
  if labels:
    workload.labels = CreateBetaLabels(labels)
  if compliance_regime:
    workload.complianceRegime = workload_message.ComplianceRegimeValueValuesEnum(
        compliance_regime)
    if compliance_regime == 'FEDRAMP_MODERATE':
      settings = workloads_messages.GoogleCloudAssuredworkloadsV1beta1WorkloadFedrampModerateSettings(
      )
      settings.kmsSettings = CreateBetaKmsSettings(workloads_messages,
                                                   next_rotation_time,
                                                   rotation_period)
      workload.fedrampModerateSettings = settings
    elif compliance_regime == 'FEDRAMP_HIGH':
      settings = workloads_messages.GoogleCloudAssuredworkloadsV1beta1WorkloadFedrampHighSettings(
      )
      settings.kmsSettings = CreateBetaKmsSettings(workloads_messages,
                                                   next_rotation_time,
                                                   rotation_period)
      workload.fedrampHighSettings = settings
    elif compliance_regime == 'CJIS':
      settings = workloads_messages.GoogleCloudAssuredworkloadsV1beta1WorkloadCJISSettings(
      )
      settings.kmsSettings = CreateBetaKmsSettings(workloads_messages,
                                                   next_rotation_time,
                                                   rotation_period)
      workload.cjisSettings = settings
    elif compliance_regime == 'IL4':
      settings = workloads_messages.GoogleCloudAssuredworkloadsV1beta1WorkloadIL4Settings(
      )
      settings.kmsSettings = CreateBetaKmsSettings(workloads_messages,
                                                   next_rotation_time,
                                                   rotation_period)
      workload.il4Settings = settings
  if provisioned_resources_parent:
    workload.provisionedResourcesParent = provisioned_resources_parent
  return workload


def CreateBetaKmsSettings(messages, next_rotation_time, rotation_period):
  return messages.GoogleCloudAssuredworkloadsV1beta1WorkloadKMSSettings(
      nextRotationTime=next_rotation_time, rotationPeriod=rotation_period)


def CreateBetaLabels(labels):
  workload_message = GetBetaWorkloadMessage()
  workload_labels = []
  for key, value in labels.items():
    new_label = workload_message.LabelsValue.AdditionalProperty(
        key=key, value=value)
    workload_labels.append(new_label)
  return workload_message.LabelsValue(additionalProperties=workload_labels)


def CreateUpdateMask(display_name, labels):
  update_mask = []
  if display_name:
    update_mask.append('workload.display_name')
  if labels:
    update_mask.append('workload.labels')
  return ','.join(update_mask)
