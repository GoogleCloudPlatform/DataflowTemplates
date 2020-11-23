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
"""Gather stage/condition information for any important objects here."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.core.console import progress_tracker

READY = 'Ready'
SERVICE_IAM_POLICY_SET = 'IamPolicySet'
SERVICE_ROUTES_READY = 'RoutesReady'
SERVICE_CONFIGURATIONS_READY = 'ConfigurationsReady'
BUILD_READY = 'BuildReady'
UPLOAD_SOURCE = 'UploadSource'


def _UploadSourceStage():
  return progress_tracker.Stage(
      'Uploading sources...', key=UPLOAD_SOURCE)


def _BuildContainerStage():
  return progress_tracker.Stage(
      'Building Container...', key=BUILD_READY)


def _NewRoutingTrafficStage():
  return progress_tracker.Stage(
      'Routing traffic...', key=SERVICE_ROUTES_READY)


def UpdateTrafficStages():
  return [_NewRoutingTrafficStage()]


# Because some terminals cannot update multiple lines of output simultaneously,
# the order of conditions in this dictionary should match the order in which we
# expect cloud run resources to complete deployment.
def ServiceStages(include_iam_policy_set=False,
                  include_route=True,
                  include_build=False):
  """Return the progress tracker Stages for conditions of a Service."""
  stages = []
  if include_build:
    stages.append(_UploadSourceStage())
    stages.append(_BuildContainerStage())
  stages.append(progress_tracker.Stage(
      'Creating Revision...', key=SERVICE_CONFIGURATIONS_READY))
  if include_route:
    stages.append(_NewRoutingTrafficStage())
  if include_iam_policy_set:
    stages.append(progress_tracker.Stage(
        'Setting IAM Policy...', key=SERVICE_IAM_POLICY_SET))
  return stages


def ServiceDependencies():
  """Dependencies for the Service resource, for passing to ConditionPoller."""
  return {
      SERVICE_ROUTES_READY: {SERVICE_CONFIGURATIONS_READY}
  }
