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
"""Utilities for calling the Metastore Operations API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.metastore import util as api_util
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.calliope import base


def GetOperation(release_track=base.ReleaseTrack.ALPHA):
  return api_util.GetClientInstance(
      release_track=release_track).projects_locations_operations


def Delete(relative_resource_name, release_track=base.ReleaseTrack.ALPHA):
  """Calls the Metastore Operations.Delete method.

  Args:
    relative_resource_name: str, the relative resource name of
      the Metastore operation to delete.
    release_track: base.ReleaseTrack, the release track of command. Will dictate
        which Composer client library will be used.

  Returns:
    Empty
  """
  return GetOperation(release_track=release_track).Delete(
      api_util.GetMessagesModule(release_track=release_track)
      .MetastoreProjectsLocationsOperationsDeleteRequest(
          name=relative_resource_name))


def WaitForOperation(operation, message, release_track=base.ReleaseTrack.ALPHA):
  """Waits for an operation to complete.

  Polls the operation at least every 15 seconds, showing a progress indicator.
  Returns when the operation has completed.

  Args:
    operation: Operation Message, the operation to poll
    message: str, a message to display with the progress indicator. For example,
      'Waiting for deletion of [some resource]'.
    release_track: base.ReleaseTrack, the release track of command. Will dictate
      which Metastore client library will be used.
  """
  waiter.WaitFor(
      _OperationPoller(release_track=release_track),
      operation.name,
      message,
      max_wait_ms=3600 * 1000,
      wait_ceiling_ms=15 * 1000)


class _OperationPoller(waiter.CloudOperationPollerNoResources):
  """Class for polling Metastore longrunning Operations."""

  def __init__(self, release_track=base.ReleaseTrack.ALPHA):
    super(_OperationPoller,
          self).__init__(GetOperation(release_track=release_track), lambda x: x)

  def IsDone(self, operation):
    if not operation.done:
      return False
    if operation.error:
      raise api_util.OperationError(operation.name, operation.error.message)
    return True
