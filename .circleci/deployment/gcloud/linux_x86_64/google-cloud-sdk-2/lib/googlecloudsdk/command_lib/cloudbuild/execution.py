# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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

"""Support library for execution with the container builds submit command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import threading

from googlecloudsdk.core import log
from googlecloudsdk.core.util import keyboard_interrupt
import six


class MashHandler(object):
  """MashHandler only invokes its base handler once.

  On the third attempt, the execution is hard-killed.
  """

  def __init__(self, base_handler):
    self._interrupts = 0
    self._base_handler = base_handler
    self._lock = threading.Lock()

  def __call__(self, signal_number, stack_frame):
    with self._lock:
      self._interrupts += 1
      # Copy the interrupts count and perform the handler outside of this
      # lock context. The handler can take a long time and we want to make
      # sure that future interrupts don't wait for it.
      interrupts = self._interrupts
    if interrupts == 1:
      # Only do the base handler once.
      self._base_handler(signal_number, stack_frame)
    elif interrupts == 3:
      # If we detect mashing, fallback to gcloud's original handler.
      keyboard_interrupt.HandleInterrupt(signal_number, stack_frame)


def GetCancelBuildHandler(client, messages, build_ref):
  def _CancelBuildHandler(unused_signal_number, unused_stack_frame):
    log.status.Print('Cancelling...')
    client.projects_builds.Cancel(
        messages.CloudbuildProjectsBuildsCancelRequest(
            projectId=build_ref.projectId,
            id=build_ref.id))
    log.status.Print('Cancelled [{r}].'.format(r=six.text_type(build_ref)))
  return _CancelBuildHandler
