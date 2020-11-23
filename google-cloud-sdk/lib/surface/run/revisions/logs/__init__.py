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
"""Group definition for logs."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.run import exceptions
from googlecloudsdk.command_lib.run import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Logs(base.Group):
  """Read logs for Cloud Run (fully managed) revisions."""

  def _CheckPlatform(self):
    if flags.GetPlatform() != flags.PLATFORM_MANAGED:
      raise exceptions.PlatformError(
          'This command group only supports listing regions for '
          'Cloud Run (fully managed).')

  def Filter(self, context, args):
    self._CheckPlatform()
    return context
