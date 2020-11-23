# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""The main command group for cloud source command group."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class Source(base.Group):
  """Cloud git repository commands.

  The {command} group lets you manipulate git repositories within a Google
  Cloud project.
  """

  category = base.SDK_TOOLS_CATEGORY

  def Filter(self, context, args):
    """Initialize context for source commands.

    Args:
      context: The current context.
      args: The argparse namespace that was specified on the CLI or API.

    Returns:
      The updated context.
    """
    base.DisableUserProjectQuota()
