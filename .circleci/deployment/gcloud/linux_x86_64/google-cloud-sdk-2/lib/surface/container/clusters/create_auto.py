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
"""Create-auto cluster command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from surface.container.clusters import create

# Select which flags are auto flags
auto_flags = [
    'args',
    'clusterversion',
    'ipalias_additional',
    'masterauth',
    'nodeidentity',
    'privatecluster',
    'releasechannel',
]

# Change default flag values in create-auto
flag_overrides = {
    'num_nodes': 1,
    'enable_private_nodes': True,
    'enable_ip_alias': True,
    'enable_master_authorized_networks': False,
    'privatecluster': {
        'enable_private_nodes': None,
        'private_cluster': None,
    },
}

auto_flag_defaults = dict(list(create.base_flag_defaults.items()) + \
                          list(flag_overrides.items()))


@base.Hidden
@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(create.CreateBeta):
  autogke = True
  default_flag_values = auto_flag_defaults

  @staticmethod
  def Args(parser):
    create.AddFlags(create.BETA, parser, auto_flag_defaults, auto_flags)


@base.Hidden
@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(create.CreateAlpha):
  autogke = True
  default_flag_values = auto_flag_defaults

  @staticmethod
  def Args(parser):
    create.AddFlags(create.ALPHA, parser, auto_flag_defaults, auto_flags)
