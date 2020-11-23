# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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
"""Package for the sole-tenancy CLI commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.ALPHA)
class SoleTenancy(base.Group):
  """Read and manage Compute Engine sole-tenancy resources."""


SoleTenancy.category = base.INSTANCES_CATEGORY

SoleTenancy.detailed_help = {
    'DESCRIPTION': """
        Read and manage Compute Engine sole-tenancy resources.

        For more information about sole-tenancy resources, see the
        [sole-tenancy documentation](https://cloud.google.com/compute/docs/nodes/).
    """,
}
