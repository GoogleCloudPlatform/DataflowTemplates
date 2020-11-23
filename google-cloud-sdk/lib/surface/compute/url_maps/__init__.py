# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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
"""Commands for reading and manipulating URL maps."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


class URLMaps(base.Group):
  """List, create, and delete URL maps."""


URLMaps.category = base.NETWORKING_CATEGORY

URLMaps.detailed_help = {
    'DESCRIPTION': """
        List, create, and delete URL maps for external HTTP(S) Load Balancing,
        internal HTTP(S) Load Balancing, and Traffic Director.

        For more information about URL maps, see the
        [URL maps documentation](https://cloud.google.com/load-balancing/docs/url-map-concepts).

        See also: [URL maps API](https://cloud.google.com/compute/docs/reference/rest/v1/urlMaps)
        and
        [regional URL maps API](https://cloud.google.com/compute/docs/reference/rest/v1/regionUrlMaps).
    """,
}
