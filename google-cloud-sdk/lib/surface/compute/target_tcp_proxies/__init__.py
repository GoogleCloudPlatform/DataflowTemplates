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
"""Commands for reading and manipulating target TCP proxies."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


class TargetTCPProxies(base.Group):
  """List, create, and delete target TCP proxies."""


TargetTCPProxies.category = base.NETWORKING_CATEGORY

TargetTCPProxies.detailed_help = {
    'DESCRIPTION': """
        List, create, and delete target TCP proxies for TCP Proxy Load
        Balancing.

        For more information about target TCP proxies, see the
        [TCP proxy load balancer documentation](https://cloud.google.com/load-balancing/docs/tcp/).

        See also: [Target TCP proxies API](https://cloud.google.com/compute/docs/reference/rest/v1/targetTcpProxies).
    """,
}
