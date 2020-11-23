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
"""Utilities for `gcloud network-connectivity`."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from apitools.base.py import encoding
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources

from googlecloudsdk.api_lib.network_connectivity \
    import networkconnectivity_client as ch_client

def AppendLocationsGlobalToParent(unused_ref, unused_args, request):
  """Add locations/global to parent path."""

  request.parent += "/locations/global"
  return request
