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
"""Code that's shared between multiple service-attachments subcommands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def GetConnectionPreference(args, messages):
  """Get connection preference of the service attachment."""
  if args.connection_preference == 'ACCEPT_AUTOMATIC':
    return messages.ServiceAttachment.ConnectionPreferenceValueValuesEnum.ACCEPT_AUTOMATIC

  return None
