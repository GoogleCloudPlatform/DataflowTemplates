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
"""Resource parsing helpers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from googlecloudsdk.calliope import exceptions


def ValidatePort(port):
  """Python hook to validate that the port is between 1024 and 65535, inclusive."""
  if port < 1024 or port > 65535:
    raise exceptions.BadArgumentException(
        '--port', 'Port ({0}) is not in the range [1025, 65535].'.format(port))
  return port


def ValidateGcsUri(arg_name):
  """Validates the gcs uri is formatted correctly."""

  def Process(gcs_uri):
    if not gcs_uri.startswith('gs://'):
      raise exceptions.BadArgumentException(
          arg_name, 'Expected URI {0} to start with `gs://`.'.format(gcs_uri))
    return gcs_uri

  return Process


def ValidateKerberosPrincipal(kerberos_principal):
  pattern = re.compile(r'^(.+)/(.+)@(.+)$')
  if not pattern.match(kerberos_principal):
    raise exceptions.BadArgumentException(
        '--kerberos-principal',
        'Kerberos Principal {0} does not match ReGeX {1}.'.format(
            kerberos_principal, pattern))
  return kerberos_principal
