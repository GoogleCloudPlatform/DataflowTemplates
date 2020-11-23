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
"""Code that's shared between multiple target-*-proxies subcommands."""


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def AddProxyHeaderRelatedCreateArgs(parser, default='NONE'):
  """Adds parser arguments for creation related to ProxyHeader."""

  parser.add_argument(
      '--proxy-header',
      choices={
          'NONE': 'No proxy header is added.',
          'PROXY_V1': ('Enables PROXY protocol (version 1) for passing client '
                       'connection information.'),
      },
      default=default,
      help='The type of proxy protocol header to be sent to the backend.')


def AddProxyHeaderRelatedUpdateArgs(parser):
  """Adds parser arguments for update related to ProxyHeader."""

  AddProxyHeaderRelatedCreateArgs(parser, default=None)


def AddQuicOverrideCreateArgs(parser, default='NONE'):
  """Adds parser arguments for creation related to QuicOverride."""

  parser.add_argument(
      '--quic-override',
      choices={
          'NONE': 'Allows Google to control when QUIC is rolled out.',
          'ENABLE': 'Allows load balancer to negotiate QUIC with clients.',
          'DISABLE': 'Disallows load balancer to negotiate QUIC with clients.',
      },
      default=default,
      help='Controls whether load balancer may negotiate QUIC with clients. '
      'QUIC is a new transport which reduces latency compared to that of TCP. '
      'See https://www.chromium.org/quic for more details.')


def AddQuicOverrideUpdateArgs(parser):
  """Adds parser arguments for update related to QuicOverride."""

  AddQuicOverrideCreateArgs(parser, default=None)
