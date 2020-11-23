# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import absolute_import
import sys
import warnings

from googlecloudsdk.third_party.logging_v2 import types
from googlecloudsdk.third_party.logging_v2.gapic import config_service_v2_client
from googlecloudsdk.third_party.logging_v2.gapic import enums
from googlecloudsdk.third_party.logging_v2.gapic import logging_service_v2_client
from googlecloudsdk.third_party.logging_v2.gapic import metrics_service_v2_client


if sys.version_info[:2] == (2, 7):
    message = (
        'A future version of this library will drop support for Python 2.7. '
        'More details about Python 2 support for Google Cloud Client Libraries '
        'can be found at https://cloud.google.com/python/docs/python2-sunset/'
    )
    warnings.warn(message, DeprecationWarning)

class ConfigServiceV2Client(config_service_v2_client.ConfigServiceV2Client):
    __doc__ = config_service_v2_client.ConfigServiceV2Client.__doc__
    enums = enums

class LoggingServiceV2Client(logging_service_v2_client.LoggingServiceV2Client):
    __doc__ = logging_service_v2_client.LoggingServiceV2Client.__doc__
    enums = enums

class MetricsServiceV2Client(metrics_service_v2_client.MetricsServiceV2Client):
    __doc__ = metrics_service_v2_client.MetricsServiceV2Client.__doc__
    enums = enums


__all__ = (
    'enums',
    'types',
    'ConfigServiceV2Client',
    'LoggingServiceV2Client',
    'MetricsServiceV2Client',
)
