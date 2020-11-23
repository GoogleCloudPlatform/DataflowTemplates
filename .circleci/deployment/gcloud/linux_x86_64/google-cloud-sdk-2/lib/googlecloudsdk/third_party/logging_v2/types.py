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

from google.api_core.protobuf_helpers import get_messages

from google.api import distribution_pb2
from google.api import label_pb2
from google.api import metric_pb2
from google.api import monitored_resource_pb2
from google.logging.type import http_request_pb2
from google.protobuf import any_pb2
from google.protobuf import duration_pb2
from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
from googlecloudsdk.third_party.logging_v2.proto import log_entry_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_config_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_metrics_pb2
from googlecloudsdk.third_party.logging_v2.proto import logging_pb2


_shared_modules = [
    distribution_pb2,
    label_pb2,
    metric_pb2,
    monitored_resource_pb2,
    http_request_pb2,
    any_pb2,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    struct_pb2,
    timestamp_pb2,
]

_local_modules = [
    log_entry_pb2,
    logging_config_pb2,
    logging_metrics_pb2,
    logging_pb2,
]

names = []

for module in _shared_modules:  # pragma: NO COVER
    for name, message in get_messages(module).items():
        setattr(sys.modules[__name__], name, message)
        names.append(name)
for module in _local_modules:
      for name, message in get_messages(module).items():
          message.__module__ = 'googlecloudsdk.third_party.logging_v2.types'
          setattr(sys.modules[__name__], name, message)
          names.append(name)


__all__ = tuple(sorted(names))
