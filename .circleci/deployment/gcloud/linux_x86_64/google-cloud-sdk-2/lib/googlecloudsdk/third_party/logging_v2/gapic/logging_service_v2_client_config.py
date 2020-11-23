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


config = {
  "interfaces": {
    "google.logging.v2.LoggingServiceV2": {
      "retry_codes": {
        "idempotent": [
          "DEADLINE_EXCEEDED",
          "INTERNAL",
          "UNAVAILABLE"
        ],
        "non_idempotent": [],
        "idempotent2": [
          "DEADLINE_EXCEEDED",
          "UNAVAILABLE"
        ]
      },
      "retry_params": {
        "default": {
          "initial_retry_delay_millis": 100,
          "retry_delay_multiplier": 1.3,
          "max_retry_delay_millis": 60000,
          "initial_rpc_timeout_millis": 20000,
          "rpc_timeout_multiplier": 1.0,
          "max_rpc_timeout_millis": 20000,
          "total_timeout_millis": 600000
        }
      },
      "methods": {
        "DeleteLog": {
          "timeout_millis": 60000,
          "retry_codes_name": "idempotent",
          "retry_params_name": "default"
        },
        "ListLogEntries": {
          "timeout_millis": 10000,
          "retry_codes_name": "idempotent",
          "retry_params_name": "default"
        },
        "WriteLogEntries": {
          "timeout_millis": 60000,
          "retry_codes_name": "idempotent",
          "retry_params_name": "default",
          "bundling": {
            "element_count_threshold": 1000,
            "request_byte_threshold": 1048576,
            "delay_threshold_millis": 50
          }
        },
        "ListMonitoredResourceDescriptors": {
          "timeout_millis": 60000,
          "retry_codes_name": "idempotent2",
          "retry_params_name": "default"
        },
        "ListLogs": {
          "timeout_millis": 60000,
          "retry_codes_name": "idempotent2",
          "retry_params_name": "default"
        },
        "TailLogEntries": {
          "timeout_millis": 3600000,
          "retry_codes_name": "non_idempotent",
          "retry_params_name": "default"
        }
      }
    }
  }
}
