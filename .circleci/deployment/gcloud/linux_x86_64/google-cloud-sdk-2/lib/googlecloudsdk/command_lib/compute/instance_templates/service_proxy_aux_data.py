# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Auxiliary data for implementing Service Proxy flags in Instance Templates."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import enum


class TracingState(str, enum.Enum):
  ON = 'ON'
  OFF = 'OFF'


# Don't put sudo when running service-proxy-agent-bootstrap.sh, as exported
# variables don't get passed to the script when run with sudo. It's not a
# problem because all commands inside service-proxy-agent-bootstrap.sh
# are run with sudo anyway.
startup_script = """#! /bin/bash
ZONE=$( curl --silent http://metadata.google.internal/computeMetadata/v1/instance/zone -H Metadata-Flavor:Google | cut -d/ -f4 )
export SERVICE_PROXY_AGENT_DIRECTORY=$(mktemp -d)
sudo gsutil cp \
  gs://gce-service-proxy-${ZONE}/service-proxy-agent/releases/service-proxy-agent-0.2.tgz \
  ${SERVICE_PROXY_AGENT_DIRECTORY} \
  || sudo gsutil cp \
    gs://gce-service-proxy/service-proxy-agent/releases/service-proxy-agent-0.2.tgz \
    ${SERVICE_PROXY_AGENT_DIRECTORY}
sudo tar -xzf ${SERVICE_PROXY_AGENT_DIRECTORY}/service-proxy-agent-0.2.tgz -C ${SERVICE_PROXY_AGENT_DIRECTORY}
${SERVICE_PROXY_AGENT_DIRECTORY}/service-proxy-agent/service-proxy-agent-bootstrap.sh"""

startup_script_with_location_template = """#! /bin/bash
export SERVICE_PROXY_AGENT_DIRECTORY=$(mktemp -d)
sudo gsutil cp %s ${SERVICE_PROXY_AGENT_DIRECTORY}
ARCHIVE_NAME=$(ls ${SERVICE_PROXY_AGENT_DIRECTORY})
sudo tar -xzf ${SERVICE_PROXY_AGENT_DIRECTORY}/${ARCHIVE_NAME} -C ${SERVICE_PROXY_AGENT_DIRECTORY}
${SERVICE_PROXY_AGENT_DIRECTORY}/service-proxy-agent/service-proxy-agent-bootstrap.sh"""
