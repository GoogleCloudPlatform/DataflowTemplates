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
"""Formatting strings for Artifact Registry commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

BUILD_GIT_SHA_FORMAT = ("BUILD_DETAILS.buildDetails.provenance."
                        "sourceProvenance.context.cloudRepo.revisionId"
                        ".notnull().list().slice(:8).join(''):optional:label"
                        "=GIT_SHA")

BUILD_FORMAT = ("BUILD_DETAILS.buildDetails.provenance.id.notnull().list()"
                ":optional:label=BUILD")

VULNERABILITY_FORMAT = "vuln_counts.list():optional:label=VULNERABILITIES"

IMAGE_BASIS_FORMAT = ("IMAGE_BASIS.derivedImage.sort(distance).map()"
                      ".extract(baseResourceUrl).slice(:1).map().list().list()"
                      ".split('//').slice(1:).list().split('@').slice(:1)"
                      ".list():optional:label=FROM")

DISCOVERY_FORMAT = ("DISCOVERY[0].discovered.analysisStatus:optional:label"
                    "=VULNERABILITY_SCAN_STATUS")

CONTAINER_ANALYSIS_METADATA_FORMAT = """
  {},
  {},
  {},
  {},
  {}
""".format(BUILD_GIT_SHA_FORMAT, VULNERABILITY_FORMAT, IMAGE_BASIS_FORMAT,
           BUILD_FORMAT, DISCOVERY_FORMAT)
