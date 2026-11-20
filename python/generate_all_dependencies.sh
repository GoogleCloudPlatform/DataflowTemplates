# Copyright 2025 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# See [maintainers-guide](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/maintainers-guide.md#validating-and-upgrading-beam-versions) for more information.

# Exit immediately on error
set -e

SCRIPTPATH=$(dirname "$0")

# Generate a base set of dependencies to use for any templates without special dependencies
mkdir -p $SCRIPTPATH/__build__/
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/default_base_bqmonitor_requirements.txt $SCRIPTPATH/__build__/bqmonitor_requirements.txt
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/default_base_yaml_requirements.txt $SCRIPTPATH/__build__/default_yaml_requirements.txt

cp $SCRIPTPATH/__build__/bqmonitor_requirements.txt $SCRIPTPATH/../python/src/main/python/bigquery-anomaly-detection/requirements_all.txt

cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../python/src/main/python/yaml-template/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../python/src/main/python/job-builder-server/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../yaml/src/main/python/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-elasticsearch/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-googlecloud/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-splunk/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-and-mongodb/src/main/resources/requirements.txt

# Regenerate hash-pinned CI workflow and test-infra helper requirements once present on main.
update_if_present() {
  BASE_REQ="$1"
  BUILD_REQ="$2"
  DEST_REQ="$3"
  if [ -f "$DEST_REQ" ] && grep -q -- "--hash=" "$DEST_REQ"; then
    sh "$SCRIPTPATH/generate_dependencies.sh" "$BASE_REQ" "$BUILD_REQ"
    cp "$BUILD_REQ" "$DEST_REQ"
  fi
}

update_if_present \
  "$SCRIPTPATH/../python/src/test/python/bigquery-anomaly-detection/requirements-test.in" \
  "$SCRIPTPATH/__build__/bqmonitor_test_requirements.txt" \
  "$SCRIPTPATH/../python/src/test/python/bigquery-anomaly-detection/requirements-test.txt"

update_if_present \
  "$SCRIPTPATH/../.github/workflows/requirements/yaml-pr.in" \
  "$SCRIPTPATH/__build__/yaml_pr_hashes.txt" \
  "$SCRIPTPATH/../.github/workflows/requirements/yaml-pr-hashes.txt"

update_if_present \
  "$SCRIPTPATH/../.github/workflows/requirements/yaml-config-params-report.in" \
  "$SCRIPTPATH/__build__/yaml_config_params_report_hashes.txt" \
  "$SCRIPTPATH/../.github/workflows/requirements/yaml-config-params-report-hashes.txt"

update_if_present \
  "$SCRIPTPATH/../test-infra/requirements.txt" \
  "$SCRIPTPATH/__build__/test_infra_requirements_hashes.txt" \
  "$SCRIPTPATH/../test-infra/requirements-hashes.txt"

update_if_present \
  "$SCRIPTPATH/../.github/workflows/requirements/poetry-requirements.in" \
  "$SCRIPTPATH/__build__/poetry_requirements_hashes.txt" \
  "$SCRIPTPATH/../.github/workflows/requirements/poetry-requirements-hashes.txt"
