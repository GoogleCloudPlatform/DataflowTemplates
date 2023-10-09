#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# access secrets from secretsmanager
secrets=$(gcloud secrets versions access latest --secret="GITACTION_SECRET_NAME")

# set secrets as env vars
secretsConfig=($secrets)
for var in "${secretsConfig[@]}"; do
  export "${var?}"
done

# retrieve gitaction runner token
ACTIONS_RUNNER_INPUT_TOKEN="$(curl -sS --request POST --url "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runners/registration-token" --header "authorization: Bearer ${GITHUB_TOKEN}"  --header 'content-type: application/json' | jq -r .token)"

# stop and uninstall the runner
sudo -u runner bash -c "cd /home/runner/actions-runner && ./svc.sh stop && ./svc.sh uninstall"
sudo -u runner bash -c "cd /home/runner/actions-runner && ./config.sh remove --url $REPO_URL --token $ACTIONS_RUNNER_INPUT_TOKEN  --unattended"