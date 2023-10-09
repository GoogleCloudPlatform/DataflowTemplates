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

user=runner
id -u $user &> /dev/null | sudo useradd $user

# increase ulimit size
ulimit -n 65536

# increase max virtual memory
sudo sysctl -w vm.max_map_count=262144

# install jq
apt-get update
apt-get -y install jq

# install maven
sudo apt update
sudo apt install git maven -y

# update git
sudo add-apt-repository ppa:git-core/ppa -y
sudo apt-get update
sudo apt-get install git -y

# install gh
sudo type -p curl >/dev/null || (sudo apt update && sudo apt install curl -y)
sudo curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
&& sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
&& sudo apt update \
&& sudo apt install gh -y

# install docker
sudo apt-get update
sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y

# add runner to docker group
sudo groupadd docker
sudo gpasswd -a runner docker

# create runner HOME
sudo mkdir /home/runner
sudo chown runner /home/runner

# access secrets from secretsmanager
secrets=$(gcloud secrets versions access latest --secret="GITACTION_SECRET_NAME")

# set secrets as env vars
secretsConfig=($secrets)
for var in "${secretsConfig[@]}"; do
  export "${var?}"
done

# retrieve gitaction runner token
ACTIONS_RUNNER_INPUT_TOKEN="$(curl -sS --request POST --url "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runners/registration-token" --header "authorization: Bearer ${GITHUB_TOKEN}"  --header 'content-type: application/json' | jq -r .token)"

# create actions-runner directory
sudo -u runner bash -c "mkdir /home/runner/actions-runner"

# download and extract gitactions binary
sudo -u runner bash -c "cd /home/runner/actions-runner && curl -o actions-runner-linux-x64.tar.gz --location https://github.com/actions/runner/releases/download/v${GH_RUNNER_VERSION}/actions-runner-linux-x64-${GH_RUNNER_VERSION}.tar.gz"
sudo -u runner bash -c "cd /home/runner/actions-runner && tar -zxf ./actions-runner-linux-x64.tar.gz"

# configure and run gitactions runner
sudo -u runner bash -c "cd /home/runner/actions-runner && ./config.sh --url ${REPO_URL} --token ${ACTIONS_RUNNER_INPUT_TOKEN} --labels ${GITACTIONS_LABELS} --unattended"
sudo -u runner bash -c "cd /home/runner/actions-runner && ./run.sh &"
