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

# update git
sudo add-apt-repository ppa:git-core/ppa -y
sudo apt update
sudo apt install git -y

# install jq
sudo apt install jq -y

# install maven
sudo apt install git maven -y

# install gh
sudo apt install curl -y \
&& sudo curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
&& sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
&& sudo apt update \
&& sudo apt install gh -y

# install docker
sudo apt update
sudo apt install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y

# add user to docker group
sudo groupadd docker
sudo gpasswd -a $user docker

# create user HOME
sudo mkdir /home/$user
sudo chown $user /home/$user

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
sudo -u $user bash -c "mkdir /home/$user/actions-runner"

# download and extract gitactions binary
sudo -u $user bash -c "cd /home/$user/actions-runner && curl -o actions-runner-linux-x64.tar.gz --location https://github.com/actions/runner/releases/download/v${GH_RUNNER_VERSION}/actions-runner-linux-x64-${GH_RUNNER_VERSION}.tar.gz"
sudo -u $user bash -c "cd /home/$user/actions-runner && tar -zxf ./actions-runner-linux-x64.tar.gz"

# configure and run gitactions runner
sudo -u $user bash -c "cd /home/$user/actions-runner && ./config.sh --url ${REPO_URL} --token ${ACTIONS_RUNNER_INPUT_TOKEN} --labels ${GITACTIONS_LABELS} --unattended"
sudo -u $user bash -c "cd /home/$user/actions-runner && ./run.sh &"
