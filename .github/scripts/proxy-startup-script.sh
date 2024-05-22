#!/bin/bash
# Copyright 2024 Google LLC
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

user=proxy
id -u $user &> /dev/null | sudo useradd $user

# create user HOME
sudo mkdir /home/$user
sudo chown $user /home/$user

# Install Cloud SQL Auth Proxy
sudo -u $user bash -c "cd /home/$user && curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.0/cloud-sql-proxy.linux.amd64"
sudo -u $user bash -c "cd /home/$user && chmod +x cloud-sql-proxy"

# Start cloud proxy for mySQL
sudo -u $user bash -c "cd /home/$user && ./cloud-sql-proxy --address 0.0.0.0 --port 33134 CLOUD_SQL_PREFIX-mysql &"