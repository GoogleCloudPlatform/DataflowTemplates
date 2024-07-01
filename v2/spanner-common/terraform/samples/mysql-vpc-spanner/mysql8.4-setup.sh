#! /bin/bash
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
echo "Starting startup script"
apt update
apt-get install -y libnuma-dev
mkdir /mysql-install
cd /mysql-install
#Install wget if not found
apt install wget -y
wget https://cdn.mysql.com//Downloads/MySQL-8.4/mysql-8.4.0-linux-glibc2.28-x86_64.tar.xz
groupadd mysql
useradd -r -g mysql -s /bin/false mysql
tar xvf mysql-8.4.0-linux-glibc2.28-x86_64.tar.xz
cd /usr/local
ln -s /mysql-install/mysql-8.4.0-linux-glibc2.28-x86_64 mysql
cd mysql
mkdir mysql-files
chown mysql:mysql mysql-files
chmod 750 mysql-files
# Setup bin logs for Mysql
cat > /etc/my.cnf <<EOL
  [mysqld]
  log-bin=mysql-bin
  server-id=1
  log_replica_updates=true
  binlog_expire_logs_seconds=604800
EOL
echo "#### Initializing MySQL 8.4 Database ###"
bin/mysqld --initialize-insecure --user=mysql
echo "Initialized."
bin/mysqld_safe --user=mysql &
cp support-files/mysql.server /etc/init.d/mysql.server
# Server started
echo "Server started "
# Installing client
apt install mysql-client-core-8.0
mysql -u root --socket /tmp/mysql.sock -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '${root_password}'; CREATE USER '${custom_user}'@'%' IDENTIFIED BY '${custom_user_password}'; GRANT ALL PRIVILEGES ON *.* TO '${custom_user}'@'%';"
mysql -u "${custom_user}" --password="${custom_user_password}" --socket /tmp/mysql.sock -e "${ddl}"
echo "Setup complete and server is running"