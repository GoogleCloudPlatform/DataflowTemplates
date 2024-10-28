#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import logging
import subprocess
import time

from apache_beam.utils.subprocess_server import JavaJarServer
from apache_beam.yaml import yaml_provider


def cache_provider_jars(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--maven_central_repository_url',
    required=True,
    help='A Maven repo to use to cache expansion service jars.')
  parser.add_argument(
    '--cache_dir',
    required=True,
    help='A Maven repo to use to cache expansion service jars.')
  known_args, _ = parser.parse_known_args(argv)

  providers_by_id = {}
  for providers in yaml_provider.standard_providers().values():
    for provider in providers:
      providers_by_id[id(provider)] = provider
  for provider in providers_by_id.values():
    t = time.time()
    artifacts = provider.cache_artifacts()
    if artifacts:
      for artifact in artifacts:
        if artifact[-4:] == '.jar':
          command = ['curl', '-L', known_args.maven_central_repository_url + artifact.replace(JavaJarServer.MAVEN_CENTRAL_REPOSITORY, ''), '-o', known_args.cache_dir + str(artifact).split('/')[-1]]
          subprocess.run(command, check=True)
      logging.info(
          'Cached %s in %0.03f seconds.', ', '.join(artifacts), time.time() - t)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  cache_provider_jars()
