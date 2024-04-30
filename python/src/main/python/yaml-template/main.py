#
# Copyright (C) 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import argparse
import logging

from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main


def run(argv=None):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_args += ['--sdk_location=container']
  cache_provider_artifacts.cache_provider_artifacts()
  main.run(argv=pipeline_args)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
