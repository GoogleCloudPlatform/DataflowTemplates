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


import argparse
import tempfile

from apache_beam.yaml import main

# A list of default runtime dependencies that will be installed when running jobs using the
# YAML template. These will only be installed if the caller do not provide a
# `requirements_file` option.
DEFAULT_DEPENDENCIES = [
    'boto3',
    'botocore',
    # When updating this, also update the container dependency in `python/default_base_yaml_requirements.txt`
    'https://storage.googleapis.com/dataflow-templates/extra-python-packages/2026-05-02/job_builder_util_transforms-0.1.1.tar.gz',
]


def run(argv=None):
  parser = argparse.ArgumentParser()
  _, args = parser.parse_known_args(argv)

  has_req_file = False
  for arg in args:
    if arg == '--requirements_file' or arg.startswith('--requirements_file='):
      has_req_file = True
      break

  if not has_req_file:
    # Create a temporary requirements.txt file with default dependencies.
    # We use delete=False because the file needs to exist during pipeline execution.
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', prefix='requirements_', delete=False) as f:
      for dep in DEFAULT_DEPENDENCIES:
        f.write(f'{dep}\n')
      temp_requirements_path = f.name
    args.append(f'--requirements_file={temp_requirements_path}')

  main.run(args)


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()