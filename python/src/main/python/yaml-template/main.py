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


# TODO(https://github.com/apache/beam/issues/29916): Remove once alias args
#  are added to main.py
def _get_alias_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--yaml_pipeline', help='A yaml description of the pipeline to run.')
  parser.add_argument(
      '--yaml_pipeline_file',
      help='A file containing a yaml description of the pipeline to run.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  if known_args.yaml_pipeline:
    pipeline_args += [f'--pipeline_spec={known_args.yaml_pipeline}']
  if known_args.yaml_pipeline_file:
    pipeline_args += [f'--pipeline_spec_file={known_args.yaml_pipeline_file}']
  return pipeline_args


def run(argv=None):
  args = _get_alias_args(argv)
  cache_provider_artifacts.cache_provider_artifacts()
  main.run(argv=args)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
