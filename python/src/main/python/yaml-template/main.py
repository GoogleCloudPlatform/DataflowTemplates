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

from apache_beam.yaml import main
from apache_beam.yaml import cache_provider_artifacts


def _pipeline_spec_file_from_args(known_args):
    if known_args.yaml:
        return known_args.yaml
    else:
        raise ValueError(
            "--yaml must be set.")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--yaml',
        dest='yaml',
        help='Input yaml file in Cloud Storage.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_spec_file = _pipeline_spec_file_from_args(known_args)
    cache_provider_artifacts.cache_provider_artifacts()

    main.run(argv=pipeline_args + [f"--pipeline_spec_file={pipeline_spec_file}",
                                   "--save_main_session"])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
