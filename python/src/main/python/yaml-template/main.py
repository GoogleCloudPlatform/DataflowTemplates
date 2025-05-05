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

from apache_beam.yaml import main


def run(argv=None):
  parser = argparse.ArgumentParser()
  _, args = parser.parse_known_args(argv)
  # TODO(https://github.com/apache/beam/pull/34569): Remove after moved to Beam 2.65.0
  args += ['--extra_packages=/root/.apache_beam/cache/runtime-py-packages/virtualenv_clone-0.5.7-py3-none-any.whl']
  main.run(args)


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()