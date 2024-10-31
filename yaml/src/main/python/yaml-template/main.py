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
  main.run(args)


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()