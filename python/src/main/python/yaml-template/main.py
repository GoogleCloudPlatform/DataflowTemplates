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
import json

from apache_beam.yaml import main


# TODO(polber) - remove _preparse_jinja_flags with Beam 2.58.0
def _preparse_jinja_flags(argv):
  """Promotes any flags to --jinja_variables based on --jinja_variable_flags.
  This is to facilitate tools (such as dataflow templates) that must pass
  options as un-nested flags.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--jinja_variable_flags',
      default=[],
      type=lambda s: s.split(','),
      help='A list of flag names that should be used as jinja variables.')
  parser.add_argument(
      '--jinja_variables',
      default={},
      type=json.loads,
      help='A json dict of variables used when invoking the jinja preprocessor '
      'on the provided yaml pipeline.')
  jinja_args, other_args = parser.parse_known_args(argv)
  if not jinja_args.jinja_variable_flags:
    return argv

  jinja_variable_parser = argparse.ArgumentParser()
  for flag_name in jinja_args.jinja_variable_flags:
    jinja_variable_parser.add_argument('--' + flag_name)
  jinja_flag_variables, pipeline_args = jinja_variable_parser.parse_known_args(
      other_args)
  jinja_args.jinja_variables.update(
      **
      {k: v
       for (k, v) in vars(jinja_flag_variables).items() if v is not None})
  if jinja_args.jinja_variables:
    pipeline_args = pipeline_args + [
        '--jinja_variables=' + json.dumps(jinja_args.jinja_variables)
    ]

  return pipeline_args


def run(argv=None):
  main.run(argv=_preparse_jinja_flags(argv))


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()