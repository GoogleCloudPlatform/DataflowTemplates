#
# Copyright (C) 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import logging
import os
import unittest

import apache_beam as beam
import yaml
from apache_beam.yaml import yaml_transform
from jinja2 import Environment, FileSystemLoader, meta

# Configure logging at the module level to ensure it's set up early.
# This will ensure logs are printed to console.
# Using basicConfig with a format to make logs more informative.
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('apache_beam').setLevel(logging.DEBUG)


class YamlSyntaxTest(unittest.TestCase):
    _logger = logging.getLogger(__name__)
    def test_all_yaml_files_are_valid_beam_pipelines(self):
        """
        Tests that all .yaml files in the src/main/yaml directory are valid
        Apache Beam pipelines after being rendered with Jinja.
        """
        yaml_dir = os.path.join(os.path.dirname(__file__), '../../main/yaml')
        self.assertTrue(os.path.isdir(yaml_dir), f"Directory not found: {yaml_dir}")

        env = Environment(loader=FileSystemLoader(yaml_dir))

        for template_name in env.list_templates(filter_func=lambda x: x.endswith('.yaml')):
            with self.subTest(template_name=template_name):
                logging.info(f"Validating {template_name}")
                self._logger.debug(f"Attempting to validate YAML: {template_name}")
                template = env.get_template(template_name)
                logging.info(f"Template: {template}")

                # Find all undeclared variables in the template
                undeclared_vars = meta.find_undeclared_variables(env.parse(template_name))

                # Use placeholder values for Jinja variables for validation purposes
                context = {var: 'placeholder' for var in undeclared_vars}
                rendered_yaml = template.render(context)
                self._logger.debug(f"Rendered YAML for {template_name}:\n{rendered_yaml}...") # Log first 500 chars

                self._logger.debug(f"Loading YAML into Beam pipeline_spec: {template_name}")
                pipeline_spec = yaml.load(rendered_yaml, Loader=yaml_transform.SafeLineLoader)

                # Validate the pipeline spec against the generic schema without trying to
                # expand the transforms, which avoids the need for expansion services.
                yaml_transform.validate_against_schema(pipeline_spec, 'generic')
                self._logger.info(f"Successfully validated YAML syntax for: {template_name}")

if __name__ == '__main__':
    unittest.main()