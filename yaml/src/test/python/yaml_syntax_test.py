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
 
 
def create_test_method(template_name, yaml_dir):
    """Creates a test method that validates a single YAML template.

    This factory function generates a test method that will be dynamically added
    to the YamlSyntaxTest class. Each generated test validates a single YAML
    template file by rendering it with placeholder values for any Jinja
    variables and then validating the resulting YAML against the Beam 'generic'
    schema.

    Args:
        template_name (str): The filename of the YAML template to be tested.
        yaml_dir (str): The directory where the YAML templates are located.

    Returns:
        function: A test method that can be attached to a unittest.TestCase class.
    """

    def test_method(self):
        self._logger.info(f"Validating {template_name}")
        env = Environment(loader=FileSystemLoader(yaml_dir), autoescape=False)
        template_source = env.loader.get_source(env, template_name)[0]
 
        # Find all undeclared variables in the template
        parsed_content = env.parse(template_source)
        undeclared_vars = meta.find_undeclared_variables(parsed_content)
 
        # Use placeholder values for Jinja variables for validation purposes
        context = {var: 'placeholder' for var in undeclared_vars}
        template = env.get_template(template_name)
        rendered_yaml = template.render(context)
        self._logger.debug(f"Rendered YAML for {template_name}:\n{rendered_yaml}...")
 
        self._logger.debug(f"Loading YAML into Beam pipeline_spec: {template_name}")
        pipeline_spec = yaml.load(rendered_yaml, Loader=yaml_transform.SafeLineLoader)
 
        # Validate the pipeline spec against the generic schema without trying to
        # expand the transforms, which avoids the need for expansion services.
        yaml_transform.validate_against_schema(pipeline_spec, 'generic')
        self._logger.info(f"Successfully validated YAML syntax for: {template_name}")
 
    return test_method
 
 
class YamlSyntaxTest(unittest.TestCase):
    """A test suite for validating the syntax of Beam YAML templates.

    This class is dynamically populated with test methods, one for each
    .yaml file found in the `src/main/yaml` directory. This is accomplished
    by the `_create_tests` function, which runs at module-load time.
    """
    _logger = logging.getLogger(__name__)
 
 
def _create_tests():
    """Discovers all YAML templates and dynamically creates a test for each.

    This function scans the `src/main/yaml` directory for `.yaml` files and,
    for each file, generates a unique test method on the `YamlSyntaxTest` class.
    This allows `unittest` or `pytest` to discover and run each validation as a
    separate test case, making it easy to identify which template is invalid.
    """
    yaml_dir = os.path.join(os.path.dirname(__file__), '../../main/yaml')
    if not os.path.isdir(yaml_dir):
        return
 
    env = Environment(loader=FileSystemLoader(yaml_dir))
    for template_name in env.list_templates(filter_func=lambda x: x.endswith('.yaml')):
        test_name = f"test_{template_name.replace('.yaml', '').replace('-', '_')}"
        test_method = create_test_method(template_name, yaml_dir)
        setattr(YamlSyntaxTest, test_name, test_method)
 
_create_tests()

if __name__ == '__main__':
    unittest.main()