# Copyright (C) 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law-or-agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import os
import unittest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open
from src.main.python import generate_yaml_java_templates

#..main.python.generate_yaml_java_templates

# Assuming the script is run from the root of the DataflowTemplates project

class GenerateYamlJavaTemplatesTest(unittest.TestCase):

  @patch('subprocess.check_output')
  def test_get_git_root_success(self, mock_check_output):
    mock_check_output.return_value = b'/path/to/git/root\n'
    self.assertEqual(generate_yaml_java_templates.get_git_root(), b'/path/to/git/root')

  @patch('subprocess.check_output')
  def test_get_git_root_failure(self, mock_check_output):
    mock_check_output.side_effect = subprocess.CalledProcessError(1, 'git')
    self.assertIsInstance(generate_yaml_java_templates.get_git_root(), subprocess.CalledProcessError)

  @patch('src.main.python.generate_yaml_java_templates.run_mvn_spotless')
  def test_generate_java_interface(self, mock_run_mvn_spotless):

    yaml_path = Path(__file__).parent / "generate_yaml_java_templates_test_input.yaml"

    # Generate tmp java file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.java') as java_template_file:
      java_path = Path(java_template_file.name)
      generate_yaml_java_templates.generate_java_interface(yaml_path, java_path)

    # Read the generated Java file
    with open(java_path, 'r') as f:
      generated_code = f.read()
    
    # Create the expected code
    expected_code = f"""
/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates.yaml;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "Kafka_to_BigQuery_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Kafka to BigQuery (YAML)",
    description = "The Apache Kafka ...",
    flexContainerName = "kafka-to-bigquery-yaml",
    yamlTemplateFile = "{yaml_path.name}",
    filesToCopy = {{"template.yaml", "main.py", "requirements.txt"}},
    documentation = "https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {{"req1",
      "req2"
    }},
    streaming = true,
    hidden = false)
public interface {java_path.stem} {{

  @TemplateParameter.Text(
      order = 1,
      name = "param1",
      optional = false,
      description = "A text parameter",
      helpText = "",
      example = ""
    )
  @Validation.Required
  String getParam1();

  @TemplateParameter.Integer(
      order = 2,
      name = "param2",
      optional = true,
      description = "An integer parameter",
      helpText = "",
      example = ""
    )
  @Default.Integer(10)
  Integer getParam2();

  @TemplateParameter.Enum(
      order = 3,
      name = "param3",
      optional = true,
      description = "An enum parameter",
      helpText = "",
      example = ""
    )
  @Default.Enum(VALUE1)
  Enum getParam3();

  @TemplateParameter.Password(
      order = 4,
      name = "param4",
      optional = true,
      description = "A password parameter",
      helpText = "",
      example = ""
    )
  String getParam4();

  @TemplateParameter.Text(
      order = 5,
      name = "param5",
      optional = true,
      description = "A map parameter",
      helpText = "",
      example = ""
    )
  String getParam5();
}}
"""
    self.assertMultiLineEqual(generated_code.strip(), expected_code.strip())

    # # Clean up the temporary files
    os.remove(java_path)



if __name__ == '__main__':
  unittest.main()
