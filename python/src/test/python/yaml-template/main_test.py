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

import unittest
from unittest.mock import patch
import os

import main


class MainTest(unittest.TestCase):

  def test_run_adds_requirements_file_if_not_present(self):
    argv = ['--yaml_pipeline=path/to/pipeline.yaml']

    with patch('main.main.run') as mock_run:
      main.run(argv)

      # Verify main.run was called
      mock_run.assert_called_once()
      called_args = mock_run.call_args[0][0]

      # Verify --requirements_file was added
      req_file_arg = None
      for arg in called_args:
        if arg.startswith('--requirements_file='):
          req_file_arg = arg
          break

      self.assertIsNotNone(req_file_arg)
      temp_path = req_file_arg.split('=', 1)[1]
      self.assertTrue(os.path.exists(temp_path))

      # Read content of the temp requirements file
      with open(temp_path, 'r') as f:
        content = f.read().splitlines()

      self.assertEqual(content, ['boto3', 'botocore'])

      # Clean up the temp file
      os.remove(temp_path)

  def test_run_does_not_override_existing_requirements_file(self):
    custom_req_path = 'custom/requirements.txt'
    argv = [
        '--yaml_pipeline=path/to/pipeline.yaml',
        f'--requirements_file={custom_req_path}',
    ]

    with patch('main.main.run') as mock_run:
      main.run(argv)

      mock_run.assert_called_once()
      called_args = mock_run.call_args[0][0]

      # Verify --requirements_file was not changed or duplicated
      req_file_args = [
          arg
          for arg in called_args
          if arg.startswith('--requirements_file=')
          or arg == '--requirements_file'
      ]
      self.assertEqual(len(req_file_args), 1)
      self.assertEqual(
          req_file_args[0], f'--requirements_file={custom_req_path}'
      )

  def test_run_does_not_override_existing_requirements_file_separated(self):
    custom_req_path = 'custom/requirements.txt'
    argv = [
        '--yaml_pipeline=path/to/pipeline.yaml',
        '--requirements_file',
        custom_req_path,
    ]

    with patch('main.main.run') as mock_run:
      main.run(argv)

      mock_run.assert_called_once()
      called_args = mock_run.call_args[0][0]

      # Verify --requirements_file was not changed or duplicated
      self.assertIn('--requirements_file', called_args)
      self.assertIn(custom_req_path, called_args)
      # Verify no other requirements_file arg was added
      req_file_args = [
          arg for arg in called_args if arg.startswith('--requirements_file=')
      ]
      self.assertEqual(len(req_file_args), 0)


if __name__ == '__main__':
  unittest.main()
