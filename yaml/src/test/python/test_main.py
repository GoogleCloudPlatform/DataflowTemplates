import sys
import os
import unittest
import yaml
from unittest.mock import MagicMock, patch, mock_open

# Mock apache_beam modules before importing main
sys.modules['apache_beam'] = MagicMock()
sys.modules['apache_beam.io'] = MagicMock()
sys.modules['apache_beam.io.filesystems'] = MagicMock()
sys.modules['apache_beam.yaml'] = MagicMock()
sys.modules['apache_beam.yaml.cache_provider_artifacts'] = MagicMock()
sys.modules['apache_beam.yaml.main'] = MagicMock()

# Add the src/main/python directory to sys.path to import main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../main/python')))

import main

class TestMain(unittest.TestCase):

    def test_clean_undefined_recursively(self):
        # Test dictionary cleaning
        data = {
            'a': 1,
            'b': main.UNDEFINED_MARKER,
            'c': {
                'd': 2,
                'e': main.UNDEFINED_MARKER
            }
        }
        expected = {'a': 1, 'c': {'d': 2}}
        self.assertEqual(main._clean_undefined_recursively(data), expected)

        # Test list cleaning
        data = [1, main.UNDEFINED_MARKER, 2]
        expected = [1, 2]
        self.assertEqual(main._clean_undefined_recursively(data), expected)

        # Test mixed
        data = {
            'a': [1, main.UNDEFINED_MARKER],
            'b': main.UNDEFINED_MARKER
        }
        expected = {'a': [1]}
        self.assertEqual(main._clean_undefined_recursively(data), expected)

    def test_extract_jinja_variable_names(self):
        yaml_content = """
        pipeline:
            name: {{ pipeline_name }}
            transform:
                - type: {{ transform_type | default('Read') }}
        """
        expected = {'pipeline_name', 'transform_type'}
        self.assertEqual(main._extract_jinja_variable_names(yaml_content), expected)

    def test_get_cleaned_pipeline(self):
        yaml_template = """
        pipeline:
            name: {{ name }}
            optional: {{ optional_var }}
        """
        # Case 1: All variables provided
        provided = {'name': 'test_pipeline', 'optional_var': 'value'}
        result = main._get_cleaned_pipeline(yaml_template, provided)
        self.assertIn('name: test_pipeline', result)
        self.assertIn('optional: value', result)

        # Case 2: Optional variable missing (should be removed)
        provided = {'name': 'test_pipeline'}
        result = main._get_cleaned_pipeline(yaml_template, provided)
        self.assertIn('name: test_pipeline', result)
        self.assertNotIn('optional:', result)

    @patch('main.FileSystems')
    def test_get_pipeline_yaml(self, mock_fs):
        mock_f = mock_open(read_data=b"pipeline: test")
        mock_fs.open.return_value = mock_f()
        
        result = main._get_pipeline_yaml()
        self.assertEqual(result, "pipeline: test")
        mock_fs.open.assert_called_with("template.yaml")

    @patch('main.FileSystems')
    def test_get_pipeline_with_options(self, mock_fs):
        # Setup mocks
        yaml_pipeline = """
        template:
            options_file:
                - my_options
            parameters:
                - my_param
        """
        options_content = """
        options:
            - name: my_param
              parameters:
                  - expanded_param_1
                  - expanded_param_2
        """
        
        # We need to handle multiple open calls if necessary, but here only one option file
        mock_f = mock_open(read_data=options_content)
        mock_fs.open.side_effect = [mock_f()] 

        result = main._get_pipeline_with_options(yaml_pipeline)
        
        self.assertIn('expanded_param_1', result)
        self.assertIn('expanded_param_2', result)
        self.assertNotIn('options_file', result)

    @patch('main._get_pipeline_yaml')
    @patch('main._get_cleaned_pipeline')
    @patch('main._get_pipeline_with_options')
    @patch('main.cache_provider_artifacts')
    @patch('main.main') # apache_beam.yaml.main
    def test_run(self, mock_beam_main, mock_cache, mock_get_options, mock_get_cleaned, mock_get_yaml):
        # Setup
        mock_get_yaml.return_value = "pipeline: {{ var1 }}"
        mock_get_cleaned.return_value = "cleaned_pipeline"
        mock_get_options.return_value = "final_pipeline"
        
        test_args = ['--var1=value1', '--runner=DirectRunner']
        
        # Execute
        main.run(test_args)
        
        # Verify
        mock_get_cleaned.assert_called()
        # Check if provided_jinja_vars were passed correctly
        call_args = mock_get_cleaned.call_args
        self.assertEqual(call_args[0][0], "pipeline: {{ var1 }}")
        self.assertEqual(call_args[0][1], {'var1': 'value1'})
        
        mock_get_options.assert_called_with("cleaned_pipeline")
        mock_cache.cache_provider_artifacts.assert_called()
        
        # Verify final arguments passed to beam main.run
        beam_run_args = mock_beam_main.run.call_args[1]['argv']
        self.assertIn('--runner=DirectRunner', beam_run_args)
        self.assertIn('--yaml_pipeline=final_pipeline', beam_run_args)
        # Ensure jinja args are removed from pipeline args
        self.assertNotIn('--var1=value1', beam_run_args)

if __name__ == '__main__':
    unittest.main()
