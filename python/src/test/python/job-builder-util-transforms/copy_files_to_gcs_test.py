import unittest
from unittest.mock import patch
import os
import tempfile
import apache_beam as beam
from apache_beam import Row
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from copy_files_to_gcs import CopyFilesToGCSDoFn
from copy_files_to_gcs import CopyFilesToGCS

class TransformsTest(unittest.TestCase):

  def test_copy_files_to_gcs_dofn_gcs_input(self):
    dofn = CopyFilesToGCSDoFn(job_level_gcs_prefix='gs://bucket/job/')
    dofn.start_bundle()
    
    input_row = Row(path='gs://bucket/source/file.txt')
    outputs = list(dofn.process(input_row))
    
    self.assertEqual(len(outputs), 1)
    self.assertEqual(outputs[0], input_row)

  def test_copy_files_to_gcs_dofn_local_input(self):
    with tempfile.TemporaryDirectory() as tmp_dir:
      src_file = os.path.join(tmp_dir, 'src.txt')
      with open(src_file, 'w') as f:
        f.write('hello world')
        
      dest_prefix = os.path.join(tmp_dir, 'dest') + '/'
      
      dofn = CopyFilesToGCSDoFn(job_level_gcs_prefix=dest_prefix)
      dofn.start_bundle()
      
      input_row = Row(path=src_file)
      outputs = list(dofn.process(input_row))
      
      self.assertEqual(len(outputs), 1)
      out_path = outputs[0].path
      self.assertTrue(out_path.startswith(dest_prefix))
      self.assertTrue(out_path.endswith('src.txt'))
      
      # Verify file was copied
      self.assertTrue(os.path.exists(out_path))
      with open(out_path, 'r') as f:
        self.assertEqual(f.read(), 'hello world')

  def test_copy_files_to_gcs_transform(self):
    with tempfile.TemporaryDirectory() as tmp_dir:
      src_file = os.path.join(tmp_dir, 'src.txt')
      with open(src_file, 'w') as f:
        f.write('hello world')
        
      dest_prefix = os.path.join(tmp_dir, 'dest') + '/'
      
      with patch('copy_files_to_gcs.secrets.token_urlsafe', return_value='fixed_token'):
        with TestPipeline() as p:
          input_rows = p | beam.Create([Row(path=src_file)])
          output = input_rows | CopyFilesToGCS(gcs_file_path=dest_prefix)
          
          expected_path = dest_prefix + 'fixed_token/' + 'fixed_token/' + 'src.txt'
          assert_that(output, equal_to([Row(path=expected_path)]))

if __name__ == '__main__':
  unittest.main()
