import os
import tempfile
import unittest
from unittest.mock import ANY, MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import managed
import pyarrow as pa
import pyarrow.parquet as pq
from read_from_delta_lake import DELTA_LAKE_READ_URN, ReadFromDeltaLake


class ReadFromDeltaLakeTest(unittest.TestCase):

  @patch('read_from_delta_lake.SchemaAwareExternalTransform')
  @patch.object(managed, 'DELTA', 'delta', create=True)
  @patch('read_from_delta_lake.managed.Read')
  def test_read_from_delta_lake_managed_read(self, mock_managed_read, mock_saet):
    mock_managed_read._READ_TRANSFORMS = {'delta': DELTA_LAKE_READ_URN}
    mock_transform = MagicMock()
    mock_managed_read.return_value = mock_transform


    table = 'gs://bucket/delta_table'
    version = 2
    timestamp = '2026-05-01T12:00:00Z'
    hadoop_config = {'fs.gs.project.id': 'test-project'}

    transform = ReadFromDeltaLake(
        table=table,
        version=version,
        timestamp=timestamp,
        hadoop_config=hadoop_config,
    )

    pbegin = MagicMock()
    transform.expand(pbegin)

    mock_managed_read.assert_called_once_with(
        'delta',
        config={
            'table': table,
            'version': version,
            'timestamp': timestamp,
            'hadoop_config': hadoop_config,
        },
    )


  @patch('read_from_delta_lake.SchemaAwareExternalTransform')
  @patch('read_from_delta_lake.managed.Read')
  def test_read_from_delta_lake_fallback(self, mock_managed_read, mock_saet):
    mock_managed_read._READ_TRANSFORMS = {}
    mock_transform = MagicMock()
    mock_saet.return_value = mock_transform

    table = '/path/to/table'
    hadoop_config = {'fs.gs.project.id': 'test-project'}

    transform = ReadFromDeltaLake(
        table=table,
        hadoop_config=hadoop_config,
    )

    pbegin = MagicMock()
    transform.expand(pbegin)

    mock_saet.assert_called_once_with(
        identifier=DELTA_LAKE_READ_URN,
        expansion_service=ANY,
        table=table,
        hadoop_config=hadoop_config,
    )

  def test_read_from_delta_lake_local_integration(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      table_dir = os.path.join(temp_dir, 'delta-table')
      os.makedirs(table_dir)

      # create parquet file
      parquet_file_path = os.path.join(table_dir, 'part-00000.parquet')
      table = pa.table({'name': ['test-name-1', 'test-name-2']})
      pq.write_table(table, parquet_file_path)

      file_size = os.path.getsize(parquet_file_path)

      log_dir = os.path.join(table_dir, '_delta_log')
      os.makedirs(log_dir)

      commit_content = (
          '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n'
          '{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},'
          '"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"name\\",\\'
          '"type\\":\\"string\\",\\"nullable\\":true,\\"metadata\\":{}}]}",'
          '"partitionColumns":[],"configuration":{},"createdAt":123456789}}\n'
          f'{{"add":{{"path":"part-00000.parquet","partitionValues":{{}},"size":{file_size},'
          '"modificationTime":123456789,"dataChange":true}}\n'
      )

      # create delta log
      with open(os.path.join(log_dir, '00000000000000000000.json'), 'w') as f:
        f.write(commit_content)

      with TestPipeline() as p:
        output = (
            p
            | ReadFromDeltaLake(table=table_dir)
            | beam.Map(lambda row: row._asdict())
        )

        expected = [{'name': 'test-name-1'}, {'name': 'test-name-2'}]
        assert_that(output, equal_to(expected))



if __name__ == '__main__':
  unittest.main()


