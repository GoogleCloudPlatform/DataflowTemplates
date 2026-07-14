import sys
import unittest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from read_from_delta_lake import ReadDeltaLakeDoFn, ReadFromDeltaLake


class ReadFromDeltaLakeTest(unittest.TestCase):

  def test_read_delta_lake_dofn(self):
    mock_deltalake = MagicMock()
    mock_pyarrow_table = MagicMock()
    mock_pyarrow_table.to_pylist.return_value = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
    ]
    mock_dt_instance = MagicMock()
    mock_dt_instance.to_pyarrow_table.return_value = mock_pyarrow_table
    mock_deltalake.DeltaTable.return_value = mock_dt_instance

    with patch.dict(sys.modules, {'deltalake': mock_deltalake}):
      dofn = ReadDeltaLakeDoFn(
          table='gs://bucket/delta',
          version=1,
          timestamp='2026-01-01T00:00:00Z',
          hadoop_config={'key': 'value'},
      )
      outputs = list(dofn.process(None))

    mock_deltalake.DeltaTable.assert_called_once_with(
        table_uri='gs://bucket/delta',
        version=1,
        storage_options={'key': 'value'},
    )
    mock_dt_instance.load_as_version.assert_called_once_with('2026-01-01T00:00:00Z')
    mock_dt_instance.to_pyarrow_table.assert_called_once_with()
    self.assertEqual(outputs, [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}])

  def test_read_from_delta_lake_transform(self):
    mock_deltalake = MagicMock()
    mock_pyarrow_table = MagicMock()
    mock_pyarrow_table.to_pylist.return_value = [
        {'id': 10, 'value': 'test_data'}
    ]
    mock_dt_instance = MagicMock()
    mock_dt_instance.to_pyarrow_table.return_value = mock_pyarrow_table
    mock_deltalake.DeltaTable.return_value = mock_dt_instance

    with patch.dict(sys.modules, {'deltalake': mock_deltalake}):
      with TestPipeline() as p:
        output = p | ReadFromDeltaLake(
            table='gs://bucket/delta_table',
            version=2,
            timestamp='2026-05-01T12:00:00Z',
            hadoop_config={'fs.gs.impl': 'gcs'},
        )
        assert_that(output, equal_to([{'id': 10, 'value': 'test_data'}]))


if __name__ == '__main__':
  unittest.main()
