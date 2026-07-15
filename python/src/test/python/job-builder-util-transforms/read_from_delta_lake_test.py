import unittest
from unittest.mock import MagicMock, patch
from apache_beam.transforms import managed
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
  def test_read_from_delta_lake_fallback(self, mock_saet):
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
        table=table,
        hadoop_config=hadoop_config,
    )



if __name__ == '__main__':
  unittest.main()


