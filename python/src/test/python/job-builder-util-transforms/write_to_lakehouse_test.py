# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import inspect
import unittest
from unittest.mock import MagicMock, patch

from apache_beam.yaml.yaml_io import write_to_iceberg
from write_to_lakehouse import WriteToLakehouse


class WriteToLakehouseTest(unittest.TestCase):

  def test_parameters_match_iceberg(self):
    """Ensure that the set of parameters does not deviate from Iceberg."""
    lakehouse_params = inspect.signature(WriteToLakehouse).parameters
    iceberg_params = inspect.signature(write_to_iceberg).parameters

    self.assertEqual(
        set(lakehouse_params.keys()),
        set(iceberg_params.keys()),
        "The set of parameters in WriteToLakehouse deviates from write_to_iceberg.",
    )
    for name, param in iceberg_params.items():
      self.assertEqual(
          lakehouse_params[name].default,
          param.default,
          f"Default value for parameter '{name}' deviates from write_to_iceberg.",
      )

  @patch("write_to_lakehouse.write_to_iceberg")
  def test_write_to_lakehouse(self, mock_write_to_iceberg):
    mock_transform = MagicMock()
    mock_write_to_iceberg.return_value = mock_transform

    table = "lakehouse_catalog.dataset.table"
    catalog_name = "lakehouse_catalog"
    catalog_properties = {"type": "hadoop", "warehouse": "gs://bucket/warehouse"}
    config_properties = {"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}
    partition_fields = ["day(ts)", "category"]
    table_properties = {"commit.retry.num-retries": "2"}
    triggering_frequency_seconds = 60
    keep = ["field1", "field2"]
    drop = ["field3"]
    only = "field4"
    distribution_mode = "hash"
    autosharding = True

    transform = WriteToLakehouse(
        table=table,
        catalog_name=catalog_name,
        catalog_properties=catalog_properties,
        config_properties=config_properties,
        partition_fields=partition_fields,
        table_properties=table_properties,
        triggering_frequency_seconds=triggering_frequency_seconds,
        keep=keep,
        drop=drop,
        only=only,
        distribution_mode=distribution_mode,
        autosharding=autosharding,
    )

    pcoll = MagicMock()
    transform.expand(pcoll)

    mock_write_to_iceberg.assert_called_once_with(
        table=table,
        catalog_name=catalog_name,
        catalog_properties=catalog_properties,
        config_properties=config_properties,
        partition_fields=partition_fields,
        table_properties=table_properties,
        triggering_frequency_seconds=triggering_frequency_seconds,
        keep=keep,
        drop=drop,
        only=only,
        distribution_mode=distribution_mode,
        autosharding=autosharding,
    )


if __name__ == "__main__":
  unittest.main()
