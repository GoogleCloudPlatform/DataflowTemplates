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

import unittest
from unittest.mock import MagicMock, patch
from write_to_lakehouse import WriteToLakehouse


class WriteToLakehouseTest(unittest.TestCase):

  @patch("write_to_lakehouse.managed.Write")
  def test_write_to_lakehouse(self, mock_managed_write):
    mock_transform = MagicMock()
    mock_managed_write.return_value = mock_transform

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
    pcoll.pipeline.options.view_as.return_value.beam_services = {}
    transform.expand(pcoll)

    mock_managed_write.assert_called_once()
    args, kwargs = mock_managed_write.call_args
    self.assertEqual(args[0], "iceberg")
    self.assertEqual(
        kwargs["config"],
        {
            "table": table,
            "catalog_name": catalog_name,
            "catalog_properties": catalog_properties,
            "config_properties": config_properties,
            "partition_fields": partition_fields,
            "table_properties": table_properties,
            "triggering_frequency_seconds": triggering_frequency_seconds,
            "keep": keep,
            "drop": drop,
            "only": only,
            "distribution_mode": distribution_mode,
            "autosharding": autosharding,
        },
    )
    self.assertIsNotNone(kwargs["expansion_service"])


if __name__ == "__main__":
  unittest.main()
