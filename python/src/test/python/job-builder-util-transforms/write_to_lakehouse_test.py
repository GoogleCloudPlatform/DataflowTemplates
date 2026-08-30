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
from unittest.mock import ANY, MagicMock, patch
from apache_beam.transforms import managed
from write_to_lakehouse import ICEBERG_WRITE_URN, WriteToLakehouse


class WriteToLakehouseTest(unittest.TestCase):

  @patch("write_to_lakehouse.SchemaAwareExternalTransform")
  def test_write_to_lakehouse(self, mock_schema_aware_transform):
    mock_transform = MagicMock()
    mock_schema_aware_transform.return_value = mock_transform

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

    mock_schema_aware_transform.assert_called_once()
    _, kwargs = mock_schema_aware_transform.call_args
    self.assertEqual(kwargs["identifier"], "beam:schematransform:org.apache.beam:iceberg_write:v1")
    self.assertEqual(kwargs["table"], table)
    self.assertEqual(kwargs["catalog_name"], catalog_name)
    self.assertEqual(kwargs["catalog_properties"], catalog_properties)
    self.assertEqual(kwargs["config_properties"], config_properties)
    self.assertEqual(kwargs["partition_fields"], partition_fields)
    self.assertEqual(kwargs["table_properties"], table_properties)
    self.assertEqual(kwargs["triggering_frequency_seconds"], triggering_frequency_seconds)
    self.assertEqual(kwargs["keep"], keep)
    self.assertEqual(kwargs["drop"], drop)
    self.assertEqual(kwargs["only"], only)
    self.assertEqual(kwargs["distribution_mode"], distribution_mode)
    self.assertEqual(kwargs["autosharding"], autosharding)
    self.assertTrue(kwargs["rearrange_based_on_discovery"])
    self.assertIsNotNone(kwargs["expansion_service"])


if __name__ == "__main__":
  unittest.main()
