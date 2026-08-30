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
  @patch.object(managed, "ICEBERG", "iceberg", create=True)
  @patch("write_to_lakehouse.managed.Write")
  def test_write_to_lakehouse_managed_write(self, mock_managed_write, mock_saet):
    mock_managed_write._WRITE_TRANSFORMS = {"iceberg": ICEBERG_WRITE_URN}
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
    transform.expand(pcoll)

    mock_managed_write.assert_called_once_with(
        "iceberg",
        config={
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

  @patch("write_to_lakehouse.SchemaAwareExternalTransform")
  @patch("write_to_lakehouse.managed.Write")
  def test_write_to_lakehouse_fallback(self, mock_managed_write, mock_saet):
    mock_managed_write._WRITE_TRANSFORMS = {}
    mock_transform = MagicMock()
    mock_saet.return_value = mock_transform

    table = "lakehouse_catalog.dataset.table"
    catalog_properties = {"type": "hadoop", "warehouse": "gs://bucket/warehouse"}

    transform = WriteToLakehouse(
        table=table,
        catalog_properties=catalog_properties,
    )

    pcoll = MagicMock()
    pcoll.pipeline.options.view_as.return_value.beam_services = {}
    transform.expand(pcoll)

    mock_saet.assert_called_once_with(
        identifier=ICEBERG_WRITE_URN,
        expansion_service=ANY,
        rearrange_based_on_discovery=True,
        table=table,
        catalog_properties=catalog_properties,
    )


if __name__ == "__main__":
  unittest.main()
