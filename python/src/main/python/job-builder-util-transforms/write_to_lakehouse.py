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

"""Module containing transforms to write data to Lakehouse tables."""

from typing import Iterable, Mapping, Optional
from apache_beam.transforms import PTransform
from apache_beam.yaml.yaml_io import write_to_iceberg


class WriteToLakehouse(PTransform):
  """A PTransform that writes data to a Lakehouse table.

  Currently, it wraps the Apache Iceberg sink.
  """

  def __init__(
      self,
      table: str,
      catalog_name: Optional[str] = None,
      catalog_properties: Optional[Mapping[str, str]] = None,
      config_properties: Optional[Mapping[str, str]] = None,
      partition_fields: Optional[Iterable[str]] = None,
      table_properties: Optional[Mapping[str, str]] = None,
      triggering_frequency_seconds: Optional[int] = None,
      keep: Optional[Iterable[str]] = None,
      drop: Optional[Iterable[str]] = None,
      only: Optional[str] = None,
      distribution_mode: Optional[str] = None,
      autosharding: Optional[bool] = None,
  ):
    super().__init__()
    self.table = table
    self.catalog_name = catalog_name
    self.catalog_properties = catalog_properties
    self.config_properties = config_properties
    self.partition_fields = partition_fields
    self.table_properties = table_properties
    self.triggering_frequency_seconds = triggering_frequency_seconds
    self.keep = keep
    self.drop = drop
    self.only = only
    self.distribution_mode = distribution_mode
    self.autosharding = autosharding

  def expand(self, pcoll):
    """Expands the WriteToLakehouse transform."""
    return pcoll | write_to_iceberg(
        table=self.table,
        catalog_name=self.catalog_name,
        catalog_properties=self.catalog_properties,
        config_properties=self.config_properties,
        partition_fields=self.partition_fields,
        table_properties=self.table_properties,
        triggering_frequency_seconds=self.triggering_frequency_seconds,
        keep=self.keep,
        drop=self.drop,
        only=self.only,
        distribution_mode=self.distribution_mode,
        autosharding=self.autosharding,
    )
