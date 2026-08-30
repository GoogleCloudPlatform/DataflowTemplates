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
from apache_beam.options.pipeline_options import CrossLanguageOptions
from apache_beam.transforms import PTransform
from apache_beam.transforms import managed
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform

ICEBERG_WRITE_URN = "beam:schematransform:org.apache.beam:iceberg_write:v1"


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
    config = {
        'table': self.table,
    }
    if self.catalog_name is not None:
      config['catalog_name'] = self.catalog_name
    if self.catalog_properties is not None:
      config['catalog_properties'] = dict(self.catalog_properties)
    if self.config_properties is not None:
      config['config_properties'] = dict(self.config_properties)
    if self.partition_fields is not None:
      config['partition_fields'] = list(self.partition_fields)
    if self.table_properties is not None:
      config['table_properties'] = dict(self.table_properties)
    if self.triggering_frequency_seconds is not None:
      config['triggering_frequency_seconds'] = self.triggering_frequency_seconds
    if self.keep is not None:
      config['keep'] = list(self.keep)
    if self.drop is not None:
      config['drop'] = list(self.drop)
    if self.only is not None:
      config['only'] = self.only
    if self.distribution_mode is not None:
      config['distribution_mode'] = self.distribution_mode
    if self.autosharding is not None:
      config['autosharding'] = self.autosharding

    iceberg_sink = getattr(managed, 'ICEBERG', 'iceberg')
    if hasattr(managed, 'Write') and iceberg_sink in getattr(
        managed.Write, '_WRITE_TRANSFORMS', {}
    ):
      return pcoll | managed.Write(iceberg_sink, config=config)
    else:
      options = pcoll.pipeline.options
      beam_services = options.view_as(CrossLanguageOptions).beam_services or {}
      if 'sdks:java:io:expansion-service:shadowJar' in beam_services:
        expansion_service = BeamJarExpansionService(
            'sdks:java:io:expansion-service:shadowJar'
        )
      else:
        expansion_service = JavaJarExpansionService(
            'https://storage.googleapis.com/dataflow-templates/extra-python-packages/2026-08-29/expansion-service-custom-0.3.1.jar'
        )

      return pcoll | SchemaAwareExternalTransform(
          identifier=ICEBERG_WRITE_URN,
          expansion_service=expansion_service,
          rearrange_based_on_discovery=True,
          **config,
      )
