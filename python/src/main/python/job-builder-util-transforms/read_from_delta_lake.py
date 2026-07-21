"""Module containing transforms to read data from Delta Lake tables."""

from typing import Mapping, Optional
from apache_beam.transforms import PTransform
from apache_beam.transforms import managed
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform

DELTA_LAKE_READ_URN = "beam:schematransform:org.apache.beam:delta_lake_read:v1"


class ReadFromDeltaLake(PTransform):
  """A PTransform that reads data from a Delta Lake table.

  Args:
    table: Identifier or path of the Delta Lake table.
    version: Version of the Delta Lake table to read.
    timestamp: Timestamp of the Delta Lake table to read.
    hadoop_config: Properties passed to Hadoop Configuration.
  """

  def __init__(
      self,
      table: str,
      version: Optional[int] = None,
      timestamp: Optional[str] = None,
      hadoop_config: Optional[Mapping[str, str]] = None,
  ):
    super().__init__()
    self.table = table
    self.version = version
    self.timestamp = timestamp
    self.hadoop_config = hadoop_config


  def expand(self, pbegin):
    """Expands the ReadFromDeltaLake transform."""
    config = {
        'table': self.table,
    }
    if self.version is not None:
      config['version'] = self.version
    if self.timestamp is not None:
      config['timestamp'] = self.timestamp
    if self.hadoop_config is not None:
      config['hadoop_config'] = dict(self.hadoop_config)

    delta_source = getattr(managed, 'DELTA', 'delta')
    if hasattr(managed, 'Read') and delta_source in getattr(
        managed.Read, '_READ_TRANSFORMS', {}
    ):
      return pbegin | managed.Read(delta_source, config=config)
    else:
      return pbegin | SchemaAwareExternalTransform(
          identifier=DELTA_LAKE_READ_URN,
          expansion_service=BeamJarExpansionService(
              'sdks:java:io:expansion-service:shadowJar'
          ),
          **config,
      )


