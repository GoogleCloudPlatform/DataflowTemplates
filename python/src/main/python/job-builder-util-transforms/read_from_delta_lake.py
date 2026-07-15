"""Module containing transforms to read data from Delta Lake tables."""

from typing import Mapping, Optional
import apache_beam as beam
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform


class ReadDeltaLakeDoFn(DoFn):
  """A DoFn that opens and reads records from a Delta Lake table."""

  def __init__(
      self,
      table: str,
      version: Optional[int] = None,
      timestamp: Optional[str] = None,
      storage_options: Optional[Mapping[str, str]] = None,
  ):
    self.table = table
    self.version = version
    self.timestamp = timestamp
    self.storage_options = storage_options

  def process(self, unused_element):
    """Reads the Delta Lake table and yields each row as a dictionary."""
    from deltalake import DeltaTable

    dt = DeltaTable(
        table_uri=self.table,
        version=self.version,
        storage_options=self.storage_options,
    )
    if self.timestamp is not None:
      dt.load_as_version(self.timestamp)

    for batch in dt.to_pyarrow_dataset().to_batches():
      for row in batch.to_pylist():
        yield row


class ReadFromDeltaLake(PTransform):
  """A PTransform that reads data from a Delta Lake table.

  Args:
    table: Identifier or path of the Delta Lake table.
    version: Version of the Delta Lake table to read.
    timestamp: Timestamp of the Delta Lake table to read.
    storage_options: Storage options (e.g. cloud-specific options like
      google_service_account_key) for connecting to storage.
  """

  def __init__(
      self,
      table: str,
      version: Optional[int] = None,
      timestamp: Optional[str] = None,
      storage_options: Optional[Mapping[str, str]] = None,
  ):
    super().__init__()
    self.table = table
    self.version = version
    self.timestamp = timestamp
    self.storage_options = storage_options

  def expand(self, pbegin):
    """Expands the ReadFromDeltaLake transform."""
    return (
        pbegin
        | beam.Create([None])
        | ParDo(
            ReadDeltaLakeDoFn(
                table=self.table,
                version=self.version,
                timestamp=self.timestamp,
                storage_options=self.storage_options,
            )
        )
    )
