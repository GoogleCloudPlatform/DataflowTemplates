"""Module containing transforms to copy files to Google Cloud Storage (GCS)."""

import apache_beam as beam
from apache_beam import Row
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.io.filesystems import FileSystems
from apache_beam.typehints.row_type import RowTypeConstraint
import shutil
import secrets

class CopyFilesToGCSDoFn(DoFn):
  """A DoFn that copies input files to a GCS destination.
  
  It skips files that are already on GCS (starting with 'gs://').
  It uses a bundle-level random prefix to avoid duplicate writes on retries.
  """
  COPY_BUFFER_SIZE = 16*1024*1024  # 16MB
  JOB_LEVEL_PREFIX = 'migrated-data-'  # 16MB
  
  def __init__(self, job_level_gcs_prefix):
    """Initializes the DoFn.
    
    Args:
      job_level_gcs_prefix: The job-level GCS prefix to use for copying files.
    """
    self.job_level_gcs_prefix = job_level_gcs_prefix

  def start_bundle(self):
    """Starts a bundle. Generates a unique bundle-level prefix if a job-level prefix is set."""
    # Generating a random string to be added to the path during bundle startup
    # so that subquent steps only consider elements from succeessful bundles executions
    # in case of a bundle retry by the runner.
    if self.job_level_gcs_prefix:
      self.bundle_level_gcs_prefix = self.job_level_gcs_prefix + secrets.token_urlsafe(32) + '/'
    else:
      self.bundle_level_gcs_prefix = None

  def process(self, input_file):
    """Processes an input file.
    
    Args:
      input_file: A `Row` object with a `path` attribute representing the file path.
      
    Yields:
      A `Row` object containing the new path of the file on GCS, or the original path if it was already on GCS.
    """
    if input_file.path.startswith('gs://'):
      # No need to copy over GCS files.
      yield input_file
      return

    if not self.bundle_level_gcs_prefix:
      raise ValueError(f"Encountered non-GCS file {input_file.path} but no gcs_file_path was provided to copy to.")

    file_name = input_file.path.split('/')[-1]
    gcs_file_path = self.bundle_level_gcs_prefix + file_name
    with FileSystems.open(input_file.path, 'application/octet-stream') as external_file:
      with FileSystems.create(gcs_file_path, 'application/octet-stream') as gcs_file:
        shutil.copyfileobj(external_file, gcs_file, length=CopyFilesToGCSDoFn.COPY_BUFFER_SIZE)

    yield Row(path=gcs_file_path)


class CopyFilesToGCS(PTransform):
  """A PTransform that copies files to GCS.
  
  It takes a PCollection of objects with a `path` attribute and copies non-GCS files to the specified GCS destination.

  This returns a PCollection of `Row` objects with the new path of the file on GCS, or the original path if it was already on GCS.

  In case of a bundle retry by the runner, this makes sure to only produce elements of successful bundles.
  Failed bundles may leave temporary files behind that should not be considered as part of the pipeline output.
  """
  def __init__(self, gcs_file_path):
    """Initializes the transform.
    
    Args:
      gcs_file_path: The base GCS path where files should be copied.
    """
    self.gcs_file_path = gcs_file_path

  def expand(self, pcoll):
    """Expands the transform.
    
    Generates a job-level random prefix and applies `CopyFilesToGCSDoFn`.
    """
    if self.gcs_file_path:
      gcs_file_path = self.gcs_file_path if self.gcs_file_path.endswith('/') else self.gcs_file_path + '/'
      # Generating a job level random prefix so that files copied in a single job can be easily identified.
      job_level_gcs_prefix = gcs_file_path + secrets.token_urlsafe(32) + '/'
    else:
      job_level_gcs_prefix = None
    return pcoll | ParDo(CopyFilesToGCSDoFn(job_level_gcs_prefix)).with_output_types(RowTypeConstraint.from_fields([('path', str)]))
