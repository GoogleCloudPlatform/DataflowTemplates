#
# Copyright (C) 2025 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
"""A template workflow to convert raw objects(text, images) into ArrayRecord files."""

import argparse
import logging
import os
import urllib

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from array_record.python.array_record_module import ArrayRecordWriter
from google.cloud import storage


class ConvertToArrayRecordGCS(beam.DoFn):
  """Write a tuple consisting of a filename and records to GCS ArrayRecords."""

  _WRITE_DIR = '/tmp/'

  def process(
      self,
      element,
      path,
      write_dir=_WRITE_DIR,
      file_path_suffix='.arrayrecord',
      overwrite_extension=False,
  ):

    ## Upload to GCS
    def upload_to_gcs(
        bucket_name, filename, prefix='', source_dir=self._WRITE_DIR
    ):
      source_filename = os.path.join(source_dir, filename)
      blob_name = os.path.join(prefix, filename)
      storage_client = storage.Client()
      bucket = storage_client.get_bucket(bucket_name)
      blob = bucket.blob(blob_name)
      blob.upload_from_filename(source_filename)

    ## Simple logic for stripping a file extension and replacing it
    def fix_filename(filename):
      base_name = os.path.splitext(filename)[0]
      new_filename = base_name + file_path_suffix
      return new_filename

    parsed_gcs_path = urllib.parse.urlparse(path)
    bucket_name = parsed_gcs_path.hostname
    gcs_prefix = parsed_gcs_path.path.lstrip('/')

    if overwrite_extension:
      filename = fix_filename(os.path.basename(element[0]))
    else:
      filename = '{}{}'.format(os.path.basename(element[0]), file_path_suffix)

    write_path = os.path.join(write_dir, filename)
    writer = ArrayRecordWriter(write_path, 'group_size:1')

    for item in element[1]:
      writer.write(bytes(item, 'utf-8'))

    writer.close()

    upload_to_gcs(bucket_name, filename, prefix=gcs_prefix)
    os.remove(os.path.join(write_dir, filename))


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_path',
      dest='input_path',
      default=(
          'gs://converter-datasets/input-datasets/google-top-terms/csv/1k/*.csv'
      ),
      help='Input file to process.',
  )
  parser.add_argument(
      '--input_format',
      dest='input_format',
      default='text|images',
      help='Input file format.',
  )
  parser.add_argument(
      '--output_destination',
      dest='output_destination',
      required=True,
      help='Output destination to write results to.',
  )
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    # TODO(iamphani): Move this out to array_record/beam/pipelines.py once it is
    # updated to support raw text.
    initial_files = p | 'Start' >> beam.Create([known_args.input_path])
    if known_args.input_format == 'text':
      parsed_files = initial_files | 'Read' >> beam.io.ReadAllFromText(
          with_filename=True
      )
    else:
      raise ValueError('Unsupported input format: %s' % known_args.input_format)
    _ = (
        parsed_files
        | 'Group' >> beam.GroupByKey()
        | 'Write to ArrayRecord in GCS'
        >> beam.ParDo(
            ConvertToArrayRecordGCS(),
            known_args.output_destination,
            file_path_suffix='.arrayrecord',
            overwrite_extension=False,
        )
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
