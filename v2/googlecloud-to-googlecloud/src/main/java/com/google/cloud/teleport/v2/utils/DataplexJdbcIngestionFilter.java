/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionOptions;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filter to exclude unwanted records if the target file exists. */
public class DataplexJdbcIngestionFilter
    extends PTransform<PCollection<GenericRecord>, PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(DataplexJdbcIngestionFilter.class);

  private final String targetRootPath;
  private final String partitionColumnName;
  private final PartitioningSchema partitioningSchema;
  private final String fileSuffix;
  private final WriteDispositionOptions writeDisposition;
  private final List<String> existingFiles;
  /** The tag for filtered records. */
  private TupleTag<GenericRecord> filteredRecordsOutputTag;
  /** The tag for existing target file names. */
  private TupleTag<String> existingTargetFilesOutputTag;
  private ZoneId zoneId;

  public DataplexJdbcIngestionFilter(
      String targetRootPath,
      String serializedAvroSchema,
      String partitionColumnName,
      PartitioningSchema partitioningSchema,
      String fileSuffix,
      WriteDispositionOptions writeDisposition,
      List<String> existingFiles,
      TupleTag<GenericRecord> filteredRecordsOutputTag,
      TupleTag<String> existingTargetFilesOutputTag) {
    this.targetRootPath = targetRootPath;
    this.partitionColumnName = partitionColumnName;
    this.partitioningSchema = partitioningSchema;
    this.fileSuffix = fileSuffix;
    this.writeDisposition = writeDisposition;
    this.existingFiles = existingFiles;
    this.filteredRecordsOutputTag = filteredRecordsOutputTag;
    this.existingTargetFilesOutputTag = existingTargetFilesOutputTag;
    this.zoneId =
        DataplexJdbcPartitionUtils.getZoneId(
            SchemaUtils.parseAvroSchema(serializedAvroSchema), partitionColumnName);
  }

  @Override
  public PCollectionTuple expand(PCollection<GenericRecord> input) {
    return input.apply(
        ParDo.of(new DataplexJdbcIngestionFilterDoFn())
            .withOutputTags(
                filteredRecordsOutputTag, TupleTagList.of(existingTargetFilesOutputTag)));
  }

  private class DataplexJdbcIngestionFilterDoFn extends DoFn<GenericRecord, GenericRecord> {

    private boolean shouldSkipRecord(String expectedFilePath) {
      switch (writeDisposition) {
        case WRITE_EMPTY:
          throw new WriteDispositionException(
              String.format(
                  "Target File %s already exists in the output asset bucket %s. Failing"
                      + " according to writeDisposition.",
                  expectedFilePath, targetRootPath));
        case SKIP:
          return true;
        case WRITE_TRUNCATE:
          return false;
        default:
          throw new UnsupportedOperationException(
              writeDisposition + " writeDisposition not implemented for writing to GCS.");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      GenericRecord record = c.element();
      List<KV<String, Integer>> partition =
          partitioningSchema.toPartition(
              Instant.ofEpochMilli(
                      DataplexJdbcPartitionUtils.partitionColumnValueToMillis(
                          record.get(partitionColumnName)))
                  .atZone(zoneId));
      String expectedFilePath =
          new DataplexJdbcIngestionNaming(
                  DataplexJdbcPartitionUtils.partitionToPath(partition), fileSuffix)
              .getSingleFilename();
      if (existingFiles.contains(expectedFilePath)) {
        // Target file exists, performing writeDisposition strategy
        if (!shouldSkipRecord(expectedFilePath)) {
          c.output(record);
        }
        // Returning existing file name for logging
        c.output(existingTargetFilesOutputTag, expectedFilePath);
      } else {
        // If target file does not exist, do not filter out the record
        c.output(record);
      }
    }
  }
}
