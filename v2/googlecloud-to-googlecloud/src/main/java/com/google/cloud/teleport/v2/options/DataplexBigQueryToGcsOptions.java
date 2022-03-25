/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.v2.transforms.DeleteBigQueryDataFn;
import com.google.cloud.teleport.v2.transforms.UpdateDataplexBigQueryToGcsExportMetadataTransform;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * The {@link DataplexBigQueryToGcsOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 */
public interface DataplexBigQueryToGcsOptions
    extends GcpOptions,
        ExperimentalOptions,
        DeleteBigQueryDataFn.Options,
        UpdateDataplexBigQueryToGcsExportMetadataTransform.Options {

  @Description(
      "BigQuery dataset to tier data from. Format: "
          + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
          + "-name> (Dataplex asset name) or projects/<name>/datasets/<dataset-id> (BigQuery"
          + " dataset ID).")
  @Required
  String getSourceBigQueryDataset();

  void setSourceBigQueryDataset(String sourceBigQueryDataset);

  @Description(
      "A comma-separated list of BigQuery tables to tier. If none specified, all tables will be"
          + " tiered. Tables should be specified by their name only (no project/dataset prefix)."
          + " Case-sensitive!")
  String getTables();

  void setTables(String tables);

  @Description(
      "Dataplex asset name for the the Cloud Storage bucket to tier data to. Format:"
          + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
          + " name>.")
  @Required
  String getDestinationStorageBucketAssetName();

  void setDestinationStorageBucketAssetName(String destinationStorageBucketAssetName);

  @Description(
      "The parameter can either be: 1) unspecified, 2) date (and optional time) 3) Duration.\n"
          + "1) If not specified move all tables / partitions.\n"
          + "2) Move data older than this date (and optional time). For partitioned tables, move"
          + " partitions last modified before this date/time. For non-partitioned tables, move if"
          + " the table was last modified before this date/time. If not specified, move all tables"
          + " / partitions. The date/time is parsed in the default time zone by default, but"
          + " optinal suffixes Z and +HH:mm are supported. Format: YYYY-MM-DD or"
          + " YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+03:00.\n"
          + "3) Similar to the above (2) but the effective date-time is derived from the current"
          + " time in the default/system timezone shifted by the provided duration in the format"
          + " based on ISO-8601 +/-PnDTnHnMn.nS "
          + "(https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-)."
          + " However only \"minus\" durations are accepted so only past effective date-times are"
          + " possible.")
  String getExportDataModifiedBeforeDateTime();

  void setExportDataModifiedBeforeDateTime(String exportDataModifiedBeforeDateTime);

  @Description(
      "The maximum number of parallel requests that will be sent to BigQuery when loading"
          + " table/partition metadata. Default: 5.")
  @Default.Integer(5)
  @Required
  Integer getMaxParallelBigQueryMetadataRequests();

  void setMaxParallelBigQueryMetadataRequests(Integer maxParallelBigQueryMetadataRequests);

  @Description("Output file format in GCS. Format: PARQUET, AVRO, or ORC. Default: PARQUET.")
  @Default.Enum("PARQUET")
  @Required
  FileFormatOptions getFileFormat();

  void setFileFormat(FileFormatOptions fileFormat);

  @Description(
      "Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. Default:"
          + " SNAPPY. BZIP2 not supported for PARQUET files.")
  @Default.Enum("SNAPPY")
  DataplexCompression getFileCompression();

  void setFileCompression(DataplexCompression fileCompression);

  @Description(
      "Process partitions with partition ID matching this regexp only. Default: process all.")
  String getPartitionIdRegExp();

  void setPartitionIdRegExp(String partitionIdRegExp);

  @Description(
      "Specifies the action that occurs if destination file already exists. Format: OVERWRITE,"
          + " FAIL, SKIP. Default: SKIP.")
  @Default.Enum("SKIP")
  WriteDispositionOptions getWriteDisposition();

  void setWriteDisposition(WriteDispositionOptions writeDisposition);

  @Description(
      "Due to a BigQuery limitation, it's not possible to have a partitioned external table with"
          + " the partition key (in the file path) having the same name as one of the columns in"
          + " the file. If enforceSamePartitionKey is true (the default), the partition key of"
          + " the target file will be set to the original partition column name and the column in"
          + " the file will be renamed. If false, it's the partition key that will be renamed.")
  @Default.Boolean(true)
  Boolean getEnforceSamePartitionKey();

  void setEnforceSamePartitionKey(Boolean enforceSamePartitionKey);
}
