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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.transforms.DeleteBigQueryDataFn;
import com.google.cloud.teleport.v2.utils.DataplexWriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
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
        DataplexUpdateMetadataOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {
        "^(projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/assets\\/[^\\n\\r\\/]+|projects\\/[^\\n\\r\\/]+\\/datasets\\/[^\\n\\r\\/]+)$"
      },
      description = "Source BigQuery dataset.",
      helpText =
          "Dataplex asset name for the BigQuery dataset to tier data from. Format: projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset name> (Dataplex asset name) or projects/<name>/datasets/<dataset-id> (BigQuery dataset ID).")
  @Required
  String getSourceBigQueryDataset();

  void setSourceBigQueryDataset(String sourceBigQueryDataset);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      regexes = {"^[a-zA-Z0-9_-]+(,[a-zA-Z0-9_-]+)*$"},
      description = "Source BigQuery tables to tier.",
      helpText =
          "A comma-separated list of BigQuery tables to tier. If none specified, all tables will be tiered. Tables should be specified by their name only (no project/dataset prefix). Case-sensitive!")
  String getTables();

  void setTables(String tables);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      regexes = {
        "^projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/assets\\/[^\\n\\r\\/]+$"
      },
      description = "Dataplex asset name for the destination Cloud Storage bucket.",
      helpText =
          "Dataplex asset name for the Cloud Storage bucket to tier data to. Format: projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset name>.")
  @Required
  String getDestinationStorageBucketAssetName();

  void setDestinationStorageBucketAssetName(String destinationStorageBucketAssetName);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {
        "^([0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})?)?|-[pP]([0-9]+(\\.[0-9]+)?Y)?([0-9]+(\\.[0-9]+)?M)?([0-9]+(\\.[0-9]+)?W)?([0-9]+(\\.[0-9]+)?D)?(T([0-9]+(\\.[0-9]+)?H)?([0-9]+(\\.[0-9]+)?M)?([0-9]+(\\.[0-9]+)?S)?)?)$"
      },
      description = "Move data older than the date.",
      helpText =
          "Move data older than this date (and optional time). For partitioned tables, move partitions last modified before this date/time. For non-partitioned tables, move if the table was last modified before this date/time. If not specified, move all tables / partitions. The date/time is parsed in the default time zone by default, but optional suffixes Z and +HH:mm are supported. Format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+03:00. Relative date/time (https://en.wikipedia.org/wiki/ISO_8601#Durations) is also supported. Format: -PnDTnHnMn.nS (must start with -P meaning time in the past).")
  String getExportDataModifiedBeforeDateTime();

  void setExportDataModifiedBeforeDateTime(String exportDataModifiedBeforeDateTime);

  @TemplateParameter.Integer(
      order = 5,
      description = "Maximum number of parallel requests.",
      helpText =
          "The maximum number of parallel requests that will be sent to BigQuery when loading table/partition metadata.")
  @Default.Integer(5)
  @Required
  Integer getMaxParallelBigQueryMetadataRequests();

  void setMaxParallelBigQueryMetadataRequests(Integer maxParallelBigQueryMetadataRequests);

  @TemplateParameter.Enum(
      order = 6,
      enumOptions = {@TemplateEnumOption("AVRO"), @TemplateEnumOption("PARQUET")},
      optional = true,
      description = "Output file format in Cloud Storage.",
      helpText = "Output file format in Cloud Storage. Format: PARQUET or AVRO.")
  @Default.Enum("PARQUET")
  @Required
  FileFormatOptions getFileFormat();

  void setFileFormat(FileFormatOptions fileFormat);

  @TemplateParameter.Enum(
      order = 7,
      enumOptions = {
        @TemplateEnumOption("UNCOMPRESSED"),
        @TemplateEnumOption("SNAPPY"),
        @TemplateEnumOption("GZIP"),
        @TemplateEnumOption("BZIP2")
      },
      optional = true,
      description = "Output file compression in Cloud Storage.",
      helpText =
          "Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. BZIP2 not supported for PARQUET files.")
  @Default.Enum("SNAPPY")
  DataplexCompression getFileCompression();

  void setFileCompression(DataplexCompression fileCompression);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Partition ID regular expression filter.",
      helpText =
          "Process partitions with partition ID matching this regexp only. Default: process all.")
  String getPartitionIdRegExp();

  void setPartitionIdRegExp(String partitionIdRegExp);

  @TemplateParameter.Enum(
      order = 9,
      enumOptions = {
        @TemplateEnumOption("OVERWRITE"),
        @TemplateEnumOption("FAIL"),
        @TemplateEnumOption("SKIP")
      },
      optional = true,
      description = "Action that occurs if a destination file already exists.",
      helpText =
          "Specifies the action that occurs if a destination file already exists. Format: OVERWRITE, FAIL, SKIP. If SKIP, only files that don't exist in the destination directory will be processed. If FAIL and at least one file already exists, no data will be processed and an error will be produced.")
  @Default.Enum("SKIP")
  WriteDispositionOptions getWriteDisposition();

  void setWriteDisposition(WriteDispositionOptions writeDisposition);

  @TemplateParameter.Boolean(
      order = 10,
      optional = true,
      description = "Enforce same partition key.",
      helpText =
          "Whether to enforce the same partition key. Due to a BigQuery limitation, it's not possible to have a partitioned external table with the partition key (in the file path) to have the same name as one of the columns in the file. If this param is true (the default), the partition key of the target file will be set to the original partition column name and the column in the file will be renamed. If false, it's the partition key that will be renamed.")
  @Default.Boolean(true)
  Boolean getEnforceSamePartitionKey();

  void setEnforceSamePartitionKey(Boolean enforceSamePartitionKey);
}
