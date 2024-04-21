/*
 * Copyright (C) 2023 Google LLC
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
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

public interface BigtableChangeStreamsToGcsOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {

  @TemplateParameter.Enum(
      order = 1,
      groupName = "Target",
      enumOptions = {@TemplateEnumOption("TEXT"), @TemplateEnumOption("AVRO")},
      optional = true,
      description = "Output file format",
      helpText =
          "The format of the output Cloud Storage file. Allowed formats are TEXT, AVRO. Defaults to AVRO.")
  @Default.Enum("AVRO")
  FileFormat getOutputFileFormat();

  void setOutputFileFormat(FileFormat outputFileFormat);

  @TemplateParameter.Duration(
      order = 2,
      groupName = "Target",
      optional = true,
      description = "Window duration",
      helpText =
          "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
              + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "1h")
  @Default.String("1h")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      optional = true,
      description = "Bigtable Metadata Table Id",
      helpText = "Table ID used for creating the metadata table.")
  String getBigtableMetadataTableTableId();

  void setBigtableMetadataTableTableId(String bigtableMetadataTableTableId);

  @TemplateParameter.Enum(
      order = 4,
      groupName = "Target",
      enumOptions = {@TemplateEnumOption("CHANGELOG_ENTRY"), @TemplateEnumOption("BIGTABLE_ROW")},
      optional = true,
      description = "Output schema format",
      helpText =
          "Schema chosen for outputting data to GCS. CHANGELOG_ENTRY support TEXT and AVRO "
              + "output formats, BIGTABLE_ROW only supports AVRO output")
  @Default.Enum("CHANGELOG_ENTRY")
  BigtableSchemaFormat getSchemaOutputFormat();

  void setSchemaOutputFormat(BigtableSchemaFormat outputSchemaFormat);

  @TemplateParameter.GcsWriteFolder(
      order = 5,
      groupName = "Target",
      description = "Output file directory in Cloud Storage",
      helpText =
          "The path and filename prefix for writing output files. Must end with a slash. "
              + "DateTime formatting is used to parse directory path for date & time formatters.",
      example = "gs://your-bucket/your-path")
  @Validation.Required
  String getGcsOutputDirectory();

  void setGcsOutputDirectory(String gcsOutputDirectory);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Target",
      optional = true,
      description = "Output filename prefix of the files to write",
      helpText = "The prefix to place on each windowed file. Defaults to \"changelog-\"",
      example = "changelog-")
  @Default.String("changelog-")
  String getOutputFilenamePrefix();

  void setOutputFilenamePrefix(String outputFilenamePrefix);

  @TemplateParameter.Integer(
      order = 7,
      groupName = "Target",
      optional = true,
      description = "Number of output file shards",
      helpText =
          "The maximum number of output shards produced when writing to Cloud Storage. "
              + "A higher number of shards means higher throughput for writing to Cloud Storage, "
              + "but potentially higher data aggregation cost across shards when processing "
              + "output Cloud Storage files.")
  @Default.Integer(20)
  Integer getOutputShardsCount();

  void setOutputShardsCount(Integer shardCount);

  @TemplateParameter.Integer(
      order = 7,
      groupName = "Target",
      optional = true,
      description = "Maximum number of mutations in a batch",
      helpText =
          "Batching mutations reduces overhead and cost. Depending on the size of values "
              + "written to Cloud Bigtable the batch size might need to be adjusted lower to avoid "
              + "memory pressures on the worker fleet. Defaults to 10000")
  @Default.Integer(10000)
  Integer getOutputBatchSize();

  void setOutputBatchSize(Integer batchSize);

  @TemplateParameter.Boolean(
      order = 8,
      groupName = "Target",
      optional = true,
      description = "Write Base64-encoded rowkeys",
      helpText =
          "Only supported for the TEXT output file format. When set to true, rowkeys will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String rowkeys"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64Rowkeys();

  void setUseBase64Rowkeys(Boolean useBase64Rowkey);

  @TemplateParameter.Boolean(
      order = 9,
      groupName = "Target",
      optional = true,
      description = "Write Base64-encoded column qualifiers",
      helpText =
          "Only supported for the TEXT output file format. When set to true, column qualifiers will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String column qualifiers"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64ColumnQualifiers();

  void setUseBase64ColumnQualifiers(Boolean useBase64ColumnQualifier);

  @TemplateParameter.Boolean(
      order = 10,
      groupName = "Target",
      optional = true,
      description = "Write Base64-encoded value",
      helpText =
          "Only supported for the TEXT output file format. When set to true, values will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String values"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64Values();

  void setUseBase64Values(Boolean useBase64Value);
}
