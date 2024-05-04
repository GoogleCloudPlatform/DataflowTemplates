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
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

/**
 * The {@link BigtableChangeStreamsToVectorSearchOptions} class provides the custom execution
 * options passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToVectorSearchOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      description = "Bigtable Metadata Table Id",
      helpText = "Table ID used for creating the metadata table.")
  String getBigtableMetadataTableTableId();

  void setBigtableMetadataTableTableId(String bigtableMetadataTableTableId);

  @TemplateParameter.Text(
      order = 2,
      description = "Embedding column",
      helpText =
          "The fully qualified column name where the embeddings are stored. In the format cf:col.")
  String getEmbeddingColumn();

  @SuppressWarnings("unused")
  void setEmbeddingColumn(String value);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "Crowding tag column",
      helpText =
          "The fully qualified column name where the crowding tag is stored. In the format cf:col.")
  String getCrowdingTagColumn();

  @SuppressWarnings("unused")
  void setCrowdingTagColumn(String value);

  @TemplateParameter.Integer(
      order = 4,
      description = "The byte size of the embeddings array. Can be 4 or 8.",
      helpText =
          "The byte size of each entry in the embeddings array. Use 4 for Float, and 8 for Double.")
  @Default.Integer(4)
  Integer getEmbeddingByteSize();

  @SuppressWarnings("unused")
  void setEmbeddingByteSize(Integer value);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      description = "Allow restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as the `allow` restricts, with their alias. In the format cf:col->alias.")
  String getAllowRestrictsMappings();

  @SuppressWarnings("unused")
  void setAllowRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "Deny restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as the `deny` restricts, with their alias. In the format cf:col->alias.")
  String getDenyRestrictsMappings();

  @SuppressWarnings("unused")
  void setDenyRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Integer numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as integer `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getIntNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setIntNumericRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Float numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as float (4 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getFloatNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setFloatNumericRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Double numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as double (8 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getDoubleNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setDoubleNumericRestrictsMappings(String value);

  @TemplateParameter.Integer(
      order = 10,
      optional = true,
      description = "Maximum batch size for upserts for Vector Search",
      helpText =
          "The maximum number of upserts to buffer before upserting the batch to the Vector Search Index. "
              + "Batches will be sent when there are either upsertBatchSize records ready, or any record has been "
              + "waiting upsertBatchDelay time has passed.",
      example = "10")
  @Default.Integer(10)
  int getUpsertMaxBatchSize();

  @SuppressWarnings("unused")
  void setUpsertMaxBatchSize(int batchSize);

  @TemplateParameter.Duration(
      order = 11,
      optional = true,
      description =
          "Maximum duration an upsert can wait in a buffer before its batch is submitted, regardless of batch size",
      helpText =
          "The maximum delay before a batch of upserts is sent to Vector Search."
              + "Batches will be sent when there are either upsertBatchSize records ready, or any record has been "
              + "waiting upsertBatchDelay time has passed. "
              + "Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "10s")
  @Default.String("10s")
  String getUpsertMaxBufferDuration();

  @SuppressWarnings("unused")
  void setUpsertMaxBufferDuration(String maxBufferDuration);

  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      description = "Maximum batch size for deletes for Vector Search",
      helpText =
          "The maximum number of deletes to buffer before deleting the batch from the Vector Search Index. "
              + "Batches will be sent when there are either deleteBatchSize records ready, or any record has been "
              + "waiting deleteBatchDelay time has passed.",
      example = "10")
  @Default.Integer(10)
  int getDeleteMaxBatchSize();

  @SuppressWarnings("unused")
  void setDeleteMaxBatchSize(int batchSize);

  @TemplateParameter.Duration(
      order = 13,
      optional = true,
      description =
          "Maximum duration a delete can wait in a buffer before its batch is submitted, regardless of batch size",
      helpText =
          "The maximum delay before a batch of deletes is sent to Vector Search."
              + "Batches will be sent when there are either deleteBatchSize records ready, or any record has been "
              + "waiting deleteBatchDelay time has passed. "
              + "Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "10s")
  @Default.String("10s")
  String getDeleteMaxBufferDuration();

  @SuppressWarnings("unused")
  void setDeleteMaxBufferDuration(String maxBufferDuration);

  @TemplateParameter.Text(
      order = 14,
      optional = false,
      description = "Vector Search Index Path",
      helpText =
          "The Vector Search Index where changes will be streamed, in the format 'projects/{projectID}/locations/{region}/indexes/{indexID}' (no leading or trailing spaces)",
      example = "projects/123/locations/us-east1/indexes/456")
  String getVectorSearchIndex();

  @SuppressWarnings("unused")
  void setVectorSearchIndex(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 15,
      optional = true,
      description = "Dead letter queue directory to store any unpublished change record.",
      helpText =
          "The path to store any unprocessed records with"
              + " the reason they failed to be processed. "
              + "Default is a directory under the Dataflow job's temp location. "
              + "The default value is enough under most conditions.")
  @Default.String("")
  String getDlqDirectory();

  @SuppressWarnings("unused")
  void setDlqDirectory(String value);
}
