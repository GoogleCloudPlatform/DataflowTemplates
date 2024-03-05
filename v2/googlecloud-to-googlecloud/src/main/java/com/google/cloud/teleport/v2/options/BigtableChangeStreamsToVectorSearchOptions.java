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
      order = 9,
      description = "The byte size of the embeddings array. Can be 4 or 8.",
      helpText =
          "The byte size of each entry in the embeddings array. Use 4 for Float, and 8 for Double.")
  @Default.Integer(4)
  Integer getEmbeddingByteSize();

  @SuppressWarnings("unused")
  void setEmbeddingByteSize(Integer value);

  // TODO(meagar): Some options end with "Column", should we be consistent?
  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Allow restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as the `allow` restricts, with their alias. In the format cf:col->alias.")
  String getAllowRestrictsMappings();

  @SuppressWarnings("unused")
  void setAllowRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Deny restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as the `deny` restricts, with their alias. In the format cf:col->alias.")
  String getDenyRestrictsMappings();

  @SuppressWarnings("unused")
  void setDenyRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "Integer numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as integer `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getIntNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setIntNumericRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "Float numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as float (4 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getFloatNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setFloatNumericRestrictsMappings(String value);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "Double numeric restricts mappings",
      helpText =
          "The comma separated fully qualified column names of the columns that should be used as double (8 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.")
  String getDoubleNumericRestrictsMappings();

  @SuppressWarnings("unused")
  void setDoubleNumericRestrictsMappings(String value);

  @TemplateParameter.Duration(
      order = 15,
      optional = true,
      description = "Window duration",
      helpText =
          "The window duration/size in which data will be written to Vector Search. Allowed formats are: Ns (for "
              + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "1h")
  @Default.String("1h")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);
}
