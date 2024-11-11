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
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamToBigQueryOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamToBigQueryOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {

  @TemplateParameter.Text(
      order = 1,
      groupName = "Target",
      description = "BigQuery dataset",
      helpText = "The dataset name of the destination BigQuery table.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.Boolean(
      order = 2,
      optional = true,
      description = "Write rowkeys as BigQuery BYTES",
      helpText =
          "Whether to write rowkeys as BigQuery `BYTES`. When set to `true`, row keys are written to the `BYTES` "
              + "column. Otherwise, rowkeys are written to the `STRING` column. Defaults to `false`.")
  @Default.Boolean(false)
  Boolean getWriteRowkeyAsBytes();

  void setWriteRowkeyAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 3,
      optional = true,
      description = "Write values as BigQuery BYTES",
      helpText =
          "When set to `true`, values are written to a column of type BYTES, otherwise to a column of type STRING . "
              + "Defaults to: `false`.")
  @Default.Boolean(false)
  Boolean getWriteValuesAsBytes();

  void setWriteValuesAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Write Bigtable timestamp as BigQuery INT",
      helpText =
          "Whether to write the Bigtable timestamp as BigQuery INT64. When set to `true`, values are written to the INT64 column."
              + " Otherwise, values are written to the `TIMESTAMP` column. Columns affected: `timestamp`, `timestamp_from`, "
              + "and `timestamp_to`. Defaults to `false`. When set to `true`, the time is measured in microseconds "
              + "since the Unix epoch (January 1, 1970 at UTC).")
  @Default.Boolean(false)
  Boolean getWriteNumericTimestamps();

  void setWriteNumericTimestamps(Boolean value);

  @TemplateParameter.ProjectId(
      order = 5,
      groupName = "Target",
      optional = true,
      description = "BigQuery project ID",
      helpText = "The BigQuery dataset project ID. The default is the project for the Dataflow job")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "BigQuery changelog table name",
      helpText =
          "Destination BigQuery table name. If not specified, "
              + "the value `bigtableReadTableId + \"_changelog\"` is used")
  @Default.String("")
  String getBigQueryChangelogTableName();

  void setBigQueryChangelogTableName(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Changelog table will be partitioned at specified granularity",
      helpText =
          "Specifies a granularity for partitioning the changelog table. When set, the table is partitioned. "
              + "Use one of the following supported values: `HOUR`, `DAY`, `MONTH`, or `YEAR`. "
              + "By default, the table isn't partitioned.")
  @Default.String("")
  String getBigQueryChangelogTablePartitionGranularity();

  void setBigQueryChangelogTablePartitionGranularity(String value);

  @TemplateParameter.Long(
      order = 8,
      optional = true,
      description = "Sets partition expiration time in milliseconds",
      helpText =
          "Sets the changelog table partition expiration time, in milliseconds. When set to `true`, "
              + "partitions older than the specified number of milliseconds are deleted. "
              + "By default, no expiration is set.")
  Long getBigQueryChangelogTablePartitionExpirationMs();

  void setBigQueryChangelogTablePartitionExpirationMs(Long value);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Optional changelog table columns to be disabled",
      helpText =
          "A comma-separated list of the changelog columns that, when specified, aren't "
              + "created and populated. Use one of the following supported values: `is_gc`, "
              + "`source_instance`, `source_cluster`, `source_table`, `tiebreaker`, or `big_query_commit_timestamp`. "
              + "By default, all columns are populated.")
  String getBigQueryChangelogTableFieldsToIgnore();

  void setBigQueryChangelogTableFieldsToIgnore(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 10,
      optional = true,
      description = "Dead letter queue directory",
      helpText =
          "The directory to use for the dead-letter queue. Records that fail to be processed are stored in this directory. "
              + "The default is a directory under the Dataflow job's temp location. "
              + "In most cases, you can use the default path.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String value);
}
