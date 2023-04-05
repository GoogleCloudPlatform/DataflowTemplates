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
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamsToBigQueryOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToBigQueryOptions
    extends DataflowPipelineOptions, BigtableCommonOptions.ReadChangeStreamsOptions {

  @TemplateParameter.Text(
      order = 1,
      description = "BigQuery dataset",
      helpText = "The BigQuery dataset for change streams output.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      description = "Cloud Bigtable column families to ignore",
      helpText = "A comma-separated list of column family names changes to which won't be captured")
  @Default.String("")
  String getIgnoreColumnFamilies();

  void setIgnoreColumnFamilies(String value);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "Cloud Bigtable columns to ignore",
      helpText = "A comma-separated list of column names changes to which won't be captured")
  @Default.String("")
  String getIgnoreColumns();

  void setIgnoreColumns(String value);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Write rowkeys as BigQuery BYTES",
      helpText =
          "When set true rowkeys are written to BYTES column, otherwise to STRING column. "
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getWriteRowkeyAsBytes();

  void setWriteRowkeyAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 5,
      optional = true,
      description = "Write values as BigQuery BYTES",
      helpText =
          "When set true values are written to BYTES column, otherwise to STRING column. "
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getWriteValuesAsBytes();

  void setWriteValuesAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 6,
      optional = true,
      description = "Write Bigtable timestamp as BigQuery INT",
      helpText =
          "When set true values are written to INT column, otherwise to TIMESTAMP column. "
              + "Columns affected: `timestamp`, `timestamp_from`, `timestamp_to`. "
              + "Defaults to false. When set to true the value is a number of microseconds "
              + "since midnight of 01-JAN-1970")
  @Default.Boolean(false)
  Boolean getWriteNumericTimestamps();

  void setWriteNumericTimestamps(Boolean value);

  @TemplateParameter.ProjectId(
      order = 7,
      optional = true,
      description = "BigQuery project ID",
      helpText = "The BigQuery Project. Default is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "BigQuery changelog table name",
      helpText =
          "The BigQuery table name that contains the changelog records."
              + " Default: {bigtableTableId}_changelog")
  @Default.String("")
  String getBigQueryChangelogTableName();

  void setBigQueryChangelogTableName(String value);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Changelog table will be partitioned at specified granularity",
      helpText =
          "When set, table partitioning will be in effect. Accepted values: `HOUR`, "
              + "`DAY`, `MONTH`, `YEAR`. Default is no partitioning.")
  @Default.String("")
  String getBigQueryChangelogTablePartitionGranularity();

  void setBigQueryChangelogTablePartitionGranularity(String value);

  @TemplateParameter.Long(
      order = 10,
      optional = true,
      description = "Sets partition expiration time in milliseconds",
      helpText =
          "When set true partitions older than specified number of milliseconds will be "
              + "deleted. Default is no expiration.")
  Long getBigQueryChangelogTablePartitionExpirationMs();

  void setBigQueryChangelogTablePartitionExpirationMs(Long value);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Optional changelog table columns to be disabled",
      helpText =
          "A comma-separated list of the changelog columns which will not be created and "
              + "populated if specified. Supported values should be from the following list: `is_gc`, "
              + "`source_instance`, `source_cluster`, `source_table`, `tiebreaker`, "
              + "`big_query_commit_timestamp`. Defaults to all columns to be populated")
  String getBigQueryChangelogTableFieldsToIgnore();

  void setBigQueryChangelogTableFieldsToIgnore(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 12,
      optional = true,
      description = "Dead letter queue directory",
      helpText =
          "The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String value);

  @TemplateParameter.Integer(
      order = 13,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  @TemplateParameter.Integer(
      order = 14,
      optional = true,
      description = "Dead letter maximum retries",
      helpText = "The number of attempts to process change stream mutations. Defaults to 5.")
  @Default.Integer(5)
  Integer getDlqMaxRetries();

  void setDlqMaxRetries(Integer value);
}
