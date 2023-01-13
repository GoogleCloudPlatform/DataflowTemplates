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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamsToBigQueryOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToBigQueryOptions extends DataflowPipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      description = "Cloud Bigtable instance ID",
      helpText = "The Cloud Bigtable instance to read change streams from.")
  @Validation.Required
  String getBigtableInstanceId();

  void setBigtableInstanceId(String value);

  @TemplateParameter.Text(
      order = 2,
      description = "Cloud Bigtable table ID",
      helpText = "The Cloud Bigtable table to read change streams from.")
  @Validation.Required
  String getBigtableTableId();

  void setBigtableTableId(String value);

  @TemplateParameter.Text(
      order = 3,
      description = "BigQuery dataset",
      helpText = "The BigQuery dataset for change streams output.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.Text(
      order = 4,
      description = "Cloud Bigtable application profile ID",
      helpText = "The application profile is used to distinguish workload in Cloud Bigtable")
  @Validation.Required
  String getBigtableAppProfileId();

  void setBigtableAppProfileId(String value);

  @TemplateParameter.ProjectId(
      order = 5,
      optional = true,
      description = "Cloud Bigtable Project ID",
      helpText =
          "Project to read change streams from. The default for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getBigtableProjectId();

  void setBigtableProjectId(String projectId);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "Cloud Bigtable metadata instance ID",
      helpText = "The Cloud Bigtable instance to use for the change streams connector metadata table.")
  @Default.String("")
  String getBigtableMetadataInstanceId();

  void setBigtableMetadataInstanceId(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Cloud Bigtable metadata table ID",
      helpText =
          "The Cloud Bigtable change streams connector metadata table ID to use. If not "
              + "provided, a Cloud Bigtable change streams connector metadata table will automatically be "
              + "created during the pipeline flow. This parameter must be provided when updating an "
              + "existing pipeline and should not be provided otherwise.")
  @Default.String("__change_stream_md_table")
  String getBigtableMetadataTableTableId();

  void setBigtableMetadataTableTableId(String value);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Cloud Bigtable column families to ignore",
      helpText = "A comma-separated list of column family names changes to which won't be captured")
  @Default.String("")
  String getIgnoreColumnFamilies();

  void setIgnoreColumnFamilies(String value);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Cloud Bigtable columns to ignore",
      helpText = "A comma-separated list of column names changes to which won't be captured")
  @Default.String("")
  String getIgnoreColumns();

  void setIgnoreColumns(String value);

  @TemplateParameter.DateTime(
      order = 10,
      optional = true,
      description = "The timestamp to read change streams from",
      helpText =
          "The starting DateTime, inclusive, to use for reading change streams "
              + "(https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the "
              + "timestamp when the pipeline starts.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @TemplateParameter.DateTime(
      order = 11,
      optional = true,
      description = "The timestamp to read change streams to",
      helpText =
          "The ending DateTime, inclusive, to use for reading change streams "
              + "(https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an infinite "
              + "time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @TemplateParameter.Boolean(
      order = 12,
      optional = true,
      description = "Write values as BigQuery BYTES",
      helpText = "When set true rowkeys are written to BYTES column, otherwise to STRING column. " +
          "Defaults to false.")
  @Default.Boolean(false)
  Boolean getWriteRowkeyAsBytes();

  void setWriteRowkeyAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 13,
      optional = true,
      description = "Write values as BigQuery BYTES",
      helpText = "When set true values are written to BYTES column, otherwise to STRING column. " +
          "Defaults to false.")
  @Default.Boolean(false)
  Boolean getWriteValuesAsBytes();

  void setWriteValuesAsBytes(Boolean value);

  @TemplateParameter.Boolean(
      order = 14,
      optional = true,
      description = "Write Bigtable timestamp as BigQuery INT",
      helpText = "When set true values are written to INT column, otherwise to TIMESTAMP column. " +
          "Columns affected: `timestamp`, `timestamp_from`, `timestamp_to`. " +
          "Defaults to false.")
  @Default.Boolean(false)
  Boolean getWriteNumericTimestamps();

  void setWriteNumericTimestamps(Boolean value);

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      description = "BigQuery charset name when reading values and column qualifiers",
      helpText = "BigQuery charset name when reading values and column qualifiers. " +
          "Default is UTF-8")
  @Default.String("UTF-8")
  String getBigtableCharset();

  void setBigtableCharset(String value);

  @TemplateParameter.ProjectId(
      order = 16,
      optional = true,
      description = "BigQuery project ID",
      helpText = "The BigQuery Project. Default is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "BigQuery table name",
      helpText = "The BigQuery table name that contains the change log. Default: {bigtableTableId}_changelog")
  @Default.String("")
  String getBigQueryChangelogTableName();

  void setBigQueryChangelogTableName(String value);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "Changelog table will be partitioned at specified granularity",
      helpText = "When set, table partitioning will be in effect. Accepted values: `HOUR`, " +
          "`DAY`, `MONTH`, `YEAR`. Default is no partitioning.")
  @Default.String("")
  String getBigQueryChangelogTablePartitionGranularity();

  void setBigQueryChangelogTablePartitionGranularity(String value);

  @TemplateParameter.Long(
      order = 19,
      optional = true,
      description = "Sets partition expiration time in milliseconds",
      helpText = "When set true partitions older than specified number of milliseconds will be " +
           "deleted. Default is no expiration.")
  Long getBigQueryChangelogTablePartitionExpirationMs();

  void setBigQueryChangelogTablePartitionExpirationMs(Long value);

  @TemplateParameter.Text(
      order = 20,
      optional = true,
      description = "Optional changelog table columns to be disabled",
      helpText = "A comma-separated list of the changelog columns which will not be created and " +
          "populated if specified. Supported values should be from the following list: `is_gc`, " +
          "`source_instance`, `source_cluster`, `source_table`, `tiebreaker`, " +
          "`big_query_commit_timestamp`. Defaults to all columns to be populated")
  String getBigQueryChangelogTableFieldsToIgnore();

  void setBigQueryChangelogTableFieldsToIgnore(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 21,
      optional = true,
      description = "Dead letter queue directory",
      helpText =
          "The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String value);

  @TemplateParameter.Integer(
      order = 22,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

}
