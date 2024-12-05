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

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link SpannerChangeStreamsToBigQueryOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface SpannerChangeStreamsToBigQueryOptions
    extends DataflowPipelineOptions, BigQueryStorageApiStreamingOptions {

  @TemplateParameter.ProjectId(
      order = 1,
      groupName = "Source",
      optional = true,
      description = "Spanner Project ID",
      helpText =
          "The project to read change streams from. This value is also the project where the change streams connector metadata table is created."
              + " The default value for this parameter is the project where the Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      description = "Spanner instance ID",
      helpText = "The Spanner instance to read change streams from.")
  @Validation.Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String value);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      description = "Spanner database",
      helpText = "The Spanner database to read change streams from.")
  @Validation.Required
  String getSpannerDatabase();

  void setSpannerDatabase(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description = "Spanner database role",
      helpText =
          "The Spanner database role to use when running the template. This parameter is required only when the IAM principal who is running the template is a"
              + " fine-grained access control user. The database role must have the `SELECT` privilege on the change stream"
              + " and the `EXECUTE` privilege on the change stream's read function. For more information, see"
              + " Fine-grained access control for change streams (https://cloud.google.com/spanner/docs/fgac-change-streams).")
  String getSpannerDatabaseRole();

  void setSpannerDatabaseRole(String spannerDatabaseRole);

  @TemplateParameter.Text(
      order = 5,
      description = "Spanner metadata instance ID",
      helpText = "The Spanner instance to use for the change streams connector metadata table.")
  @Validation.Required
  String getSpannerMetadataInstanceId();

  void setSpannerMetadataInstanceId(String value);

  @TemplateParameter.Text(
      order = 6,
      description = "Spanner metadata database",
      helpText = "The Spanner database to use for the change streams connector metadata table.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Cloud Spanner metadata table name",
      helpText =
          "The Spanner change streams connector metadata table name to use. If not provided,"
              + " a Spanner change streams connector metadata table is automatically created during the pipeline flow."
              + " You must provide this parameter when updating an existing pipeline."
              + " Otherwise, don't provide this parameter.")
  String getSpannerMetadataTableName();

  void setSpannerMetadataTableName(String value);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Source",
      description = "Spanner change stream",
      helpText = "The name of the Spanner change stream to read from.")
  @Validation.Required
  String getSpannerChangeStreamName();

  void setSpannerChangeStreamName(String value);

  @TemplateParameter.Enum(
      order = 9,
      enumOptions = {
        @TemplateEnumOption("LOW"),
        @TemplateEnumOption("MEDIUM"),
        @TemplateEnumOption("HIGH")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The request priority for Spanner calls. The value must be one of the following values: `HIGH`, `MEDIUM`, or `LOW`. The default value is `HIGH`.")
  @Default.Enum("HIGH")
  RpcPriority getRpcPriority();

  void setRpcPriority(RpcPriority value);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
      example = "https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.DateTime(
      order = 11,
      optional = true,
      description = "The timestamp to read change streams from",
      helpText =
          "The starting DateTime (https://datatracker.ietf.org/doc/html/rfc3339), inclusive, to use for reading change streams."
              + " Ex-2021-10-12T07:20:50.52Z."
              + " Defaults to the timestamp when the pipeline starts, that is, the current time.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @TemplateParameter.DateTime(
      order = 12,
      optional = true,
      description = "The timestamp to read change streams to",
      helpText =
          "The ending DateTime (https://datatracker.ietf.org/doc/html/rfc3339), inclusive, to use for reading change streams."
              + "Ex-2021-10-12T07:20:50.52Z. Defaults to an"
              + " infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @TemplateParameter.Text(
      order = 13,
      groupName = "Target",
      description = "BigQuery dataset",
      helpText = "The BigQuery dataset for change streams output.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.ProjectId(
      order = 14,
      groupName = "Target",
      optional = true,
      description = "BigQuery project ID",
      helpText = "The BigQuery project. The default value is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @TemplateParameter.Text(
      order = 15,
      groupName = "Target",
      optional = true,
      description = "BigQuery table name Template",
      helpText = "The template for the name of the BigQuery table that contains the changelog.")
  @Default.String("{_metadata_spanner_table_name}_changelog")
  String getBigQueryChangelogTableNameTemplate();

  void setBigQueryChangelogTableNameTemplate(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 16,
      optional = true,
      description = "Dead letter queue directory",
      helpText =
          "The path to store any unprocessed records."
              + " The default path is a directory under the Dataflow job's temp location. The default value is usually sufficient.")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  @TemplateParameter.Integer(
      order = 17,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText =
          "The number of minutes between dead-letter queue retries. The default value is `10`.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  // TODO(haikuo-google): Test this in UIF test.
  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "Fields to be ignored",
      helpText =
          "A comma-separated list of fields (case sensitive) to ignore. These fields might be fields of watched tables, "
              + "or metadata fields added by the pipeline. Ignored fields aren't inserted into BigQuery."
              + " When you ignore the _metadata_spanner_table_name field,"
              + " the bigQueryChangelogTableNameTemplate parameter is also ignored.")
  @Default.String("")
  String getIgnoreFields();

  void setIgnoreFields(String value);

  @TemplateParameter.Boolean(
      order = 19,
      optional = true,
      description = "Whether or not to disable retries for the DLQ",
      helpText = "Whether or not to disable retries for the DLQ")
  @Default.Boolean(false)
  Boolean getDisableDlqRetries();

  void setDisableDlqRetries(Boolean value);
}
