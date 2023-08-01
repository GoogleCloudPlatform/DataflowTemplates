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
      optional = true,
      description = "Spanner Project ID",
      helpText =
          "Project to read change streams from. The default for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @TemplateParameter.Text(
      order = 2,
      description = "Spanner instance ID",
      helpText = "The Spanner instance to read change streams from.")
  @Validation.Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String value);

  @TemplateParameter.Text(
      order = 3,
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
          "Database role user assumes while reading from the change stream. The database role"
              + " should have required privileges to read from change stream. If a database role is"
              + " not specified, the user should have required IAM permissions to read from the"
              + " database.")
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
      helpText =
          "The Spanner database to use for the change streams connector metadata table. For change"
              + " streams tracking all tables in a database, we recommend putting the metadata"
              + " table in a separate database.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Cloud Spanner metadata table name",
      helpText =
          "The Cloud Spanner change streams connector metadata table name to use. If not provided,"
              + " a Cloud Spanner change streams connector metadata table will automatically be"
              + " created during the pipeline flow. This parameter must be provided when updating"
              + " an existing pipeline and should not be provided otherwise.")
  String getSpannerMetadataTableName();

  void setSpannerMetadataTableName(String value);

  @TemplateParameter.Text(
      order = 8,
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
          "The request priority for Cloud Spanner calls. The value must be one of:"
              + " [HIGH,MEDIUM,LOW].")
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
          "The starting DateTime, inclusive, to use for reading change streams"
              + " (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z."
              + " Defaults to the timestamp when the pipeline starts.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @TemplateParameter.DateTime(
      order = 12,
      optional = true,
      description = "The timestamp to read change streams to",
      helpText =
          "The ending DateTime, inclusive, to use for reading change streams"
              + " (https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an"
              + " infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @TemplateParameter.Text(
      order = 13,
      description = "BigQuery dataset",
      helpText = "The BigQuery dataset for change streams output.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.ProjectId(
      order = 14,
      optional = true,
      description = "BigQuery project ID",
      helpText = "The BigQuery Project. Default is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      description = "BigQuery table name Template",
      helpText = "The Template for the BigQuery table name that contains the change log")
  @Default.String("{_metadata_spanner_table_name}_changelog")
  String getBigQueryChangelogTableNameTemplate();

  void setBigQueryChangelogTableNameTemplate(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 16,
      optional = true,
      description = "Dead letter queue directory",
      helpText =
          "The file path to store any unprocessed records with the reason they failed to be"
              + " processed. Default is a directory under the Dataflow job's temp location. The"
              + " default value is enough under most conditions.")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  @TemplateParameter.Integer(
      order = 17,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  // TODO(haikuo-google): Test this in UIF test.
  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "Fields to be ignored",
      helpText =
          "Comma separated list of fields to be ignored, these could be fields of tracked tables, "
              + "or metadata fields which are _metadata_spanner_mod_type, "
              + "_metadata_spanner_table_name, "
              + "_metadata_spanner_commit_timestamp, "
              + "_metadata_spanner_server_transaction_id, "
              + "_metadata_spanner_record_sequence, "
              + "_metadata_spanner_is_last_record_in_transaction_in_partition, "
              + "_metadata_spanner_number_of_records_in_transaction, "
              + "_metadata_spanner_number_of_partitions_in_transaction, "
              + "_metadata_big_query_commit_timestamp")
  @Default.String("")
  String getIgnoreFields();

  void setIgnoreFields(String value);

  @TemplateParameter.Boolean(
      order = 19,
      optional = true,
      description = "Whether or not to disable retries for the DLQ",
      helpText = "Whether or not to disable retries for the DLQ")
  @Default.Boolean(false)
  boolean getDisableDlqRetries();

  void setDisableDlqRetries(boolean value);
}
