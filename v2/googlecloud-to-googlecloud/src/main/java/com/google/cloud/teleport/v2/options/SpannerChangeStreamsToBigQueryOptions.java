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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link SpannerChangeStreamsToBigQueryOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface SpannerChangeStreamsToBigQueryOptions extends DataflowPipelineOptions {

  @Description("The Spanner instance ID that contains the Change Stream.")
  @Validation.Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String value);

  @Description("The Spanner database ID that contains the Change Stream.")
  @Validation.Required
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String value);

  @Description("The Spanner metadata instance ID that's used by the Change Stream connector.")
  @Validation.Required
  String getSpannerMetadataInstanceId();

  void setSpannerMetadataInstanceId(String value);

  @Description("The Spanner metadata database ID that's used by the Change Stream connector.")
  @Validation.Required
  String getSpannerMetadataDatabaseId();

  void setSpannerMetadataDatabaseId(String value);

  @Description("The name of the Spanner Change Stream.")
  @Validation.Required
  String getSpannerChangeStream();

  void setSpannerChangeStream(String value);

  @Description(
      "Priority for Spanner RPC invocations. Defaults to HIGH. Allowed priorites are LOW, MEDIUM,"
          + " HIGH.")
  @Default.Enum("HIGH")
  RpcPriority getSpannerRpcPriority();

  void setSpannerRpcPriority(RpcPriority value);

  @Description("Spanner host endpoint (only used for testing).")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @Description(
      "The starting DateTime to use for reading Change Streams"
          + " (https://tools.ietf.org/html/rfc3339). Defaults to pipeline start time.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @Description(
      "The ending DateTime to use for reading Change Streams"
          + " (https://tools.ietf.org/html/rfc3339). The default value is \"max\", which represents"
          + " an infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @Description("The output BigQuery dataset.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @Description("The BigQuery Project ID. Default is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @Description("The changelog BigQuery table name Template")
  @Default.String("{_metadata_spanner_table_name}_changelog")
  String getBigQueryChangelogTableNameTemplate();

  void setBigQueryChangelogTableNameTemplate(String value);

  @Description("The Dead Letter Queue GCS Prefix to use for errored data")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String value);

  @Description("The number of minutes between deadletter queue retries")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  // TODO(haikuo-google): Implement this functionality.
  @Description("Comma Separated list of metadata fields to be ignored")
  @Default.String("")
  String getIgnoreMetadataFields();

  void setIgnoreMetadataFields(String value);
}
