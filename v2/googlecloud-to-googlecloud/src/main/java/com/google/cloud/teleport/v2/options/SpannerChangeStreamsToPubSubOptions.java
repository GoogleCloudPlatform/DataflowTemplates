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
 * The {@link SpannerChangeStreamsToPubSubOptions} interface provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface SpannerChangeStreamsToPubSubOptions extends DataflowPipelineOptions {

  @TemplateParameter.ProjectId(
      order = 1,
      groupName = "Source",
      optional = true,
      description = "Spanner Project ID",
      helpText =
          "The project to read change streams from. This project is also where the change "
              + "streams connector metadata table is created. The default for this parameter is the project where the Dataflow pipeline is running.")
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

  void setSpannerInstanceId(String spannerInstanceId);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      description = "Spanner database",
      helpText = "The Spanner database to read change streams from.")
  @Validation.Required
  String getSpannerDatabase();

  void setSpannerDatabase(String spannerDatabase);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description = "Spanner database role",
      helpText =
          "The Spanner database role to use when running the template. This parameter is required"
              + " only when the IAM principal who is running the template is a fine-grained access control user. The"
              + " database role must have the `SELECT` privilege on the change stream and the `EXECUTE` privilege on"
              + " the change stream's read function. For more information, see Fine-grained access control for change streams (https://cloud.google.com/spanner/docs/fgac-change-streams).")
  String getSpannerDatabaseRole();

  void setSpannerDatabaseRole(String spannerDatabaseRole);

  @TemplateParameter.Text(
      order = 5,
      description = "Spanner metadata instance ID",
      helpText = "The Spanner instance to use for the change streams connector metadata table.")
  @Validation.Required
  String getSpannerMetadataInstanceId();

  void setSpannerMetadataInstanceId(String spannerMetadataInstanceId);

  @TemplateParameter.Text(
      order = 6,
      description = "Spanner metadata database",
      helpText = "The Spanner database to use for the change streams connector metadata table.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String spannerMetadataDatabase);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Cloud Spanner metadata table name",
      helpText =
          "The Spanner change streams connector metadata table name to use. If not provided,"
              + " Spanner automatically creates the streams connector metadata table during the pipeline flow"
              + " change. You must provide this parameter when updating an existing pipeline. Don't use this"
              + " parameter for other cases.")
  String getSpannerMetadataTableName();

  void setSpannerMetadataTableName(String value);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Source",
      description = "Spanner change stream",
      helpText = "The name of the Spanner change stream to read from.")
  @Validation.Required
  String getSpannerChangeStreamName();

  void setSpannerChangeStreamName(String spannerChangeStreamName);

  @TemplateParameter.DateTime(
      order = 9,
      optional = true,
      description = "The timestamp to read change streams from",
      helpText =
          "The starting DateTime (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, ex-"
              + " 2021-10-12T07:20:50.52Z. Defaults to the timestamp when the pipeline starts, that is, the current"
              + " time.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @TemplateParameter.DateTime(
      order = 10,
      optional = true,
      description = "The timestamp to read change streams to",
      helpText =
          "The ending DateTime (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, ex-"
              + " 2021-10-12T07:20:50.52Z. Defaults to an infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
      example = "https://spanner.googleapis.com")
  @Default.String("https://spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.Enum(
      order = 12,
      enumOptions = {@TemplateEnumOption("JSON"), @TemplateEnumOption("AVRO")},
      optional = true,
      description = "Output data format",
      helpText =
          "The format of the output. Output is wrapped in many PubsubMessages and sent to a Pub/Sub topic. Allowed formats are JSON and AVRO. Default is JSON.")
  @Default.String("JSON")
  String getOutputDataFormat();

  void setOutputDataFormat(String outputDataFormat);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "Pub/Sub API",
      helpText =
          "The Pub/Sub API used to implement the pipeline. Allowed APIs are `pubsubio` and `native_client`."
              + " For a small number of queries per second (QPS), `native_client` has less latency. For a"
              + " large number of QPS, `pubsubio` provides better and more stable performance. The default is `pubsubio`.")
  @Default.String("pubsubio")
  String getPubsubAPI();

  void setPubsubAPI(String pubsubAPI);

  @TemplateParameter.ProjectId(
      order = 14,
      groupName = "Target",
      optional = true,
      description = "Pub/Sub Project ID",
      helpText =
          "Project of Pub/Sub topic. The default for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getPubsubProjectId();

  void setPubsubProjectId(String pubsubProjectId);

  @TemplateParameter.Text(
      order = 15,
      groupName = "Target",
      description = "The output Pub/Sub topic",
      helpText = "The Pub/Sub topic for change streams output.")
  @Validation.Required
  String getPubsubTopic();

  void setPubsubTopic(String pubsubTopic);

  @TemplateParameter.Enum(
      order = 16,
      enumOptions = {
        @TemplateEnumOption("LOW"),
        @TemplateEnumOption("MEDIUM"),
        @TemplateEnumOption("HIGH")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The request priority for Spanner calls. Allowed values are HIGH, MEDIUM, and LOW. Defaults to: HIGH)")
  @Default.Enum("HIGH")
  RpcPriority getRpcPriority();

  void setRpcPriority(RpcPriority rpcPriority);
}
