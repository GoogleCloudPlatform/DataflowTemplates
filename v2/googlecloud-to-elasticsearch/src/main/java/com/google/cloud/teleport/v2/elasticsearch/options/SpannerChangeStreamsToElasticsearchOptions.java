/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link SpannerChangeStreamsToElasticsearchOptions} interface provides the custom execution
 * options passed by the executor at the command-line.
 */
public interface SpannerChangeStreamsToElasticsearchOptions
    extends DataflowPipelineOptions, ElasticsearchWriteOptions {
  @Description(
      "Project to read change streams from. The default for this parameter is the project where the"
          + " Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @Description("The Spanner instance ID that contains the change stream.")
  @Validation.Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String value);

  @Description("The Spanner database that contains the change stream.")
  @Validation.Required
  String getSpannerDatabase();

  void setSpannerDatabase(String value);

  @Description("The Spanner metadata instance ID that's used by the change stream connector.")
  @Validation.Required
  String getSpannerMetadataInstanceId();

  void setSpannerMetadataInstanceId(String value);

  @Description("The Spanner metadata database that's used by the change stream connector.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String value);

  @Description(
      "The Cloud Spanner change streams Connector metadata table name to use. If not provided, a"
          + " Cloud Spanner change streams Connector metadata table will automatically be created"
          + " during the pipeline flow.")
  String getSpannerMetadataTableName();

  void setSpannerMetadataTableName(String value);

  @Description("The name of the Spanner change stream.")
  @Validation.Required
  String getSpannerChangeStreamName();

  void setSpannerChangeStreamName(String value);

  @Description("Spanner host endpoint (only used for testing).")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @Description(
      "Priority for Spanner RPC invocations. Defaults to HIGH. Allowed priorites are LOW, MEDIUM,"
          + " HIGH.")
  @Default.Enum("HIGH")
  RpcPriority getRpcPriority();

  void setRpcPriority(RpcPriority rpcPriority);

  @Description(
      "The starting DateTime to use for reading change streams"
          + " (https://tools.ietf.org/html/rfc3339). Defaults to pipeline start time.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @Description(
      "The ending DateTime to use for reading change streams"
          + " (https://tools.ietf.org/html/rfc3339). The default value is \"max\", which represents"
          + " an infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);
}
