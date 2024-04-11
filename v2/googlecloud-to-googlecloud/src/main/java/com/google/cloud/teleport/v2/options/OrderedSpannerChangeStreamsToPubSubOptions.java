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
 * The {@link OrderedSpannerChangeStreamsToPubSubOptions} interface provides the custom execution
 * options passed by the executor at the command-line.
 */
public interface OrderedSpannerChangeStreamsToPubSubOptions extends DataflowPipelineOptions {

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

  void setSpannerInstanceId(String spannerInstanceId);

  @TemplateParameter.Text(
      order = 3,
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

  void setSpannerMetadataInstanceId(String spannerMetadataInstanceId);

  @TemplateParameter.Text(
      order = 6,
      description = "Spanner metadata database",
      helpText =
          "The Spanner database to use for the change streams connector metadata table. For change"
              + " streams tracking all tables in a database, we recommend putting the metadata"
              + " table in a separate database.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String spannerMetadataDatabase);

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

  void setSpannerChangeStreamName(String spannerChangeStreamName);

  @TemplateParameter.DateTime(
      order = 9,
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
      order = 10,
      optional = true,
      description = "The timestamp to read change streams to",
      helpText =
          "The ending DateTime, inclusive, to use for reading change streams"
              + " (https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an"
              + " infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @TemplateParameter.Enum(
      order = 11,
      enumOptions = {@TemplateEnumOption("PRIMARY_KEY")},
      optional = true,
      description =
          "Partition/Grouping key within which records will be ordered by commit timestamp",
      helpText =
          "Same key will also be used as ordering key for pub/sub publisher. Only PRIMARY_KEY supported as of now")
  @Default.String("PRIMARY_KEY")
  String getOrderingPartitionKey();

  void setOrderingPartitionKey(String orderingPartitionKey);

  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      description = "Maximum number of partition buckets to create for ordering",
      helpText =
          "This values is used to have a deterministic number of states and timers for performance purposes."
              + " Note that having too many buckets might have undesirable effects if it results in a"
              + " low number of records per bucket. On the other hand, having too few buckets might"
              + " also be problematic, since many records will be contained within them."
              + " Default value is 1000")
  @Default.Integer(1000)
  Integer getOrderingPartitionBucketCount();

  void setOrderingPartitionBucketCount(Integer orderingPartitionBucketCount);

  @TemplateParameter.Integer(
      order = 13,
      optional = true,
      description =
          "Duration in seconds between calls to stateful timer processing which sorts and flushes the buffer.",
      helpText =
          "This interval is used to set the expiration time of the timer to time T"
              + "in the future (commitTimestamp + bufferTimerInterval)."
              + " When the Dataflow watermark passes time T,"
              + " all records will flushed from the buffer with timestamp less than T,"
              + " orders these records by commit timestamp, and outputs a key-value pair where."
              + " Default value is 6 (seconds)")
  @Default.Integer(6)
  Integer getBufferTimerInterval();

  void setBufferTimerInterval(Integer bufferTimerInterval);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
      example = "https://spanner.googleapis.com")
  @Default.String("https://spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.Enum(
      order = 15,
      enumOptions = {@TemplateEnumOption("JSON")},
      optional = true,
      description = "Output data format",
      helpText = "The format of the output to Pub/Sub. Only JSON supported as of now")
  @Default.String("JSON")
  String getOutputDataFormat();

  void setOutputDataFormat(String outputDataFormat);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      description = "Pub/Sub API",
      helpText =
          "Pub/Sub API used to implement the pipeline. Only native_client API is supported as of now.")
  @Default.String("native_client")
  String getPubsubAPI();

  void setPubsubAPI(String pubsubAPI);

  @TemplateParameter.ProjectId(
      order = 17,
      optional = true,
      description = "Pub/Sub Project ID",
      helpText =
          "Project of Pub/Sub topic. The default for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getPubsubProjectId();

  void setPubsubProjectId(String pubsubProjectId);

  @TemplateParameter.Text(
      order = 18,
      description = "The output Pub/Sub Topic ID",
      helpText = "The Pub/Sub Topic ID to publish PubsubMessage.",
      example = "spanner-change-records-topic")
  @Validation.Required
  String getPubsubTopic();

  void setPubsubTopic(String pubsubTopic);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      description = "The Pub/Sub regional/locational endpoint",
      helpText =
          "Check list of endpoints at https://cloud.google.com/pubsub/docs/reference/service_apis_overview#pubsub_endpoints (example: us-central1-pubsub.googleapis.com:443)\n"
              + " Message ordering is ensured for messages published in the same region."
              + " Publishers must use the locational service endpoints to publish messages to the same region for the same order key.\n"
              + " By default, the global endpoint (pubsub.googleapis.com:443) is used. Requests to the global endpoint that originate from within Google Cloud are routed to the Pub/Sub service in the region of origin.")
  @Default.String("pubsub.googleapis.com:443")
  String getPubsubRegionalEndpoint();

  void setPubsubRegionalEndpoint(String pubsubRegionalEndpoint);

  @TemplateParameter.Enum(
      order = 20,
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

  void setRpcPriority(RpcPriority rpcPriority);
}
