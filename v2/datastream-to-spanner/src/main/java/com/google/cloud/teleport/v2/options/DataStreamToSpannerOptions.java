/*
 * Copyright (C) 2026 Google LLC
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
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/**
 * Options supported by the pipeline.
 *
 * <p>Inherits standard configuration options.
 */
public interface DataStreamToSpannerOptions
    extends PipelineOptions, StreamingOptions, DataflowPipelineWorkerPoolOptions {
  @TemplateParameter.GcsReadFile(
      order = 1,
      groupName = "Source",
      optional = true,
      description =
          "File location for Datastream file output in Cloud Storage. Support for this feature has been disabled.",
      helpText =
          "The Cloud Storage file location that contains the Datastream files to replicate. Typically, "
              + "this is the root path for a stream. Support for this feature has been disabled."
              + " Please use this feature only for retrying entries that land in severe DLQ.")
  String getInputFilePattern();

  void setInputFilePattern(String value);

  @TemplateParameter.Enum(
      order = 2,
      enumOptions = {@TemplateEnumOption("avro"), @TemplateEnumOption("json")},
      optional = true,
      description = "Datastream output file format (avro/json).",
      helpText =
          "The format of the output file produced by Datastream. For example `avro,json`. Defaults to `avro`.")
  @Default.String("avro")
  String getInputFileFormat();

  void setInputFileFormat(String value);

  @TemplateParameter.GcsReadFile(
      order = 3,
      optional = true,
      description = "Session File Path in Cloud Storage",
      helpText =
          "Session file path in Cloud Storage that contains mapping information from"
              + " HarbourBridge")
  String getSessionFilePath();

  void setSessionFilePath(String value);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Target",
      description = "Cloud Spanner Instance Id.",
      helpText = "The Spanner instance where the changes are replicated.")
  String getInstanceId();

  void setInstanceId(String value);

  @TemplateParameter.Text(
      order = 5,
      groupName = "Target",
      description = "Cloud Spanner Database Id.",
      helpText = "The Spanner database where the changes are replicated.")
  String getDatabaseId();

  void setDatabaseId(String value);

  @TemplateParameter.ProjectId(
      order = 6,
      groupName = "Target",
      optional = true,
      description = "Cloud Spanner Project Id.",
      helpText = "The Spanner project ID.")
  String getProjectId();

  void setProjectId(String projectId);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Target",
      optional = true,
      description = "The Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template.",
      example = "https://batch-spanner.googleapis.com")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.PubsubSubscription(
      order = 8,
      optional = true,
      description = "The Pub/Sub subscription being used in a Cloud Storage notification policy.",
      helpText =
          "The Pub/Sub subscription being used in a Cloud Storage notification policy. For the name,"
              + " use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.")
  String getGcsPubSubSubscription();

  void setGcsPubSubSubscription(String value);

  @TemplateParameter.Text(
      order = 9,
      groupName = "Source",
      optional = true,
      description = "Datastream stream name.",
      helpText =
          "The name or template for the stream to poll for schema information and source type.")
  String getStreamName();

  void setStreamName(String value);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Cloud Spanner shadow table prefix.",
      helpText = "The prefix used to name shadow tables. Default: `shadow_`.")
  @Default.String("shadow_")
  String getShadowTablePrefix();

  void setShadowTablePrefix(String value);

  @TemplateParameter.Boolean(
      order = 11,
      optional = true,
      description = "If true, create shadow tables in Cloud Spanner.",
      helpText =
          "This flag indicates whether shadow tables must be created in Cloud Spanner database.")
  @Default.Boolean(true)
  Boolean getShouldCreateShadowTables();

  void setShouldCreateShadowTables(Boolean value);

  @TemplateParameter.DateTime(
      order = 12,
      optional = true,
      description =
          "The starting DateTime used to fetch from Cloud Storage "
              + "(https://tools.ietf.org/html/rfc3339).",
      helpText =
          "The starting DateTime used to fetch from Cloud Storage "
              + "(https://tools.ietf.org/html/rfc3339).")
  @Default.String("1970-01-01T00:00:00.00Z")
  String getRfcStartDateTime();

  void setRfcStartDateTime(String value);

  @TemplateParameter.Integer(
      order = 13,
      optional = true,
      description = "File read concurrency",
      helpText = "The number of concurrent DataStream files to read.")
  @Default.Integer(30)
  Integer getFileReadConcurrency();

  void setFileReadConcurrency(Integer value);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "Dead letter queue directory.",
      helpText =
          "The file path used when storing the error queue output. "
              + "The default file path is a directory under the Dataflow job's temp location.")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  @TemplateParameter.Integer(
      order = 15,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead letter queue retries. Defaults to `10`.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  @TemplateParameter.Integer(
      order = 16,
      optional = true,
      description = "Dead letter queue maximum retry count",
      helpText =
          "The max number of times temporary errors can be retried through DLQ. Defaults to `500`.")
  @Default.Integer(500)
  Integer getDlqMaxRetryCount();

  void setDlqMaxRetryCount(Integer value);

  // DataStream API Root Url (only used for testing)
  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "Datastream API Root URL (only required for testing)",
      helpText = "Datastream API Root URL.")
  @Default.String("https://datastream.googleapis.com/")
  String getDataStreamRootUrl();

  void setDataStreamRootUrl(String value);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "Datastream source type (only required for testing)",
      helpText =
          "This is the type of source database that Datastream connects to. Example -"
              + " mysql/oracle. Need to be set when testing without an actual running"
              + " Datastream.")
  String getDatastreamSourceType();

  void setDatastreamSourceType(String value);

  @TemplateParameter.Boolean(
      order = 19,
      optional = true,
      description =
          "If true, rounds the decimal values in json columns to a number that can be stored"
              + " without loss of precision.",
      helpText =
          "This flag if set, rounds the decimal values in json columns to a number that can be"
              + " stored without loss of precision.")
  @Default.Boolean(false)
  Boolean getRoundJsonDecimals();

  void setRoundJsonDecimals(Boolean value);

  @TemplateParameter.Enum(
      order = 20,
      optional = true,
      description = "Run mode - currently supported are : regular, retryDLQ, or retryAllDLQ",
      enumOptions = {
        @TemplateEnumOption(Constants.RUN_MODE_REGULAR),
        @TemplateEnumOption(Constants.RUN_MODE_RETRY_DLQ),
        @TemplateEnumOption(Constants.RUN_MODE_RETRY_ALL_DLQ)
      },
      helpText =
          "This is the run mode type. Default is regular. Use `retryDLQ` mode to process exclusively severe error files concurrently with your live migration pipeline. Use `retryAllDLQ` mode only when the regular pipeline is stopped. This mode processes both retry and severe directories. Do NOT run `retryAllDLQ` concurrently with any active pipeline as it will cause conflicts.")
  @Default.String(Constants.RUN_MODE_REGULAR)
  String getRunMode();

  void setRunMode(String value);

  @TemplateParameter.GcsReadFile(
      order = 21,
      optional = true,
      helpText =
          "Transformation context file path in cloud storage used to populate data used in"
              + " transformations performed during migrations   Eg: The shard id to db name to"
              + " identify the db from which a row was migrated",
      description = "Transformation context file path in cloud storage")
  String getTransformationContextFilePath();

  void setTransformationContextFilePath(String value);

  @TemplateParameter.Integer(
      order = 22,
      optional = true,
      description = "Directory watch duration in minutes. Default: 10 minutes",
      helpText =
          "The Duration for which the pipeline should keep polling a directory in GCS. Datastream"
              + "output files are arranged in a directory structure which depicts the timestamp "
              + "of the event grouped by minutes. This parameter should be approximately equal to"
              + "maximum delay which could occur between event occurring in source database and "
              + "the same event being written to GCS by Datastream. 99.9 percentile = 10 minutes")
  @Default.Integer(10)
  Integer getDirectoryWatchDurationInMinutes();

  void setDirectoryWatchDurationInMinutes(Integer value);

  @TemplateParameter.Enum(
      order = 23,
      enumOptions = {
        @TemplateEnumOption("LOW"),
        @TemplateEnumOption("MEDIUM"),
        @TemplateEnumOption("HIGH")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The request priority for Cloud Spanner calls. The value must be one of:"
              + " [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.")
  @Default.Enum("HIGH")
  RpcPriority getSpannerPriority();

  void setSpannerPriority(RpcPriority value);

  @TemplateParameter.PubsubSubscription(
      order = 24,
      optional = true,
      description =
          "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
              + " retry directory when running in regular mode.",
      helpText =
          "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
              + " retry directory when running in regular mode. For the name, use the format"
              + " `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`. When set, the"
              + " deadLetterQueueDirectory and dlqRetryMinutes are ignored.")
  String getDlqGcsPubSubSubscription();

  void setDlqGcsPubSubSubscription(String value);

  @TemplateParameter.GcsReadFile(
      order = 25,
      optional = true,
      description = "Custom jar location in Cloud Storage",
      helpText =
          "Custom JAR file location in Cloud Storage for the file that contains the custom transformation logic for processing records"
              + " in forward migration.")
  @Default.String("")
  String getTransformationJarPath();

  void setTransformationJarPath(String value);

  @TemplateParameter.Text(
      order = 26,
      optional = true,
      description = "Custom class name",
      helpText =
          "Fully qualified class name having the custom transformation logic.  It is a"
              + " mandatory field in case transformationJarPath is specified")
  @Default.String("")
  String getTransformationClassName();

  void setTransformationClassName(String value);

  @TemplateParameter.Text(
      order = 27,
      optional = true,
      description = "Custom parameters for transformation",
      helpText =
          "String containing any custom parameters to be passed to the custom transformation class.")
  @Default.String("")
  String getTransformationCustomParameters();

  void setTransformationCustomParameters(String value);

  @TemplateParameter.Text(
      order = 28,
      optional = true,
      description = "Filtered events directory",
      helpText =
          "This is the file path to store the events filtered via custom transformation. Default is a directory"
              + " under the Dataflow job's temp location. The default value is enough under most"
              + " conditions.")
  @Default.String("")
  String getFilteredEventsDirectory();

  void setFilteredEventsDirectory(String value);

  @TemplateParameter.GcsReadFile(
      order = 29,
      optional = true,
      description = "Source Config URL",
      helpText =
          "Cloud Storage path to a shard config file for sharded migrations. It expects a HOCON or JSON file. For a sample file, please refer to v2/datastream-to-spanner/src/test/resources/DatastreamToSpannerSingleDFShardedMigrationIT/sharding-config.conf in the repository. For example, `gs://my-bucket/my-shard-config.conf`.",
      example = "gs://my-bucket/my-shard-config.conf")
  String getSourceConfigURL();

  void setSourceConfigURL(String value);

  @TemplateParameter.Text(
      order = 30,
      optional = true,
      description = "Table name overrides from source to spanner",
      regexes =
          "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
      example = "[{Singers, Vocalists}, {Albums, Records}]",
      helpText =
          "These are the table name overrides from source to spanner. They are written in the"
              + "following format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]"
              + "This example shows mapping Singers table to Vocalists and Albums table to Records.")
  @Default.String("")
  String getTableOverrides();

  void setTableOverrides(String value);

  @TemplateParameter.Text(
      order = 31,
      optional = true,
      regexes =
          "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
      description = "Column name overrides from source to spanner",
      example = "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
      helpText =
          "These are the column name overrides from source to spanner. They are written in the"
              + "following format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]"
              + "Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides."
              + "The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively.")
  @Default.String("")
  String getColumnOverrides();

  void setColumnOverrides(String value);

  @TemplateParameter.Text(
      order = 32,
      optional = true,
      description = "File based overrides from source to spanner",
      helpText =
          "A file which specifies the table and the column name overrides from source to spanner.")
  @Default.String("")
  String getSchemaOverridesFilePath();

  void setSchemaOverridesFilePath(String value);

  @TemplateParameter.Text(
      order = 33,
      optional = true,
      groupName = "Target",
      description = "Cloud Spanner Shadow Table Instance Id.",
      helpText =
          "Optional separate instance for shadow tables. If not specified, shadow tables will be created in the main instance. If specified, ensure shadowTableSpannerDatabaseId is specified as well.")
  @Default.String("")
  String getShadowTableSpannerInstanceId();

  void setShadowTableSpannerInstanceId(String value);

  @TemplateParameter.Text(
      order = 33,
      optional = true,
      groupName = "Target",
      description = "Cloud Spanner Shadow Table Database Id.",
      helpText =
          "Optional separate database for shadow tables. If not specified, shadow tables will be created in the main database. If specified, ensure shadowTableSpannerInstanceId is specified as well.")
  @Default.String("")
  String getShadowTableSpannerDatabaseId();

  void setShadowTableSpannerDatabaseId(String value);

  @TemplateParameter.Text(
      order = 34,
      optional = true,
      description = "Failure injection parameter",
      helpText = "Failure injection parameter. Only used for testing.")
  @Default.String("")
  String getFailureInjectionParameter();

  void setFailureInjectionParameter(String value);
}
