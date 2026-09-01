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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_REGULAR;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_RETRY_ALL_DLQ;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_RETRY_DLQ;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.StreamingOptions;

/**
 * Options supported by the pipeline.
 *
 * <p>Inherits standard configuration options.
 */
  public interface SpannerToSourceDbOptions extends CommonTemplateOptions, StreamingOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Name of the change stream to read from",
        helpText =
            "This is the name of the Spanner change stream that the pipeline will read from.")
    String getChangeStreamName();

    void setChangeStreamName(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changestream is present.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database that the changestream is monitoring")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 4,
        optional = false,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getSpannerProjectId();

    void setSpannerProjectId(String projectId);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        description = "Cloud Spanner Instance to store metadata when reading from changestreams",
        helpText =
            "This is the instance to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataInstance();

    void setMetadataInstance(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "Cloud Spanner Database to store metadata when reading from changestreams",
        helpText =
            "This is the database to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataDatabase();

    void setMetadataDatabase(String value);

    @TemplateParameter.Text(
        order = 36,
        optional = true,
        description = "Cloud Spanner Database to store change stream connector metadata",
        helpText =
            "This is the database to store the metadata used by the change stream connector. "
                + "If not provided, it defaults to the metadata database.")
    String getChangeStreamMetadataDatabase();

    void setChangeStreamMetadataDatabase(String value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Cloud Spanner metadata table name",
        helpText =
            "The Spanner change streams connector metadata table name to use. If not provided,"
                + " Spanner automatically creates the streams connector metadata table during the pipeline flow"
                + " change. You must provide this parameter when updating an existing pipeline to ensure"
                + " that the metadata table from the original job is carried over.")
    String getSpannerMetadataTableName();

    void setSpannerMetadataTableName(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Changes are read from the given timestamp",
        helpText = "Read changes from the given timestamp.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Changes are read until the given timestamp",
        helpText =
            "Read changes until the given timestamp. If no timestamp provided, reads indefinitely.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        description = "Cloud Spanner shadow table prefix.",
        helpText = "The prefix used to name shadow tables. Default: `shadow_`.")
    @Default.String("rev_shadow_")
    String getShadowTablePrefix();

    void setShadowTablePrefix(String value);

    @TemplateParameter.GcsReadFile(
        order = 11,
        optional = false,
        description = "Path to GCS file containing the the Source shard details",
        helpText = "Path to GCS file containing connection profile info for source shards.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 12,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 13,
        optional = true,
        enumOptions = {@TemplateEnumOption("none"), @TemplateEnumOption("forward_migration")},
        description = "Filtration mode",
        helpText =
            "Mode of Filtration, decides how to drop certain records based on a criteria. Currently"
                + " supported modes are: none (filter nothing), forward_migration (filter records"
                + " written via the forward migration pipeline). Defaults to forward_migration.")
    @Default.String("forward_migration")
    String getFiltrationMode();

    void setFiltrationMode(String value);

    @TemplateParameter.GcsReadFile(
        order = 14,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the customization logic"
                + " for fetching shard id.")
    @Default.String("")
    String getShardingCustomJarPath();

    void setShardingCustomJarPath(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom shard id implementation.  It is a"
                + " mandatory field in case shardingCustomJarPath is specified")
    @Default.String("")
    String getShardingCustomClassName();

    void setShardingCustomClassName(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "Custom sharding logic parameters",
        helpText =
            "String containing any custom parameters to be passed to the custom sharding class.")
    @Default.String("")
    String getShardingCustomParameters();

    void setShardingCustomParameters(String value);

    @TemplateParameter.Text(
        order = 17,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.PubsubSubscription(
        order = 18,
        optional = true,
        description =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode. The name should be in the format"
                + " of projects/<project-id>/subscriptions/<subscription-name>. When set, the"
                + " deadLetterQueueDirectory and dlqRetryMinutes are ignored.")
    String getDlqGcsPubSubSubscription();

    void setDlqGcsPubSubSubscription(String value);

    @TemplateParameter.Text(
        order = 19,
        optional = true,
        description = "Directory name for holding skipped records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("skip")
    String getSkipDirectoryName();

    void setSkipDirectoryName(String value);

    @TemplateParameter.Long(
        order = 20,
        optional = true,
        description = "Maximum connections per shard.",
        helpText = "This will come from shard file eventually.")
    @Default.Long(10000)
    Long getMaxShardConnections();

    void setMaxShardConnections(Long value);

    @TemplateParameter.Text(
        order = 21,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The file path used when storing the error queue output. "
                + "The default file path is a directory under the Dataflow job's temp location.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 22,
        optional = true,
        description = "Dead letter queue maximum retry count",
        helpText =
            "The max number of times temporary errors can be retried through DLQ. Defaults to 500.")
    @Default.Integer(500)
    Integer getDlqMaxRetryCount();

    void setDlqMaxRetryCount(Integer value);

    @TemplateParameter.Enum(
        order = 23,
        optional = true,
        description = "Run mode - currently supported are : regular, retryDLQ, or retryAllDLQ",
        enumOptions = {
          @TemplateEnumOption(RUN_MODE_REGULAR),
          @TemplateEnumOption(RUN_MODE_RETRY_DLQ),
          @TemplateEnumOption(RUN_MODE_RETRY_ALL_DLQ)
        },
        helpText =
            "This is the run mode type. Default is regular. Use `retryDLQ` mode to process exclusively severe error files concurrently with your reverse migration pipeline. Use `retryAllDLQ` mode only when the regular pipeline is stopped. This mode processes both retry and severe directories. Do NOT run `retryAllDLQ` concurrently with any active pipeline as it will cause conflicts.")
    @Default.String(RUN_MODE_REGULAR)
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.Integer(
        order = 24,
        optional = true,
        description = "Dead letter queue retry minutes",
        helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);

    @TemplateParameter.Enum(
        order = 25,
        optional = true,
        description = "Source database type, ex: mysql",
        enumOptions = {
          @TemplateEnumOption("mysql"),
          @TemplateEnumOption("cassandra"),
          @TemplateEnumOption("postgresql"),
          @TemplateEnumOption("spanner"),
          @TemplateEnumOption("oracle")
        },
        helpText = "The type of source database to reverse replicate to.")
    @Default.String("mysql")
    String getSourceType();

    void setSourceType(String value);

    @TemplateParameter.GcsReadFile(
        order = 26,
        optional = true,
        description = "Custom transformation jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the custom transformation logic for processing records"
                + " in reverse replication.")
    @Default.String("")
    String getTransformationJarPath();

    void setTransformationJarPath(String value);

    @TemplateParameter.Text(
        order = 27,
        optional = true,
        description = "Custom class name for transformation",
        helpText =
            "Fully qualified class name having the custom transformation logic.  It is a"
                + " mandatory field in case transformationJarPath is specified")
    @Default.String("")
    String getTransformationClassName();

    void setTransformationClassName(String value);

    @TemplateParameter.Text(
        order = 28,
        optional = true,
        description = "Custom parameters for transformation",
        helpText =
            "String containing any custom parameters to be passed to the custom transformation class.")
    @Default.String("")
    String getTransformationCustomParameters();

    void setTransformationCustomParameters(String value);

    @TemplateParameter.Text(
        order = 29,
        optional = true,
        description = "Table name overrides from spanner to source",
        regexes =
            "^\\[([[:space:]]*\\{[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example = "[{Singers, Vocalists}, {Albums, Records}]",
        helpText =
            "These are the table name overrides from spanner to source. They are written in the"
                + "following format: [{SpannerTableName1, SourceTableName1}, {SpannerTableName2, SourceTableName2}]"
                + "This example shows mapping Singers table to Vocalists and Albums table to Records.")
    @Default.String("")
    String getTableOverrides();

    void setTableOverrides(String value);

    @TemplateParameter.Text(
        order = 30,
        optional = true,
        description = "Column name overrides from spanner to source",
        regexes =
            "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example =
            "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
        helpText =
            "These are the column name overrides from spanner to source. They are written in the"
                + "following format: [{SpannerTableName1.SpannerColumnName1, SpannerTableName1.SourceColumnName1}, {SpannerTableName2.SpannerColumnName1, SpannerTableName2.SourceColumnName1}]"
                + "Note that the SpannerTableName should remain the same in both the spanner and source pair. To override table names, use tableOverrides."
                + "The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively.")
    @Default.String("")
    String getColumnOverrides();

    void setColumnOverrides(String value);

    @TemplateParameter.GcsReadFile(
        order = 31,
        optional = true,
        description = "File based overrides from spanner to source",
        helpText =
            "A file which specifies the table and the column name overrides from spanner to source.")
    @Default.String("")
    String getSchemaOverridesFilePath();

    void setSchemaOverridesFilePath(String value);

    @TemplateParameter.Text(
        order = 32,
        optional = true,
        description = "Directory name for holding filtered records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("filteredEvents")
    String getFilterEventsDirectoryName();

    void setFilterEventsDirectoryName(String value);

    @TemplateParameter.Boolean(
        order = 33,
        optional = true,
        description = "Boolean setting if reverse migration is sharded",
        helpText =
            "Sets the template to a sharded migration. If source shard template contains more"
                + " than one shard, the value will be set to true. This value defaults to false.")
    @Default.Boolean(false)
    Boolean getIsShardedMigration();

    void setIsShardedMigration(Boolean value);

    @TemplateParameter.Text(
        order = 34,
        optional = true,
        description = "Failure injection parameter",
        helpText = "Failure injection parameter. Only used for testing.")
    @Default.String("")
    String getFailureInjectionParameter();

    void setFailureInjectionParameter(String value);

    @TemplateParameter.Enum(
        order = 35,
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
  }
