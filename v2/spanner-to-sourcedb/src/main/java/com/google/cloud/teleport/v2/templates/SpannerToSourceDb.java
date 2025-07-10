/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.PubSubNotifiedDlqIO;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraConfigFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessorFactory;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.ConvertChangeStreamErrorRecordToFailsafeElementFn;
import com.google.cloud.teleport.v2.templates.transforms.ConvertDlqRecordToTrimmedShardedDataChangeRecordFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.SourceWriterTransform;
import com.google.cloud.teleport.v2.templates.transforms.UpdateDlqMetricsFn;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This pipeline reads Spanner Change streams data and writes them to a source DB. */
@Template(
    name = "Spanner_to_SourceDb",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Source Database",
    description =
        "Streaming pipeline. Reads data from Spanner Change Streams and"
            + " writes them to a source.",
    optionsClass = Options.class,
    flexContainerName = "spanner-to-sourcedb",
    contactInformation = "https://cloud.google.com/support",
    hidden = false,
    streaming = true)
public class SpannerToSourceDb {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

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
        order = 7,
        optional = true,
        description = "Changes are read from the given timestamp",
        helpText = "Read changes from the given timestamp.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Changes are read until the given timestamp",
        helpText =
            "Read changes until the given timestamp. If no timestamp provided, reads indefinitely.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Cloud Spanner shadow table prefix.",
        helpText = "The prefix used to name shadow tables. Default: `shadow_`.")
    @Default.String("rev_shadow_")
    String getShadowTablePrefix();

    void setShadowTablePrefix(String value);

    @TemplateParameter.GcsReadFile(
        order = 10,
        optional = false,
        description = "Path to GCS file containing the the Source shard details",
        helpText = "Path to GCS file containing connection profile info for source shards.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 11,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 12,
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
        order = 13,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the customization logic"
                + " for fetching shard id.")
    @Default.String("")
    String getShardingCustomJarPath();

    void setShardingCustomJarPath(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom shard id implementation.  It is a"
                + " mandatory field in case shardingCustomJarPath is specified")
    @Default.String("")
    String getShardingCustomClassName();

    void setShardingCustomClassName(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Custom sharding logic parameters",
        helpText =
            "String containing any custom parameters to be passed to the custom sharding class.")
    @Default.String("")
    String getShardingCustomParameters();

    void setShardingCustomParameters(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.PubsubSubscription(
        order = 17,
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
        order = 18,
        optional = true,
        description = "Directory name for holding skipped records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("skip")
    String getSkipDirectoryName();

    void setSkipDirectoryName(String value);

    @TemplateParameter.Long(
        order = 19,
        optional = true,
        description = "Maximum connections per shard.",
        helpText = "This will come from shard file eventually.")
    @Default.Long(10000)
    Long getMaxShardConnections();

    void setMaxShardConnections(Long value);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The file path used when storing the error queue output. "
                + "The default file path is a directory under the Dataflow job's temp location.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 21,
        optional = true,
        description = "Dead letter queue maximum retry count",
        helpText =
            "The max number of times temporary errors can be retried through DLQ. Defaults to 500.")
    @Default.Integer(500)
    Integer getDlqMaxRetryCount();

    void setDlqMaxRetryCount(Integer value);

    @TemplateParameter.Enum(
        order = 22,
        optional = true,
        description = "Run mode - currently supported are : regular or retryDLQ",
        enumOptions = {@TemplateEnumOption("regular"), @TemplateEnumOption("retryDLQ")},
        helpText =
            "This is the run mode type, whether regular or with retryDLQ.Default is regular."
                + " retryDLQ is used to retry the severe DLQ records only.")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.Integer(
        order = 23,
        optional = true,
        description = "Dead letter queue retry minutes",
        helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);

    @TemplateParameter.Enum(
        order = 24,
        optional = true,
        description = "Source database type, ex: mysql",
        enumOptions = {@TemplateEnumOption("mysql"), @TemplateEnumOption("cassandra")},
        helpText = "The type of source database to reverse replicate to.")
    @Default.String("mysql")
    String getSourceType();

    void setSourceType(String value);

    @TemplateParameter.GcsReadFile(
        order = 25,
        optional = true,
        description = "Custom transformation jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the custom transformation logic for processing records"
                + " in reverse replication.")
    @Default.String("")
    String getTransformationJarPath();

    void setTransformationJarPath(String value);

    @TemplateParameter.Text(
        order = 26,
        optional = true,
        description = "Custom class name for transformation",
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
        order = 29,
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
        order = 30,
        optional = true,
        description = "File based overrides from spanner to source",
        helpText =
            "A file which specifies the table and the column name overrides from spanner to source.")
    @Default.String("")
    String getSchemaOverridesFilePath();

    void setSchemaOverridesFilePath(String value);

    @TemplateParameter.Text(
        order = 31,
        optional = true,
        description = "Directory name for holding filtered records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("filteredEvents")
    String getFilterEventsDirectoryName();

    void setFilterEventsDirectoryName(String value);

    @TemplateParameter.Boolean(
        order = 32,
        optional = true,
        description = "Boolean setting if reverse migration is sharded",
        helpText =
            "Sets the template to a sharded migration. If source shard template contains more"
                + " than one shard, the value will be set to true. This value defaults to false.")
    @Default.Boolean(false)
    Boolean getIsShardedMigration();

    void setIsShardedMigration(Boolean value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sink");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);

    // calculate the max connections per worker
    int maxNumWorkers =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers() > 0
            ? pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers()
            : 1;
    int connectionPoolSizePerWorker = (int) (options.getMaxShardConnections() / maxNumWorkers);
    if (connectionPoolSizePerWorker < 1) {
      // This can happen when the number of workers is more than max.
      // This can cause overload on the source database. Error out and let the user know.
      LOG.error(
          "Max workers {} is more than max shard connections {}, this can lead to more database"
              + " connections than desired",
          maxNumWorkers,
          options.getMaxShardConnections());
      throw new IllegalArgumentException(
          "Max Dataflow workers "
              + maxNumWorkers
              + " is more than max per shard connections: "
              + options.getMaxShardConnections()
              + " this can lead to more"
              + " database connections than desired. Either reduce the max allowed workers or"
              + " incease the max shard connections");
    }

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

    // Create shadow tables
    // Note that there is a limit on the number of tables that can be created per DB: 5000.
    // If we create shadow tables per shard, there will be an explosion of tables.
    // Anyway the shadow table has Spanner PK so no need to again separate by the shard
    // Lookup by the Spanner PK should be sufficient.

    // Prepare Spanner config
    SpannerConfig spannerMetadataConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getMetadataInstance()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getMetadataDatabase()));

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            spannerConfig,
            spannerMetadataConfig,
            SpannerAccessor.getOrCreate(spannerMetadataConfig)
                .getDatabaseAdminClient()
                .getDatabase(
                    spannerMetadataConfig.getInstanceId().get(),
                    spannerMetadataConfig.getDatabaseId().get())
                .getDialect(),
            options.getShadowTablePrefix());

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);

    shadowTableCreator.createShadowTablesInSpanner();
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    List<Shard> shards;
    String shardingMode;

    // Create appropriate schema mapper based on source type
    if (MYSQL_SOURCE_TYPE.equals(options.getSourceType())) {
      ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
      shards = shardFileReader.getOrderedShardDetails(options.getSourceShardsFilePath());
      shardingMode = Constants.SHARDING_MODE_MULTI_SHARD;
    } else {
      CassandraConfigFileReader cassandraConfigFileReader = new CassandraConfigFileReader();
      shards = cassandraConfigFileReader.getCassandraShard(options.getSourceShardsFilePath());
      LOG.info("Cassandra config is: {}", shards.get(0));
      shardingMode = Constants.SHARDING_MODE_SINGLE_SHARD;
    }
    SourceInformationSchemaScanner scanner = null;
    try {
      if (options.getSourceType().equals(MYSQL_SOURCE_TYPE)) {
        SourceProcessorFactory.initializeConnectionHelper(
            options.getSourceType(), shards, connectionPoolSizePerWorker);
        Connection connection =
            (Connection)
                SourceProcessorFactory.getConnectionToShard(options.getSourceType(), shards.get(0));
        scanner = new MySqlInformationSchemaScanner(connection, shards.get(0).getDbName());
      } else {
        try (CqlSession session = createCqlSession((CassandraShard) shards.get(0))) {
          scanner =
              new CassandraInformationSchemaScanner(
                  session, ((CassandraShard) shards.get(0)).getKeySpaceName());
        }
      }
    } catch (UnsupportedSourceException e) {
      LOG.error("Unsupported source type: {}", options.getSourceType());
      throw new IllegalArgumentException(e);
    } catch (ConnectionException e) {
      LOG.error("Error getting connection to shard: {}", shards.get(0));
      throw new IllegalArgumentException(e);
    }
    SourceSchema sourceSchema = scanner.scan();
    LOG.info("Source schema: {}", sourceSchema);

    if (shards.size() == 1 && !options.getIsShardedMigration()) {
      shardingMode = Constants.SHARDING_MODE_SINGLE_SHARD;
      Shard shard = shards.get(0);
      if (shard.getLogicalShardId() == null) {
        shard.setLogicalShardId(Constants.DEFAULT_SHARD_ID);
        LOG.info(
            "Logical shard id was not found, hence setting it to : " + Constants.DEFAULT_SHARD_ID);
      }
    }
    ISchemaMapper schemaMapper = getSchemaMapper(options, ddl);

    boolean isRegularMode = "regular".equals(options.getRunMode());
    PCollectionTuple reconsumedElements = null;
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    int reshuffleBucketSize =
        maxNumWorkers
            * (debugOptions.getNumberOfWorkerHarnessThreads() > 0
                ? debugOptions.getNumberOfWorkerHarnessThreads()
                : Constants.DEFAULT_WORKER_HARNESS_THREAD_COUNT);

    if (isRegularMode && (!Strings.isNullOrEmpty(options.getDlqGcsPubSubSubscription()))) {
      reconsumedElements =
          dlqManager.getReconsumerDataTransformForFiles(
              pipeline.apply(
                  "Read retry from PubSub",
                  new PubSubNotifiedDlqIO(
                      options.getDlqGcsPubSubSubscription(),
                      // file paths to ignore when re-consuming for retry
                      new ArrayList<String>(
                          Arrays.asList(
                              "/severe/",
                              "/tmp_retry",
                              "/tmp_severe/",
                              ".temp",
                              "/tmp_skip/",
                              "/" + options.getSkipDirectoryName())))));
    } else {
      reconsumedElements =
          dlqManager.getReconsumerDataTransform(
              pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    }

    PCollection<FailsafeElement<String, String>> dlqJsonStrRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<TrimmedShardedDataChangeRecord> dlqRecords =
        dlqJsonStrRecords
            .apply(
                "Convert DLQ records to TrimmedShardedDataChangeRecord",
                ParDo.of(new ConvertDlqRecordToTrimmedShardedDataChangeRecordFn()))
            .setCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class));
    PCollection<TrimmedShardedDataChangeRecord> mergedRecords = null;
    if (isRegularMode) {
      PCollection<TrimmedShardedDataChangeRecord> changeRecordsFromDB =
          pipeline
              .apply(
                  getReadChangeStreamDoFn(
                      options,
                      spannerConfig)) // This emits PCollection<DataChangeRecord> which is Spanner
              // change
              // stream data
              .apply("Reshuffle", Reshuffle.viaRandomKey())
              .apply("Filteration", ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
              .apply("Preprocess", ParDo.of(new PreprocessRecordsFn()))
              .setCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class));
      mergedRecords =
          PCollectionList.of(changeRecordsFromDB)
              .and(dlqRecords)
              .apply("Flatten", Flatten.pCollections())
              .setCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class));
    } else {
      mergedRecords = dlqRecords;
    }
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();
    SourceWriterTransform.Result sourceWriterOutput =
        mergedRecords
            .apply(
                "AssignShardId", // This emits PCollection<KV<Long,
                // TrimmedShardedDataChangeRecord>> which is Spanner change stream data with key as
                // PK
                // mod
                // number of parallelism
                ParDo.of(
                    new AssignShardIdFn(
                        spannerConfig,
                        schemaMapper,
                        ddl,
                        sourceSchema,
                        shardingMode,
                        shards.get(0).getLogicalShardId(),
                        options.getSkipDirectoryName(),
                        options.getShardingCustomJarPath(),
                        options.getShardingCustomClassName(),
                        options.getShardingCustomParameters(),
                        options.getMaxShardConnections() * shards.size(),
                        options.getSourceType()))) // currently assuming that all shards accept the
            // same
            .setCoder(
                KvCoder.of(
                    VarLongCoder.of(), SerializableCoder.of(TrimmedShardedDataChangeRecord.class)))
            .apply("Reshuffle2", Reshuffle.of())
            .apply(
                "Write to source",
                new SourceWriterTransform(
                    shards,
                    schemaMapper,
                    spannerMetadataConfig,
                    options.getSourceDbTimezoneOffset(),
                    ddl,
                    sourceSchema,
                    options.getShadowTablePrefix(),
                    options.getSkipDirectoryName(),
                    connectionPoolSizePerWorker,
                    options.getSourceType(),
                    customTransformation));

    PCollection<FailsafeElement<String, String>> dlqPermErrorRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.PERMANENT_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<FailsafeElement<String, String>> permErrorsFromSourceWriter =
        sourceWriterOutput
            .permanentErrors()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle3", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert permanent errors from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<FailsafeElement<String, String>> permanentErrors =
        PCollectionList.of(dlqPermErrorRecords)
            .and(permErrorsFromSourceWriter)
            .apply(Flatten.pCollections())
            .apply("Reshuffle", Reshuffle.viaRandomKey());

    permanentErrors
        .apply("Update DLQ metrics", ParDo.of(new UpdateDlqMetricsFn(isRegularMode)))
        .apply(
            "DLQ: Write Severe errors to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ for severe errors",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory((options).getDeadLetterQueueDirectory() + "/tmp_severe/")
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> retryErrors =
        sourceWriterOutput
            .retryableErrors()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle4", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert retryable errors from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    retryErrors
        .apply(
            "DLQ: Write retryable Failures to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ for retryable errors",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getRetryDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_retry/")
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> skippedRecords =
        sourceWriterOutput
            .skippedSourceWrites()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle5", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert skipped records from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    skippedRecords
        .apply(
            "Write skipped records to GCS", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Writing skipped records to GCS",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(
                    options.getDeadLetterQueueDirectory() + "/" + options.getSkipDirectoryName())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_skip/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      Options options, SpannerConfig spannerConfig) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(options.getChangeStreamName())
            .withMetadataInstance(options.getMetadataInstance())
            .withMetadataDatabase(options.getMetadataDatabase())
            .withInclusiveStartAt(startTime);
    if (!options.getEndTimestamp().equals("")) {
      return readChangeStreamDoFn.withInclusiveEndAt(
          Timestamp.parseTimestamp(options.getEndTimestamp()));
    }
    return readChangeStreamDoFn;
  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();
    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    options.setDeadLetterQueueDirectory(dlqDirectory);
    if ("regular".equals(options.getRunMode())) {
      return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetryCount());
    } else {
      String retryDlqUri =
          FileSystems.matchNewResource(dlqDirectory, true)
              .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
              .toString();
      LOG.info("Dead-letter retry directory: {}", retryDlqUri);
      return DeadLetterQueueManager.create(dlqDirectory, retryDlqUri, 0);
    }
  }

  private static ISchemaMapper getSchemaMapper(Options options, Ddl ddl) {
    // Check if config types are specified
    boolean hasSessionFile =
        options.getSessionFilePath() != null && !options.getSessionFilePath().equals("");
    boolean hasSchemaOverridesFile =
        options.getSchemaOverridesFilePath() != null
            && !options.getSchemaOverridesFilePath().equals("");
    boolean hasStringOverrides =
        (options.getTableOverrides() != null && !options.getTableOverrides().equals(""))
            || (options.getColumnOverrides() != null && !options.getColumnOverrides().equals(""));

    int overrideTypesCount = 0;
    if (hasSessionFile) {
      overrideTypesCount++;
    }
    if (hasSchemaOverridesFile) {
      overrideTypesCount++;
    }
    if (hasStringOverrides) {
      overrideTypesCount++;
    }

    if (overrideTypesCount > 1) {
      throw new IllegalArgumentException(
          "Only one type of schema override can be specified. Please use only one of: sessionFilePath, "
              + "schemaOverridesFilePath, or tableOverrides/columnOverrides.");
    }

    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (hasSessionFile) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    } else if (hasSchemaOverridesFile) {
      schemaMapper = new SchemaFileOverridesBasedMapper(options.getSchemaOverridesFilePath(), ddl);
    } else if (hasStringOverrides) {
      Map<String, String> userOptionsOverrides = new HashMap<>();
      if (!options.getTableOverrides().isEmpty()) {
        userOptionsOverrides.put("tableOverrides", options.getTableOverrides());
      }
      if (!options.getColumnOverrides().isEmpty()) {
        userOptionsOverrides.put("columnOverrides", options.getColumnOverrides());
      }
      schemaMapper = new SchemaStringOverridesBasedMapper(userOptionsOverrides, ddl);
    }
    return schemaMapper;
  }

  /**
   * Creates a {@link CqlSession} for the given {@link CassandraShard}.
   *
   * @param cassandraShard The shard containing connection details.
   * @return A {@link CqlSession} instance.
   */
  private static CqlSession createCqlSession(CassandraShard cassandraShard) {
    CqlSessionBuilder builder = CqlSession.builder();
    DriverConfigLoader configLoader =
        CassandraDriverConfigLoader.fromOptionsMap(cassandraShard.getOptionsMap());
    builder.withConfigLoader(configLoader);
    return builder.build();
  }
}
