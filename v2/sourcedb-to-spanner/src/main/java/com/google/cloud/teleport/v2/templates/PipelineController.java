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

import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.transformer.SourceRowToMutationDoFn;
import com.google.cloud.teleport.v2.writer.DeadLetterQueue;
import com.google.cloud.teleport.v2.writer.SpannerWriter;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Place perform pipeline level orchestration, scheduling and tuning operations. */
public class PipelineController {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);

  static PipelineResult executeSingleInstanceMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {

    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);

    List<String> tablesToMigrate =
        PipelineController.listTablesToMigrate(options.getTables(), schemaMapper, ddl);
    // Read data from source
    ReaderImpl reader =
        ReaderImpl.of(
            JdbcIoWrapper.of(
                OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(
                    options, tablesToMigrate, null)));
    migrateForReader(options, pipeline, spannerConfig, ddl, schemaMapper, reader, "");
    return pipeline.run();
  }

  /**
   * Perform migration for a given reader. This created a separate dag on dataflow per reader.
   *
   * @param options
   * @param pipeline
   * @param spannerConfig
   * @param ddl
   * @param schemaMapper
   * @param reader
   * @param shardId
   */
  private static void migrateForReader(
      SourceDbToSpannerOptions options,
      Pipeline pipeline,
      SpannerConfig spannerConfig,
      Ddl ddl,
      ISchemaMapper schemaMapper,
      ReaderImpl reader,
      String shardId) {
    String shardIdSuffix = StringUtils.isEmpty(shardId) ? "" : "_" + shardId;
    SourceSchema srcSchema = reader.getSourceSchema();

    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple rowsAndTables =
        pipeline.apply("Read_rows" + shardIdSuffix, readerTransform.readTransform());
    PCollection<SourceRow> sourceRows = rowsAndTables.get(readerTransform.sourceRowTag());

    // Transform source data to Spanner Compatible Data
    SourceRowToMutationDoFn transformDoFn = SourceRowToMutationDoFn.create(schemaMapper);
    PCollectionTuple transformationResult =
        sourceRows.apply(
            "Transform" + shardIdSuffix,
            ParDo.of(transformDoFn)
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)));

    // Write to Spanner
    SpannerWriter writer = new SpannerWriter(spannerConfig);
    PCollection<MutationGroup> failedMutations =
        writer.writeToSpanner(
            transformationResult
                .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
                .setCoder(SerializableCoder.of(RowContext.class)));

    // Dump Failed rows to DLQ
    DeadLetterQueue dlq = DeadLetterQueue.create(options.getDLQDirectory(), ddl);
    dlq.failedMutationsToDLQ(failedMutations);
    dlq.failedTransformsToDLQ(
        transformationResult
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
            .setCoder(SerializableCoder.of(RowContext.class)));
  }

  static PipelineResult executeShardedMigration(
      SourceDbToSpannerOptions options,
      Pipeline pipeline,
      List<Shard> shards,
      SpannerConfig spannerConfig) {
    // TODO
    // Merge logical shards into 1 physical shard
    // Populate completion per shard
    // Take connection properties map
    // Write to common DLQ ?

    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);

    List<String> tablesToMigrate =
        PipelineController.listTablesToMigrate(options.getTables(), schemaMapper, ddl);

    LOG.info(
        "running migration for shards: {}",
        shards.stream().map(s -> s.getHost()).collect(Collectors.toList()));
    for (Shard shard : shards) {
      for (Map.Entry<String, String> entry : shard.getDbNameToLogicalShardIdMap().entrySet()) {
        // Read data from source
        ReaderImpl reader =
            ReaderImpl.of(
                JdbcIoWrapper.of(
                    OptionsToConfigBuilder.getJdbcIOWrapperConfig(
                        tablesToMigrate,
                        null,
                        shard.getHost(),
                        Integer.parseInt(shard.getPort()),
                        shard.getUserName(),
                        shard.getPassword(),
                        entry.getKey(),
                        entry.getValue(),
                        options.getJdbcDriverClassName(),
                        options.getJdbcDriverJars(),
                        options.getMaxConnections(),
                        options.getNumPartitions())));
        migrateForReader(
            options, pipeline, spannerConfig, ddl, schemaMapper, reader, entry.getValue());
      }
    }
    return pipeline.run();
  }

  @VisibleForTesting
  static SpannerConfig createSpannerConfig(SourceDbToSpannerOptions options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));
  }

  @VisibleForTesting
  static ISchemaMapper getSchemaMapper(SourceDbToSpannerOptions options, Ddl ddl) {
    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (options.getSessionFilePath() != null && !options.getSessionFilePath().equals("")) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    }
    return schemaMapper;
  }

  /*
   * Return the available tables to migrate based on the following.
   * 1. Fetch tables from schema mapper. Override with tables from options if present
   * 2. Mark for migration if tables have corresponding spanner tables.
   * Err on the side of being lenient with configuration
   */
  static List<String> listTablesToMigrate(String tableList, ISchemaMapper mapper, Ddl ddl) {
    List<String> tablesFromOptions =
        StringUtils.isNotBlank(tableList)
            ? Arrays.stream(tableList.split(",")).collect(Collectors.toList())
            : new ArrayList<String>();

    List<String> sourceTablesConfigured = null;
    if (tablesFromOptions.isEmpty()) {
      sourceTablesConfigured = mapper.getSourceTablesToMigrate("");
      LOG.info("using tables from mapper as no overrides provided: {}", sourceTablesConfigured);
    } else {
      LOG.info("table overrides configured: {}", tablesFromOptions);
      sourceTablesConfigured = tablesFromOptions;
    }

    List<String> tablesToMigrate = new ArrayList<>();
    for (String srcTable : sourceTablesConfigured) {
      String spannerTable = null;
      try {
        spannerTable = mapper.getSpannerTableName("", srcTable);
      } catch (NoSuchElementException e) {
        LOG.info("could not fetch spanner table from mapper: {}", srcTable);
        continue;
      }

      if (spannerTable == null) {
        LOG.warn("skipping source table as there is no mapped spanner table: {} ", spannerTable);
      } else if (ddl.table(spannerTable) == null) {
        LOG.warn(
            "skipping source table: {} as there is no matching spanner table: {} ",
            srcTable,
            spannerTable);
      } else {
        // source table has matching spanner table on current spanner instance
        tablesToMigrate.add(srcTable);
      }
    }

    if (tablesToMigrate.isEmpty()) {
      LOG.error("aborting migration as no tables found to migrate");
      throw new InvalidOptionsException("no configured tables can be migrated");
    }
    return tablesToMigrate;
  }
}
