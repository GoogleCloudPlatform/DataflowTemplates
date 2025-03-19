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

import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraIOWrapperFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Place perform pipeline level orchestration, scheduling and tuning operations. */
public class PipelineController {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);
  private static final Counter tablesCompleted =
      Metrics.counter(PipelineController.class, "tablesCompleted");

  @VisibleForTesting
  protected static PipelineResult executeSingleInstanceMigrationForDbConfigContainer(
      SourceDbToSpannerOptions options,
      Pipeline pipeline,
      SpannerConfig spannerConfig,
      DbConfigContainer dbConfigContainer) {
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);
    TableSelector tableSelector = new TableSelector(options.getTables(), ddl, schemaMapper);

    Map<Integer, List<String>> levelToSpannerTableList = tableSelector.levelOrderedSpannerTables();
    setupLogicalDbMigration(
        options,
        pipeline,
        spannerConfig,
        tableSelector,
        levelToSpannerTableList,
        dbConfigContainer);

    return pipeline.run();
  }

  static PipelineResult executeJdbcSingleInstanceMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
    JdbcDbConfigContainer jdbcDbConfigContainer = new SingleInstanceJdbcDbConfigContainer(options);
    return executeSingleInstanceMigrationForDbConfigContainer(
        options, pipeline, spannerConfig, jdbcDbConfigContainer);
  }

  static PipelineResult executeJdbcShardedMigration(
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
    TableSelector tableSelector = new TableSelector(options.getTables(), ddl, schemaMapper);

    Map<Integer, List<String>> levelToSpannerTableList = tableSelector.levelOrderedSpannerTables();

    SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());

    LOG.info(
        "running migration for shards: {}",
        shards.stream().map(Shard::getHost).collect(Collectors.toList()));
    for (Shard shard : shards) {
      for (Map.Entry<String, String> entry : shard.getDbNameToLogicalShardIdMap().entrySet()) {
        // Read data from source
        String shardId = entry.getValue();

        // If a namespace is configured for a shard uses that, otherwise uses the namespace
        // configured in the options if there is one.
        String namespace = Optional.ofNullable(shard.getNamespace()).orElse(options.getNamespace());

        ShardedJdbcDbConfigContainer dbConfigContainer =
            new ShardedJdbcDbConfigContainer(
                shard, sqlDialect, namespace, shardId, entry.getKey(), options);
        setupLogicalDbMigration(
            options,
            pipeline,
            spannerConfig,
            tableSelector,
            levelToSpannerTableList,
            dbConfigContainer);
      }
    }
    return pipeline.run();
  }

  static PipelineResult executeCassandraMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
    return executeSingleInstanceMigrationForDbConfigContainer(
        options,
        pipeline,
        spannerConfig,
        new DbConfigContainerDefaultImpl(CassandraIOWrapperFactory.fromPipelineOptions(options)));
  }

  private static void setupLogicalDbMigration(
      SourceDbToSpannerOptions options,
      Pipeline pipeline,
      SpannerConfig spannerConfig,
      TableSelector tableSelector,
      Map<Integer, List<String>> levelToSpannerTableList,
      DbConfigContainer configContainer) {

    Map<Integer, PCollection<Void>> levelVsOutputMap = new HashMap<>();
    for (int currentLevel = 0; currentLevel < levelToSpannerTableList.size(); currentLevel++) {
      List<String> spannerTables = levelToSpannerTableList.get(currentLevel);
      LOG.info("processing level: {} spanner tables: {}", currentLevel, spannerTables);
      List<String> sourceTables =
          spannerTables.stream()
              .map(t -> tableSelector.getSchemaMapper().getSourceTableName("", t))
              .collect(Collectors.toList());
      LOG.info("level: {} source tables: {}", currentLevel, spannerTables);
      PCollection<Void> previousLevelPCollection = levelVsOutputMap.get(currentLevel - 1);
      if (currentLevel > 0 && previousLevelPCollection == null) {
        LOG.warn(
            "proceeding without waiting for parent. current level: {}  tables: {}",
            currentLevel,
            spannerTables);
      }
      OnSignal<@UnknownKeyFor @Nullable @Initialized Object> waitOnSignal =
          previousLevelPCollection != null ? Wait.on(previousLevelPCollection) : null;
      IoWrapper ioWrapper = configContainer.getIOWrapper(sourceTables, waitOnSignal);
      if (ioWrapper.getTableReaders().isEmpty()) {
        LOG.info("not creating reader as tables are not found at source: {}", sourceTables);
        // If tables of 1 level are ignored in middle, then the subsequent level will not wait to
        // begin processing.
        continue;
      }
      ReaderImpl reader = ReaderImpl.of(ioWrapper);
      String suffix = generateSuffix(configContainer.getShardId(), currentLevel + "");

      Map<String, String> srcTableToShardIdColumnMap =
          configContainer.getSrcTableToShardIdColumnMap(
              tableSelector.getSchemaMapper(), spannerTables);

      PCollection<Void> output =
          pipeline.apply(
              "Migrate" + suffix,
              new MigrateTableTransform(
                  options,
                  spannerConfig,
                  tableSelector.getDdl(),
                  tableSelector.getSchemaMapper(),
                  reader,
                  configContainer.getShardId(),
                  srcTableToShardIdColumnMap));
      levelVsOutputMap.put(currentLevel, output);
    }

    // Add transform to increment table counter
    Map<Integer, OnSignal<?>> tableCompletionMap =
        levelVsOutputMap.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> Wait.on(e.getValue())));
    pipeline.apply(
        "Increment_table_counters" + generateSuffix(configContainer.getShardId(), null),
        new IncrementTableCounter(tableCompletionMap, "", levelToSpannerTableList));
  }

  /**
   * For the spanner tables that contain the shard id column, returns the source table to
   * shardColumn.
   *
   * @param schemaMapper
   * @param namespace
   * @param spannerTables
   * @return
   */
  static Map<String, String> getSrcTableToShardIdColumnMap(
      ISchemaMapper schemaMapper, String namespace, List<String> spannerTables) {
    Map<String, String> srcTableToShardIdMap = new HashMap<>();
    for (String spTable : spannerTables) {
      String shardIdColumn = schemaMapper.getShardIdColumnName(namespace, spTable);
      if (shardIdColumn != null) {
        String srcTable = schemaMapper.getSourceTableName(namespace, spTable);
        srcTableToShardIdMap.put(srcTable, shardIdColumn);
      }
    }
    return srcTableToShardIdMap;
  }

  private static String generateSuffix(String shardId, String tableName) {
    String suffix = "";
    if (!StringUtils.isEmpty(shardId)) {
      suffix += "_" + shardId;
    }
    if (!StringUtils.isEmpty(tableName)) {
      suffix += "_" + tableName;
    }
    return suffix;
  }

  @VisibleForTesting
  static ISchemaMapper getSchemaMapper(SourceDbToSpannerOptions options, Ddl ddl) {
    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (options.getSessionFilePath() != null && !options.getSessionFilePath().equals("")) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    }
    return schemaMapper;
  }

  /** TODO(vardhanvthigle): Consider refactoring this to JDBC specific package. */
  interface JdbcDbConfigContainer extends DbConfigContainer {

    JdbcIOWrapperConfig getJDBCIOWrapperConfig(
        List<String> sourceTables, Wait.OnSignal<?> waitOnSignal);

    String getNamespace();

    @Override
    default IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
      return JdbcIoWrapper.of(getJDBCIOWrapperConfig(sourceTables, waitOnSignal));
    }

    @Override
    default Map<String, String> getSrcTableToShardIdColumnMap(
        ISchemaMapper schemaMapper, List<String> spannerTables) {
      String nameSpace = getNamespace();
      return PipelineController.getSrcTableToShardIdColumnMap(
          schemaMapper, nameSpace, spannerTables);
    }
  }

  static class ShardedJdbcDbConfigContainer implements JdbcDbConfigContainer {

    private Shard shard;

    private SQLDialect sqlDialect;

    private String namespace;

    private String shardId;

    private String dbName;

    private SourceDbToSpannerOptions options;

    public ShardedJdbcDbConfigContainer(
        Shard shard,
        SQLDialect sqlDialect,
        String namespace,
        String shardId,
        String dbName,
        SourceDbToSpannerOptions options) {
      this.shard = shard;
      this.sqlDialect = sqlDialect;
      this.namespace = namespace;
      this.shardId = shardId;
      this.dbName = dbName;
      this.options = options;
    }

    public JdbcIOWrapperConfig getJDBCIOWrapperConfig(
        List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
      return OptionsToConfigBuilder.getJdbcIOWrapperConfig(
          sqlDialect,
          sourceTables,
          null,
          shard.getHost(),
          shard.getConnectionProperties(),
          Integer.parseInt(shard.getPort()),
          shard.getUserName(),
          shard.getPassword(),
          dbName,
          namespace,
          shardId,
          options.getJdbcDriverClassName(),
          options.getJdbcDriverJars(),
          options.getMaxConnections(),
          options.getNumPartitions(),
          waitOnSignal,
          options.getFetchSize());
    }

    @Override
    public String getNamespace() {
      return namespace;
    }

    @Override
    public String getShardId() {
      return shardId;
    }
  }

  static class SingleInstanceJdbcDbConfigContainer implements JdbcDbConfigContainer {
    private SourceDbToSpannerOptions options;

    public SingleInstanceJdbcDbConfigContainer(SourceDbToSpannerOptions options) {
      this.options = options;
    }

    public JdbcIOWrapperConfig getJDBCIOWrapperConfig(
        List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
      return OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
          options, sourceTables, null, waitOnSignal);
    }

    @Override
    public String getNamespace() {
      return options.getNamespace();
    }

    public String getShardId() {
      return null;
    }
  }
}
