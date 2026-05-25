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

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.ReaderImpl;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerServiceFactoryImpl;
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

  public static PipelineResult executeMigrationForDbConfigContainer(
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

  @VisibleForTesting
  static void setupLogicalDbMigration(
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
      LOG.info("level: {} source tables: {}", currentLevel, sourceTables);
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
      String suffix = generateSuffix(currentLevel + "");

      if (options.getFailureInjectionParameter() != null
          && !options.getFailureInjectionParameter().isBlank()) {
        spannerConfig =
            SpannerServiceFactoryImpl.createSpannerService(
                spannerConfig, options.getFailureInjectionParameter());
      }

      PCollection<Void> output =
          pipeline.apply(
              "Migrate" + suffix,
              new MigrateTableTransform(
                  options,
                  spannerConfig,
                  tableSelector.getDdl(),
                  tableSelector.getSchemaMapper(),
                  reader));
      levelVsOutputMap.put(currentLevel, output);
    }

    // Add transform to increment table counter
    Map<Integer, OnSignal<?>> tableCompletionMap =
        levelVsOutputMap.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> Wait.on(e.getValue())));
    pipeline.apply(
        "Increment_table_counters",
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

  private static String generateSuffix(String tableName) {
    String suffix = "";
    if (!StringUtils.isEmpty(tableName)) {
      suffix += "_" + tableName;
    }
    return suffix;
  }

  @VisibleForTesting
  static ISchemaMapper getSchemaMapper(SourceDbToSpannerOptions options, Ddl ddl) {
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

    public static SourceConnectionConfig getSourceConnectionConfig(
            String sourceType, String sourceShardsFilePath) {
        ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
        SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
        SourceConnectionConfig sourceConnectionConfig;
        try {
            // Parse the source shards configuration file to respective
            // SourceConnectionConfig.
            LOG.info("Parsing source shards configuration file: {}", sourceShardsFilePath);
            return sourceConfigParser.parseConfiguration(sourceType, sourceShardsFilePath);
        } catch (Exception e) {
            LOG.error("Error parsing source config", e);
            throw new RuntimeException("Error parsing source config", e);
        }
    }
}
