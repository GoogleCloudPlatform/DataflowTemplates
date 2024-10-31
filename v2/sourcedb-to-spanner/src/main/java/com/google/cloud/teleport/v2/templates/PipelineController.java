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

import static com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants.MAX_RECOMMENDED_TABLES_PER_JOB;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Place perform pipeline level orchestration, scheduling and tuning operations. */
public class PipelineController {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);
  private static final Counter tablesCompleted =
      Metrics.counter(PipelineController.class, "tablesCompleted");

  // static PipelineResult executeSingleInstanceMigration(
  //     SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
  //
  //   Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
  //   ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);
  //
  //   List<String> srcTablesToMigrate = listTablesToMigrate(options.getTables(), schemaMapper, ddl);
  //   Set<String> tablesToMigrateSet = new HashSet<>(srcTablesToMigrate);
  //
  //   List<String> spannerTablesToMigrate =
  //       listSpannerTablesToMigrate(ddl, schemaMapper, tablesToMigrateSet);
  //
  //   Map<String, PCollection<Void>> outputs = new HashMap<>();
  //   for (String spTable : spannerTablesToMigrate) {
  //     String srcTable = schemaMapper.getSourceTableName("", spTable);
  //     List<PCollection<?>> parentOutputs = new ArrayList<>();
  //     for (String parentSpTable : ddl.tablesReferenced(spTable)) {
  //       String parentSrcName;
  //       try {
  //         parentSrcName = schemaMapper.getSourceTableName("", parentSpTable);
  //       } catch (NoSuchElementException e) {
  //         // This will occur when the spanner table name does not exist in source for
  //         // sessionBasedMapper.
  //         LOG.warn(
  //             spTable
  //                 + " references table "
  //                 + parentSpTable
  //                 + " which does not have an equivalent source table. Writes to "
  //                 + spTable
  //                 + " could fail, check DLQ for failed records.");
  //         continue;
  //       }
  //       // This parent is not in tables selected for migration.
  //       if (!tablesToMigrateSet.contains(parentSrcName)) {
  //         LOG.warn(
  //             spTable
  //                 + " references table "
  //                 + parentSpTable
  //                 + " which is not selected for migration (Provide the source table name "
  //                 + parentSrcName
  //                 + " via the 'tables' option if this is a mistake!). Writes to "
  //                 + spTable
  //                 + " could fail, check DLQ for failed records.");
  //         continue;
  //       }
  //       PCollection<Void> parentOutputPcollection = outputs.get(parentSrcName);
  //       // Since we are iterating the tables topologically, all parents should have been
  //       // processed.
  //       Preconditions.checkState(
  //           parentOutputPcollection != null,
  //           "Output PCollection for parent table should not be null.");
  //       parentOutputs.add(parentOutputPcollection);
  //     }
  //     JdbcIoWrapper jdbcIoWrapper =
  //         JdbcIoWrapper.of(
  //             OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
  //                 options, List.of(srcTable), null, Wait.on(parentOutputs)));
  //     if (jdbcIoWrapper.getTableReaders().isEmpty()) {
  //       LOG.info("not creating reader as table is not found at source: {}", srcTable);
  //       continue;
  //     }
  //     ReaderImpl reader = ReaderImpl.of(jdbcIoWrapper);
  //     String suffix = generateSuffix("", srcTable);
  //     String shardIdColumn = "";
  //     PCollection<Void> output =
  //         pipeline.apply(
  //             "Migrate" + suffix,
  //             new MigrateTableTransform(
  //                 options, spannerConfig, ddl, schemaMapper, reader, "", shardIdColumn));
  //     outputs.put(srcTable, output);
  //   }
  //
  //   // Add transform to increment table counter
  //   Map<String, Wait.OnSignal<?>> waitOnsMap =
  //       outputs.entrySet().stream()
  //           .collect(Collectors.toMap(Map.Entry::getKey, entry -> Wait.on(entry.getValue())));
  //   pipeline.apply("Increment_table_counters", new IncrementTableCounter(waitOnsMap, ""));
  //
  //   return pipeline.run();
  // }


  static PipelineResult executeMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {

    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);
    TableSelector tableSelector = new TableSelector(options.getTables(), ddl, schemaMapper);

    Map<Integer, List<String>> levelToSpannerTableList = tableSelector.levelOrderedSpannerTables();
    Map<String, PCollection<Void>> levelVsOutputMap = new HashMap<>();
    Map<String, Wait.OnSignal<?>> tableCompletionMap = new HashMap<>();
    for (int currentLevel = 0; currentLevel < levelToSpannerTableList.size(); currentLevel++) {
      List<String> spannerTables = levelToSpannerTableList.get(currentLevel);
      LOG.info("processing level: {} spanner tables: {}", currentLevel, spannerTables);
      List<String> sourceTables =
          spannerTables.stream()
              .map(t -> tableSelector.getSchemaMapper().getSourceTableName("", t))
              .collect(Collectors.toList());
      LOG.info("level: {} source tables: {}", currentLevel, spannerTables);
      OnSignal<@UnknownKeyFor @Nullable @Initialized Object> waitOnSignal =
          currentLevel > 0 ? Wait.on(levelVsOutputMap.get(currentLevel - 1)) : null;
      JdbcIoWrapper jdbcIoWrapper =
          JdbcIoWrapper.of(
              OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
                  options, sourceTables, null, waitOnSignal));
      if (jdbcIoWrapper.getTableReaders().isEmpty()) {
        LOG.info("not creating reader as tables are not found at source: {}", sourceTables);
        //If tables of 1 level are ignored in middle, then the subsequent level will not wait to
        //begin processing.
        continue;
      }
      ReaderImpl reader = ReaderImpl.of(jdbcIoWrapper);
      String suffix = generateSuffix("", currentLevel + "");
      String shardIdColumn = "";
      PCollection<Void> output =
          pipeline.apply(
              "Migrate" + suffix,
              new MigrateTableTransform(options, spannerConfig, tableSelector.getDdl(),
                  tableSelector.getSchemaMapper(), reader, "", shardIdColumn));
      levelVsOutputMap.put(currentLevel + "", output);

      for (String srcTable : sourceTables) {
        tableCompletionMap.put(srcTable, Wait.on(output));
      }
    }

    // Add transform to increment table counter
    pipeline.apply("Increment_table_counters", new IncrementTableCounter(tableCompletionMap, ""));

    return pipeline.run();
  }

  @NotNull
  /**
   * This list will contain the final list of tables that actually get migrated, which will be the
   * intersection of Spanner and source tables.
   */
  static List<String> listSpannerTablesToMigrate(
      Ddl ddl, ISchemaMapper schemaMapper, Set<String> tablesToMigrateSet) {
    // This list is all Spanner tables topologically ordered.
    List<String> orderedSpTables = ddl.getTablesOrderedByReference();

    // This list will contain the final list of tables that actually get migrated, which will be the
    // intersection of Spanner and source tables.
    List<String> finalTablesToMigrate = new ArrayList<>();
    for (String spTable : orderedSpTables) {
      try {
        String srcTable = schemaMapper.getSourceTableName("", spTable);
        if (!tablesToMigrateSet.contains(srcTable)) {
          LOG.info("ignoring table as no source maps to this spanner table: {}", spTable);
          continue;
        }
        finalTablesToMigrate.add(spTable);
      } catch (NoSuchElementException e) {
        LOG.info("ignoring table not identified by schema mapper: {}", spTable);
      }
    }
    LOG.info(
        "{} Spanner tables in final selection for migration: {}",
        finalTablesToMigrate.size(),
        finalTablesToMigrate);
    if (finalTablesToMigrate.size() > MAX_RECOMMENDED_TABLES_PER_JOB) {
      LOG.warn(
          "Migrating {} tables in a single job (max recommended: {}). Consider splitting tables across jobs to avoid launch issues.",
          finalTablesToMigrate.size(),
          MAX_RECOMMENDED_TABLES_PER_JOB);
    }
    return finalTablesToMigrate;
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

    SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(options, ddl);

    List<String> srcTablesToMigrate = listTablesToMigrate(options.getTables(), schemaMapper, ddl);
    Set<String> tablesToMigrateSet = new HashSet<>(srcTablesToMigrate);

    List<String> spannerTablesToMigrate =
        listSpannerTablesToMigrate(ddl, schemaMapper, tablesToMigrateSet);

    LOG.info(
        "running migration for shards: {}",
        shards.stream().map(Shard::getHost).collect(Collectors.toList()));
    for (Shard shard : shards) {
      for (Map.Entry<String, String> entry : shard.getDbNameToLogicalShardIdMap().entrySet()) {
        // Read data from source
        String shardId = entry.getValue();
        Map<String, PCollection<Void>> outputs = new HashMap<>();
        for (String spTable : spannerTablesToMigrate) {
          String srcTable = schemaMapper.getSourceTableName("", spTable);
          List<PCollection<?>> parentOutputs = new ArrayList<>();
          for (String parentSpTable : ddl.tablesReferenced(spTable)) {
            String parentSrcName;
            try {
              parentSrcName = schemaMapper.getSourceTableName("", parentSpTable);
            } catch (NoSuchElementException e) {
              // This will occur when the spanner table name does not exist in source for
              // sessionBasedMapper.
              continue;
            }
            // This parent is not in tables selected for migration.
            if (!tablesToMigrateSet.contains(parentSrcName)) {
              continue;
            }
            PCollection<Void> parentOutputPcollection = outputs.get(parentSrcName);
            // Since we are iterating the tables topologically, all parents should have been
            // processed.
            Preconditions.checkState(
                parentOutputPcollection != null,
                "Output PCollection for parent table should not be null.");
            parentOutputs.add(parentOutputPcollection);
          }
          // If a namespace is configured for a shard uses that, otherwise uses the namespace
          // configured in the options if there is one.
          String namespace =
              Optional.ofNullable(shard.getNamespace()).orElse(options.getNamespace());
          JdbcIoWrapper jdbcIoWrapper =
              JdbcIoWrapper.of(
                  OptionsToConfigBuilder.getJdbcIOWrapperConfig(
                      sqlDialect,
                      List.of(srcTable),
                      null,
                      shard.getHost(),
                      shard.getConnectionProperties(),
                      Integer.parseInt(shard.getPort()),
                      shard.getUserName(),
                      shard.getPassword(),
                      entry.getKey(),
                      namespace,
                      shardId,
                      options.getJdbcDriverClassName(),
                      options.getJdbcDriverJars(),
                      options.getMaxConnections(),
                      options.getNumPartitions(),
                      Wait.on(parentOutputs)));
          if (jdbcIoWrapper.getTableReaders().isEmpty()) {
            LOG.info(
                "not creating reader as table is not found at source: {} shard: {}",
                srcTable,
                shard.getLogicalShardId());
            continue;
          }
          ReaderImpl reader = ReaderImpl.of(jdbcIoWrapper);
          String suffix = generateSuffix(shardId, srcTable);
          String shardIdColumn =
              schemaMapper.getShardIdColumnName(
                  reader.getSourceSchema().schemaReference().namespace(), srcTable);
          PCollection<Void> output =
              pipeline.apply(
                  "Migrate" + suffix,
                  new MigrateTableTransform(
                      options, spannerConfig, ddl, schemaMapper, reader, shardId, shardIdColumn));
          outputs.put(srcTable, output);
        }
        // Add transform to increment table counter
        Map<String, Wait.OnSignal<?>> waitOnsMap =
            outputs.entrySet().stream()
                .collect(
                    Collectors.toMap(Map.Entry::getKey, mapEntry -> Wait.on(mapEntry.getValue())));
        pipeline.apply(
            "Increment_table_counters_" + shardId, new IncrementTableCounter(waitOnsMap, shardId));
      }
    }
    return pipeline.run();
  }

  // Calculate the total number of logical shards in the list of physical shards.
  private static long findNumLogicalshards(List<Shard> shards) {
    return shards.stream().mapToLong(shard -> shard.getDbNameToLogicalShardIdMap().size()).sum();
  }

  @VisibleForTesting
  static SpannerConfig createSpannerConfig(SourceDbToSpannerOptions options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
        .withRpcPriority(RpcPriority.HIGH);
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
            ? Arrays.stream(tableList.split("\\:|,")).collect(Collectors.toList())
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
        if (!tablesToMigrate.contains(srcTable)) {
          tablesToMigrate.add(srcTable);
        }
      }
    }

    if (tablesToMigrate.isEmpty()) {
      LOG.error("aborting migration as no tables found to migrate");
      throw new InvalidOptionsException("no configured tables can be migrated");
    }
    return tablesToMigrate;
  }

  static class TableSelector {

    private String configuredTableList;
    private Ddl ddl;
    private ISchemaMapper schemaMapper;
    private List<String> srcTablesToMigrate;
    private List<String> spTablesToMigrate;

    public TableSelector(String configuredTableList, Ddl ddl, ISchemaMapper schemaMapper) {
      this.configuredTableList = configuredTableList;
      this.ddl = ddl;
      this.schemaMapper = schemaMapper;
      init();
    }

    private void init() {
      srcTablesToMigrate = listTablesToMigrate(configuredTableList, schemaMapper, ddl);
      spTablesToMigrate =
          listSpannerTablesToMigrate(ddl, schemaMapper, new HashSet<>(srcTablesToMigrate));

      for (String spTable : spTablesToMigrate) {
        try {
          checkTableConfigIssues(spTable);
        } catch (RuntimeException e) {
          LOG.warn(e.getMessage());
        }
      }
    }

    public Ddl getDdl() {
      return ddl;
    }

    public ISchemaMapper getSchemaMapper() {
      return schemaMapper;
    }

    public List<String> getSrcTablesToMigrate() {
      return srcTablesToMigrate;
    }

    public List<String> getSpTablesToMigrate() {
      return spTablesToMigrate;
    }

    public Map<Integer, List<String>> levelOrderedSpannerTables() {
      Map<Integer, List<String>> levelOrderedTables = new HashMap<>();
      // This does not handle scenarios where tables are split into multiple tables.
      // In that case if the level of tables is different this logic may not work
      // The logic will need to change to create source levels based on Spanner tables

      List<String> tablesToProcess = new LinkedList<>(spTablesToMigrate);
      Map<String, List<String>> referencedTablesMap =
          spTablesToMigrate.stream()
              .collect(Collectors.toMap(e -> e, e -> ddl.getAllReferencedTables(e)));

      List<String> allTablesAddedToLevels = new ArrayList<>();
      // 1. Identify tables without references - add to level 0 and remove from list
      // 2. Identify tables which have been processed in above levels and add to current level
      // Repeat until no tables are left to process
      // In the worst case - there will be 1 table per level, so this loop should not execute more
      // times than the number of tables.
      int currentLevel = 0;
      while (!tablesToProcess.isEmpty()) {
        if (currentLevel > spTablesToMigrate.size()) {
          throw new RuntimeException(
              "too many iterations while creating level ordered tables:" + currentLevel);
        }
        List<String> currentLevelTables = new ArrayList<>();
        ListIterator<String> iter = tablesToProcess.listIterator();
        while (iter.hasNext()) {
          String currentTableName = iter.next();
          List<String> referencedTables = referencedTablesMap.get(currentTableName);
          if (referencedTables.isEmpty() || allTablesAddedToLevels.containsAll(referencedTables)) {
            LOG.debug("all referenced tables are marked for processing: {}", currentTableName);
            currentLevelTables.add(currentTableName);
            iter.remove();
          }
        }
        levelOrderedTables.put(currentLevel, currentLevelTables);
        allTablesAddedToLevels.addAll(currentLevelTables);
        currentLevel++;
      }
      return levelOrderedTables;
    }

    private void checkTableConfigIssues(String spTable) {
      for (String parentSpTable : ddl.tablesReferenced(spTable)) {
        try {
          String parentSrcName = schemaMapper.getSourceTableName("", parentSpTable);

          // This parent is not in tables selected for migration.
          if (!srcTablesToMigrate.contains(parentSrcName)) {
            throw new RuntimeException(
                spTable
                    + " references table "
                    + parentSpTable
                    + " which is not selected for migration (Provide the source table name "
                    + parentSrcName
                    + " via the 'tables' option if this is a mistake!). Writes to "
                    + spTable
                    + " could fail, check DLQ for failed records.");
          }
        } catch (NoSuchElementException e) {
          // This will occur when the spanner table name does not exist in source for
          // sessionBasedMapper.
          throw new RuntimeException(
              spTable
                  + " references table "
                  + parentSpTable
                  + " which does not have an equivalent source table. Writes to "
                  + spTable
                  + " could fail, check DLQ for failed records.");
        }
      }
    }
  }


}
