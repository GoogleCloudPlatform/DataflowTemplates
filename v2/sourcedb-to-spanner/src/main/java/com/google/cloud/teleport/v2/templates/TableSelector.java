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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle all logic for selection and ordering of tables. TODO- Check if class can be moved
 * to Spanner common along with schema mapper.
 */
public class TableSelector {

  private static final Logger LOG = LoggerFactory.getLogger(TableSelector.class);
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
    spTablesToMigrate = listSpannerTablesToMigrate(ddl, schemaMapper);

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
            .collect(Collectors.toMap(t -> t, t -> ddl.getAllReferencedTables(t)));

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
      LOG.info(
          "level based tables generated. level: {} tables: {}", currentLevel, currentLevelTables);
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

  @NotNull
  /**
   * This list will contain the final list of tables that actually get migrated, which will be the
   * intersection of Spanner and source tables.
   */
  private List<String> listSpannerTablesToMigrate(Ddl ddl, ISchemaMapper schemaMapper) {
    Set<String> tablesToMigrateSet = new HashSet<>(srcTablesToMigrate);
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
    return finalTablesToMigrate;
  }

  /*
   * Return the available tables to migrate based on the following.
   * 1. Fetch tables from schema mapper. Override with tables from options if present
   * 2. Mark for migration if tables have corresponding spanner tables.
   * Err on the side of being lenient with configuration
   */
  private List<String> listTablesToMigrate(String tableList, ISchemaMapper mapper, Ddl ddl) {
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
}
