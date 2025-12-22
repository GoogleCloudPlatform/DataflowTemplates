/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.constants.DatastreamConstants;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/** Helper class to create shadow tables for different source types. */
public class ShadowTableCreator {

  private final String sourceType;
  private final String shadowTablePrefix;
  private final Map<String, Pair<String, String>> sortOrderMap;

  ShadowTableCreator(String sourceType, String shadowTablePrefix, Dialect dialect) {
    this.sourceType = sourceType;
    this.shadowTablePrefix = shadowTablePrefix;
    Map<String, Map<String, Pair<String, String>>> dialectToSortOrder;
    dialectToSortOrder = DatastreamConstants.DIALECT_TO_SORT_ORDER.get(dialect);
    if (dialectToSortOrder == null) {
      throw new IllegalArgumentException("Unsupported dialect specified: " + dialect);
    }
    sortOrderMap = dialectToSortOrder.get(sourceType);
    if (sortOrderMap == null) {
      throw new IllegalArgumentException(
          "Unsupported datastream source type specified: " + sourceType);
    }
  }

  /*
   * Constructs a shadow table for a data table in the information schema.
   * Note: Shadow tables for interleaved tables are not interleaved to
   * their shadow parent table.
   */
  Table constructShadowTable(Ddl informationSchema, String dataTableName, Dialect dialect) {

    // Create a new shadow table with the given prefix.
    Table.Builder shadowTableBuilder = Table.builder(dialect);
    String shadowTableName = shadowTablePrefix + dataTableName;
    shadowTableBuilder.name(shadowTableName);

    // Add key columns from the data table to the shadow table builder.
    Table dataTable = informationSchema.table(dataTableName);
    Set<String> primaryKeyColNames =
        dataTable.primaryKeys().stream().map(k -> k.name()).collect(Collectors.toSet());
    List<Column> primaryKeyCols =
        dataTable.columns().stream()
            .filter(col -> primaryKeyColNames.contains(col.name()))
            .collect(Collectors.toList());
    for (Column col : primaryKeyCols) {
      shadowTableBuilder.addColumn(col);
    }

    // Add primary key constraints.
    for (IndexColumn keyColumn : dataTable.primaryKeys()) {
      if (keyColumn.order() == IndexColumn.Order.ASC) {
        shadowTableBuilder.primaryKey().asc(keyColumn.name()).end();
      } else if (keyColumn.order() == IndexColumn.Order.DESC) {
        shadowTableBuilder.primaryKey().desc(keyColumn.name()).end();
      }
    }

    // Add extra column to track ChangeEventSequence information
    addChangeEventSequenceColumns(shadowTableBuilder, primaryKeyColNames);

    return shadowTableBuilder.build();
  }

  private void addChangeEventSequenceColumns(
      Table.Builder shadowTableBuilder, Set<String> primaryKeyColNames) {
    for (Pair<String, String> shadowInfo : sortOrderMap.values()) {
      String baseShadowColumnName = shadowInfo.getLeft();
      String finalShadowColumnName =
          getSafeShadowColumnName(baseShadowColumnName, primaryKeyColNames);
      Column.Builder versionColumnBuilder = shadowTableBuilder.column(finalShadowColumnName);
      versionColumnBuilder.parseType(shadowInfo.getRight());
      versionColumnBuilder.endColumn();
    }
  }

  /**
   * Generates a safe column name for a shadow table by checking for collisions with existing column
   * names and iteratively prepending a prefix until a unique name is found.
   *
   * @param baseShadowColumnName the initial desired name for the shadow column
   * @param existingPrimaryKeyColumnNames a set of existing column names in the data table (should
   *     be lowercase)
   * @return a unique and safe column name
   */
  public static String getSafeShadowColumnName(
      String baseShadowColumnName, Set<String> existingPrimaryKeyColumnNames) {
    Set<String> normalizedKeys =
        existingPrimaryKeyColumnNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
    String safeName = baseShadowColumnName;
    while (normalizedKeys.contains(safeName.toLowerCase())) {
      safeName = "shadow_" + safeName;
    }
    return safeName;
  }
}
