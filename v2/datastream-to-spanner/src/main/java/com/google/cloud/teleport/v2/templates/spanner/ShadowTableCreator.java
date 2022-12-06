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

import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Column;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Table;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/** Helper class to create shadow tables for different source types. */
class ShadowTableCreator {

  private final String sourceType;
  private final String shadowTablePrefix;
  private final Map<String, Pair<String, String>> sortOrderMap;

  ShadowTableCreator(String sourceType, String shadowTablePrefix) {
    this.sourceType = sourceType;
    this.shadowTablePrefix = shadowTablePrefix;
    if (DatastreamConstants.ORACLE_SOURCE_TYPE.equals(sourceType)) {
      sortOrderMap = DatastreamConstants.ORACLE_SORT_ORDER;
    } else if (DatastreamConstants.MYSQL_SOURCE_TYPE.equals(sourceType)) {
      sortOrderMap = DatastreamConstants.MYSQL_SORT_ORDER;
    } else if (DatastreamConstants.POSTGRES_SOURCE_TYPE.equals(sourceType)) {
      sortOrderMap = DatastreamConstants.POSTGRES_SORT_ORDER;
    } else {
      sortOrderMap = null;
      throw new IllegalArgumentException(
          "Unsupported datastream source type specified: " + sourceType);
    }
  }

  /*
   * Constructs a shadow table for a data table in the information schema.
   * Note: Shadow tables for interleaved tables are not interleaved to
   * their shadow parent table.
   */
  Table constructShadowTable(Ddl informationSchema, String dataTableName) {

    // Create a new shadow table with the given prefix.
    Table.Builder shadowTableBuilder = Table.builder();
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
    addChangeEventSequenceColumns(shadowTableBuilder);

    return shadowTableBuilder.build();
  }

  private void addChangeEventSequenceColumns(Table.Builder shadowTableBuilder) {
    for (Pair<String, String> shadowInfo : sortOrderMap.values()) {
      Column.Builder versionColumnBuilder = shadowTableBuilder.column(shadowInfo.getLeft());
      versionColumnBuilder.parseType(shadowInfo.getRight());
      versionColumnBuilder.endColumn();
    }
  }
}
