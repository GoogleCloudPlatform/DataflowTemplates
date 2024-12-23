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
package com.google.cloud.teleport.v2.spanner.migrations.metadata;

import autovalue.shaded.com.google.common.collect.ImmutableList;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceColumn;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceTable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraSourceMetadata {

  private static final String SCHEMA_NAME = "cassandra"; // Constant for schema name

  /**
   * Generates a SourceSchema from a ResultSet.
   *
   * @param resultSet The ResultSet containing schema information.
   * @return A SourceSchema instance.
   */
  private static SourceSchema generateSourceSchema(ResultSet resultSet) {
    Map<String, Map<String, SourceColumn>> schema = new HashMap<>();

    resultSet.forEach(
        row -> {
          String tableName = row.getString("table_name");
          String columnName = row.getString("column_name");
          String dataType = row.getString("type");
          String kind = row.getString("kind");

          boolean isPrimaryKey = isPrimaryKey(kind);
          SourceColumn sourceColumn = SourceColumn.create(columnName, kind, dataType, isPrimaryKey);

          schema.computeIfAbsent(tableName, k -> new HashMap<>()).put(columnName, sourceColumn);
        });

    Map<String, SourceTable> tables =
        schema.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        SourceTable.create(
                            entry.getKey(), ImmutableList.copyOf(entry.getValue().values()))));

    return SourceSchema.create(Map.copyOf(tables));
  }

  /**
   * Converts a ResultSet to a Schema object, updating the provided schema.
   *
   * @param schema The schema to update.
   * @param resultSet The ResultSet containing schema information.
   */
  public static void generateSourceSchema(Schema schema, ResultSet resultSet) {
    SourceSchema sourceSchema = generateSourceSchema(resultSet);
    Map<String, com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable> sourceTableMap =
        convertSourceSchemaToMap(sourceSchema);
    schema.setSrcSchema(sourceTableMap);
    schema.setToSource(convertSourceToNameAndColsTable(sourceSchema.tables().values()));
  }

  /**
   * Converts a SourceSchema to a map of Spanner table names to SourceTable objects.
   *
   * @param sourceSchema The SourceSchema to convert.
   * @return A map where the key is the table name and the value is the corresponding SourceTable.
   */
  private static Map<String, com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable>
      convertSourceSchemaToMap(SourceSchema sourceSchema) {
    return sourceSchema.tables().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> convertSourceTableToSourceTable(entry.getValue())));
  }

  /**
   * Converts a SourceTable to a SourceTable for Spanner.
   *
   * @param sourceTable The SourceTable to convert.
   * @return A converted SourceTable object.
   */
  private static com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable
      convertSourceTableToSourceTable(SourceTable sourceTable) {
    List<SourceColumn> columns = sourceTable.columns();

    String[] colIds = columns.stream().map(SourceColumn::name).toArray(String[]::new);

    Map<String, SourceColumnDefinition> colDefs =
        columns.stream()
            .collect(
                Collectors.toMap(
                    SourceColumn::name,
                    col ->
                        new SourceColumnDefinition(
                            col.name(),
                            new SourceColumnType(col.sourceType(), new Long[0], new Long[0]))));

    ColumnPK[] primaryKeys =
        columns.stream()
            .filter(SourceColumn::isPrimaryKey)
            .map(col -> new ColumnPK(col.name(), getPrimaryKeyOrder(col)))
            .toArray(ColumnPK[]::new);

    return new com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable(
        sourceTable.name(),
        SCHEMA_NAME, // Use constant schema name
        colIds,
        colDefs,
        primaryKeys);
  }

  /**
   * Determines if a column is a primary key.
   *
   * @param kind The column kind.
   * @return true if the column is a primary key, false otherwise.
   */
  private static boolean isPrimaryKey(String kind) {
    return "partition_key".equals(kind) || "clustering".equals(kind);
  }

  /**
   * Gets the order of the primary key column.
   *
   * @param col The SourceColumn.
   * @return The order of the primary key.
   */
  private static int getPrimaryKeyOrder(SourceColumn col) {
    switch (col.kind()) {
      case "partition_key":
        return 1;
      case "clustering":
        return 2;
      default:
        return 0;
    }
  }

  /**
   * Converts a collection of SourceTables to a map of table names to NameAndCols.
   *
   * @param tables A collection of SourceTables.
   * @return A map where the key is the table name and the value is a NameAndCols object.
   */
  private static Map<String, NameAndCols> convertSourceToNameAndColsTable(
      Collection<SourceTable> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                SourceTable::name, CassandraSourceMetadata::convertSourceTableToNameAndCols));
  }

  /**
   * Converts a SourceTable to a NameAndCols object.
   *
   * @param sourceTable The SourceTable to convert.
   * @return A NameAndCols object representing the table and column names.
   */
  private static NameAndCols convertSourceTableToNameAndCols(SourceTable sourceTable) {
    Map<String, String> columnNames =
        sourceTable.columns().stream()
            .collect(Collectors.toMap(SourceColumn::name, SourceColumn::name));

    return new NameAndCols(sourceTable.name(), columnNames);
  }
}
