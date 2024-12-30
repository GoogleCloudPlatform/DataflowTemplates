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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@code CassandraSourceMetadata} class is responsible for generating metadata from a Cassandra
 * schema using a {@link ResultSet}, converting it into a Spanner-compatible format, and managing
 * this metadata to facilitate schema migration operations.
 *
 * <p>This class supports the following functionalities:
 *
 * <ul>
 *   <li>Extracting table, column, and primary key information from Cassandra's schema.
 *   <li>Converting Cassandra schema details into Spanner-compatible objects like {@link
 *       SourceTable}.
 *   <li>Updating a provided {@link Schema} instance with the extracted metadata.
 * </ul>
 *
 * <p>The metadata extraction process uses the {@link ResultSet} containing the schema details from
 * Cassandra, such as table names, column definitions, data types, and primary key details.
 *
 * <p><strong>Note:</strong> This class does not perform direct database interactions; it relies on
 * a pre-populated {@link ResultSet}.
 */
public class CassandraSourceMetadata {

  private final Schema schema;
  private final ResultSet resultSet;

  /**
   * Private constructor to initialize {@link CassandraSourceMetadata}.
   *
   * @param resultSet The {@link ResultSet} containing Cassandra schema metadata. Cannot be null.
   * @param schema The {@link Schema} instance to update with metadata. Cannot be null.
   */
  private CassandraSourceMetadata(ResultSet resultSet, Schema schema) {
    this.resultSet = Objects.requireNonNull(resultSet, "ResultSet cannot be null");
    this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
  }

  /**
   * Generates a map of table names to {@link SourceTable} objects, representing the schema of the
   * Cassandra source in a Spanner-compatible format.
   *
   * @return A map where keys are table names and values are {@link SourceTable} objects containing
   *     schema details.
   */
  public Map<String, SourceTable> generateSourceSchema() {
    Map<String, Map<String, SourceColumnDefinition>> colDefinitions = new HashMap<>();
    Map<String, List<ColumnPK>> columnPKs = new HashMap<>();
    Map<String, List<String>> columnIds = new HashMap<>();
    Set<String> tableNames = new HashSet<>();

    resultSet.forEach(
        row -> {
          String tableName = row.getString("table_name");
          String columnName = row.getString("column_name");
          String dataType = row.getString("type");
          String kind = row.getString("kind");

          tableNames.add(tableName);

          colDefinitions
              .computeIfAbsent(tableName, k -> new HashMap<>())
              .put(
                  columnName,
                  new SourceColumnDefinition(
                      columnName, new SourceColumnType(dataType, new Long[0], new Long[0])));

          if (isPrimaryKey(kind)) {
            columnPKs
                .computeIfAbsent(tableName, k -> new ArrayList<>())
                .add(new ColumnPK(columnName, getPrimaryKeyOrder(kind)));
          }

          columnIds.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
        });

    return tableNames.stream()
        .collect(
            Collectors.toMap(
                tableName -> tableName,
                tableName ->
                    new SourceTable(
                        tableName,
                        null,
                        columnIds.getOrDefault(tableName, List.of()).toArray(new String[0]),
                        colDefinitions.getOrDefault(tableName, Map.of()),
                        columnPKs.getOrDefault(tableName, List.of()).toArray(new ColumnPK[0]))));
  }

  /**
   * Updates the provided {@link Schema} with metadata generated from the Cassandra {@link
   * ResultSet}.
   *
   * <p>This method extracts schema details, transforms them into Spanner-compatible objects, and
   * sets the corresponding properties in the provided {@link Schema}.
   */
  public void generateAndSetSourceSchema() {
    Map<String, SourceTable> sourceTableMap = generateSourceSchema();
    schema.setSrcSchema(sourceTableMap);
    schema.setToSource(convertSourceToNameAndColsTable(sourceTableMap.values()));
  }

  /**
   * Determines whether a column is part of the primary key based on its kind.
   *
   * @param kind The column kind, such as "partition_key" or "clustering".
   * @return {@code true} if the column is a primary key; {@code false} otherwise.
   */
  private boolean isPrimaryKey(String kind) {
    return "partition_key".equals(kind) || "clustering".equals(kind);
  }

  /**
   * Determines the primary key order based on its kind.
   *
   * @param kind The kind of primary key (e.g., "partition_key" or "clustering").
   * @return An integer indicating the order of the key (e.g., 1 for "partition_key").
   */
  private int getPrimaryKeyOrder(String kind) {
    switch (kind) {
      case "partition_key":
        return 1;
      case "clustering":
        return 2;
      default:
        return 0;
    }
  }

  /**
   * Converts a collection of {@link SourceTable} objects into a map of table names to {@link
   * NameAndCols}.
   *
   * @param tables A collection of {@link SourceTable} objects representing the Cassandra schema.
   * @return A map where keys are table names and values are {@link NameAndCols}.
   */
  private Map<String, NameAndCols> convertSourceToNameAndColsTable(Collection<SourceTable> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                SourceTable::getName, CassandraSourceMetadata::convertSourceTableToNameAndCols));
  }

  /**
   * Converts a single {@link SourceTable} into a {@link NameAndCols} instance.
   *
   * @param sourceTable The {@link SourceTable} to convert.
   * @return A {@link NameAndCols} object containing the table name and column names.
   */
  private static NameAndCols convertSourceTableToNameAndCols(SourceTable sourceTable) {
    Map<String, String> columnNames =
        sourceTable.getColDefs().values().stream()
            .collect(
                Collectors.toMap(SourceColumnDefinition::getName, SourceColumnDefinition::getName));

    return new NameAndCols(sourceTable.getName(), columnNames);
  }

  /**
   * Builder class for creating instances of {@link CassandraSourceMetadata}.
   *
   * <p>The builder allows for incremental configuration of the {@link ResultSet} and {@link Schema}
   * before constructing the final {@link CassandraSourceMetadata} instance.
   */
  public static class Builder {
    private ResultSet resultSet;
    private Schema schema;

    /**
     * Sets the {@link Schema} for the builder.
     *
     * @param schema The {@link Schema} to be used in the metadata generation.
     * @return The current {@link Builder} instance.
     */
    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Sets the {@link ResultSet} for the builder.
     *
     * @param resultSet The {@link ResultSet} containing Cassandra schema information.
     * @return The current {@link Builder} instance.
     */
    public Builder setResultSet(ResultSet resultSet) {
      this.resultSet = resultSet;
      return this;
    }

    /**
     * Builds an instance of {@link CassandraSourceMetadata}, generating and setting the schema
     * metadata.
     *
     * @return A fully constructed {@link CassandraSourceMetadata} instance.
     */
    public CassandraSourceMetadata build() {
      CassandraSourceMetadata cassandraSourceMetadata =
          new CassandraSourceMetadata(resultSet, schema);
      cassandraSourceMetadata.generateAndSetSourceSchema();
      return cassandraSourceMetadata;
    }
  }
}
