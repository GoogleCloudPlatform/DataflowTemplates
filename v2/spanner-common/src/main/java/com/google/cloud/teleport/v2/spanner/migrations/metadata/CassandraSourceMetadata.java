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
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceColumn;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra.SourceTable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@code CassandraSourceMetadata} class is responsible for extracting metadata from a Cassandra
 * schema based on CQL (Cassandra Query Language) session information, transforming it into a format
 * that is compatible with Spanner, and providing methods for converting and managing this metadata.
 * This transformation is crucial for facilitating the migration of data between Spanner and
 * Cassandra, ensuring that the schema structure, including tables, columns, data types, and primary
 * key information, is accurately mapped and usable for data migration operations.
 *
 * <p>This class leverages the ResultSet obtained from executing CQL queries to gather the necessary
 * schema information from Cassandra, such as table names, column names, column data types, and
 * whether columns are primary keys. The schema is then transformed into a format suitable for
 * Spanner, taking into account the different structures and conventions between the two systems.
 *
 * <p>The main tasks performed by this class include:
 *
 * <ul>
 *   <li>Extracting Cassandra schema details from CQL ResultSet, such as tables, columns, and
 *       primary key definitions.
 *   <li>Converting this schema into a Spanner-compatible structure, including column definitions
 *       and primary key handling.
 *   <li>Providing helper methods to convert the schema into a format that can be integrated into
 *       Spanner's migration process.
 * </ul>
 *
 * This class serves as a key component in the data migration pipeline for moving data from
 * Cassandra to Spanner, particularly in cases where schema conversion and data type mapping are
 * required as part of the migration process.
 *
 * <p>It is important to note that this class does not interact with the actual Cassandra database
 * but instead relies on CQL ResultSet data that is passed to it, enabling it to operate in a
 * decoupled manner and simplifying integration into the migration flow.
 */
public class CassandraSourceMetadata {

  private final Schema schema;
  private final ResultSet resultSet;

  private CassandraSourceMetadata(ResultSet resultSet, Schema schema) {
    this.resultSet = resultSet;
    this.schema = schema;
  }

  /**
   * Generates a {@link SourceSchema} from a Cassandra {@link ResultSet}.
   *
   * @return A {@link SourceSchema} instance representing the schema of the Cassandra source.
   */
  public SourceSchema generateSourceSchema() {
    Map<String, Map<String, SourceColumn>> schemaMap = new HashMap<>();

    resultSet.forEach(
        row -> {
          String tableName = row.getString("table_name");
          String columnName = row.getString("column_name");
          String dataType = row.getString("type");
          String kind = row.getString("kind");

          boolean isPrimaryKey = isPrimaryKey(kind);
          SourceColumn sourceColumn = SourceColumn.create(columnName, kind, dataType, isPrimaryKey);

          schemaMap.computeIfAbsent(tableName, k -> new HashMap<>()).put(columnName, sourceColumn);
        });

    Map<String, SourceTable> tables =
        schemaMap.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        SourceTable.create(
                            entry.getKey(), ImmutableList.copyOf(entry.getValue().values()))));

    return SourceSchema.create(Map.copyOf(tables));
  }

  /**
   * Converts a {@link ResultSet} to a {@link Schema} object, updating the provided schema with the
   * transformed Cassandra schema.
   */
  public void generateAndSetSourceSchema() {
    SourceSchema sourceSchema = generateSourceSchema();
    Map<String, com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable> sourceTableMap =
        convertSourceSchemaToMap(sourceSchema);
    schema.setSrcSchema(sourceTableMap);
    schema.setToSource(convertSourceToNameAndColsTable(sourceSchema.tables().values()));
  }

  /**
   * Converts a {@link SourceSchema} to a map of Spanner table names to {@link SourceTable} objects.
   *
   * @param sourceSchema The SourceSchema to convert.
   * @return A map where the key is the table name and the value is the corresponding {@link
   *     SourceTable}.
   */
  private Map<String, com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable>
      convertSourceSchemaToMap(SourceSchema sourceSchema) {
    return sourceSchema.tables().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> convertSourceTableToSourceTable(entry.getValue())));
  }

  /**
   * Converts a {@link SourceTable} to a {@link SourceTable} for Spanner.
   *
   * @param sourceTable The SourceTable to convert.
   * @return A converted {@link SourceTable} object suitable for Spanner.
   */
  private com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable
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
        sourceTable.name(), null, colIds, colDefs, primaryKeys);
  }

  /**
   * Determines if a column is a primary key based on its kind.
   *
   * @param kind The column kind (e.g., "partition_key" or "clustering").
   * @return true if the column is a primary key, false otherwise.
   */
  private boolean isPrimaryKey(String kind) {
    return "partition_key".equals(kind) || "clustering".equals(kind);
  }

  /**
   * Gets the order of the primary key column.
   *
   * @param col The {@link SourceColumn} representing the primary key column.
   * @return The order of the primary key.
   */
  private int getPrimaryKeyOrder(SourceColumn col) {
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
   * Converts a collection of {@link SourceTable} objects to a map of table names to {@link
   * NameAndCols}.
   *
   * @param tables A collection of {@link SourceTable} objects.
   * @return A map where the key is the table name and the value is a {@link NameAndCols} object.
   */
  private Map<String, NameAndCols> convertSourceToNameAndColsTable(Collection<SourceTable> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                SourceTable::name, CassandraSourceMetadata::convertSourceTableToNameAndCols));
  }

  /**
   * Converts a {@link SourceTable} to a {@link NameAndCols} object.
   *
   * @param sourceTable The {@link SourceTable} to convert.
   * @return A {@link NameAndCols} object representing the table and its column names.
   */
  private static NameAndCols convertSourceTableToNameAndCols(SourceTable sourceTable) {
    Map<String, String> columnNames =
        sourceTable.columns().stream()
            .collect(Collectors.toMap(SourceColumn::name, SourceColumn::name));

    return new NameAndCols(sourceTable.name(), columnNames);
  }

  /** Builder class for {@link CassandraSourceMetadata}. */
  public static class Builder {
    private ResultSet resultSet;
    private Schema schema;

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setResultSet(ResultSet resultSet) {
      this.resultSet = resultSet;
      return this;
    }

    public CassandraSourceMetadata build() {
      CassandraSourceMetadata cassandraSourceMetadata =
          new CassandraSourceMetadata(resultSet, schema);
      cassandraSourceMetadata.generateAndSetSourceSchema();
      return cassandraSourceMetadata;
    }
  }
}
