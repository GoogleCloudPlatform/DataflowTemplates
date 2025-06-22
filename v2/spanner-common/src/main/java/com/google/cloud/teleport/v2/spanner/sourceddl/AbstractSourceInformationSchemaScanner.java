/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for source information schema scanners. Provides common functionality for
 * scanning database schemas.
 */
public abstract class AbstractSourceInformationSchemaScanner
    implements SourceInformationSchemaScanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSourceInformationSchemaScanner.class);

  protected final Connection connection;
  protected final String databaseName;
  protected final SourceDatabaseType sourceType;

  protected AbstractSourceInformationSchemaScanner(
      Connection connection, String databaseName, SourceDatabaseType sourceType) {
    this.connection = connection;
    this.databaseName = databaseName;
    this.sourceType = sourceType;
  }

  @Override
  public SourceSchema scan() {
    SourceSchema.Builder schemaBuilder =
        SourceSchema.builder(sourceType).databaseName(databaseName);

    try {
      Map<String, SourceTable> tables = scanTables();
      schemaBuilder.tables(ImmutableMap.copyOf(tables));
    } catch (SQLException e) {
      throw new RuntimeException("Error scanning database schema", e);
    }

    return schemaBuilder.build();
  }

  /** Scans all tables in the database. */
  protected Map<String, SourceTable> scanTables() throws SQLException {
    Map<String, SourceTable> tables = new HashMap<>();
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(getTablesQuery())) {
      while (rs.next()) {
        String tableName = rs.getString(1);
        String schema = rs.getString(2);

        SourceTable table = scanTable(tableName, schema);
        tables.put(tableName, table);
      }
    }
    return tables;
  }

  /** Scans a single table's structure. */
  protected SourceTable scanTable(String tableName, String schema) throws SQLException {
    SourceTable.Builder tableBuilder =
        SourceTable.builder(sourceType).name(tableName).schema(schema);

    // Scan columns
    List<SourceColumn> columns = scanColumns(tableName, schema);
    tableBuilder.columns(ImmutableList.copyOf(columns));

    // Scan primary keys
    List<String> primaryKeys = scanPrimaryKeys(tableName, schema);
    tableBuilder.primaryKeyColumns(ImmutableList.copyOf(primaryKeys));

    return tableBuilder.build();
  }

  /** Gets the SQL query to list all tables. */
  protected abstract String getTablesQuery();

  /** Scans columns for a given table. */
  protected abstract List<SourceColumn> scanColumns(String tableName, String schema)
      throws SQLException;

  /** Scans primary keys for a given table. */
  protected abstract List<String> scanPrimaryKeys(String tableName, String schema)
      throws SQLException;
}
