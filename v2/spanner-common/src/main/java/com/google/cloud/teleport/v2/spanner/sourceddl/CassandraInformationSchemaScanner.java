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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scanner for reading Cassandra database schema information. Implements
 * SourceInformationSchemaScanner to provide schema scanning functionality for Cassandra.
 */
public class CassandraInformationSchemaScanner implements SourceInformationSchemaScanner {
  private static final Logger LOG =
      LoggerFactory.getLogger(CassandraInformationSchemaScanner.class);

  private final CqlSession session;
  private final String keyspaceName;

  public CassandraInformationSchemaScanner(CqlSession session, String keyspaceName) {
    this.session = session;
    this.keyspaceName = keyspaceName;
  }

  @Override
  public SourceSchema scan() {
    SourceSchema.Builder schemaBuilder =
        SourceSchema.builder(SourceDatabaseType.CASSANDRA).databaseName(keyspaceName);

    try {
      Map<String, SourceTable> tables = scanTables();
      schemaBuilder.tables(ImmutableMap.copyOf(tables));
    } catch (Exception e) {
      throw new RuntimeException("Error scanning Cassandra schema", e);
    }

    return schemaBuilder.build();
  }

  private Map<String, SourceTable> scanTables() throws Exception {
    Map<String, SourceTable> tables = new HashMap<>();

    // Query to get all tables in the keyspace
    String tableQuery = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    PreparedStatement tableStmt = session.prepare(tableQuery);
    BoundStatement tableBoundStmt = tableStmt.bind(keyspaceName);
    ResultSet tableResult = session.execute(tableBoundStmt);

    // For each table, get its columns
    while (tableResult.iterator().hasNext()) {
      String tableName = tableResult.iterator().next().getString("table_name");
      tables.put(tableName, scanTableColumns(tableName));
    }

    return tables;
  }

  private SourceTable scanTableColumns(String tableName) throws Exception {
    // Query to get column information for the table
    String columnQuery =
        "SELECT column_name, type, kind FROM system_schema.columns "
            + "WHERE keyspace_name = ? AND table_name = ?";
    PreparedStatement columnStmt = session.prepare(columnQuery);
    BoundStatement columnBoundStmt = columnStmt.bind(keyspaceName, tableName);
    ResultSet columnResult = session.execute(columnBoundStmt);

    List<SourceColumn> columns = new ArrayList<>();
    List<String> primaryKeyColumns = new ArrayList<>();

    while (columnResult.iterator().hasNext()) {
      var row = columnResult.iterator().next();
      String columnName = row.getString("column_name");
      String type = row.getString("type");
      String kind = row.getString("kind");

      // Create SourceColumn with appropriate type information
      SourceColumn.Builder columnBuilder =
          SourceColumn.builder(SourceDatabaseType.CASSANDRA)
              .name(columnName)
              .type(type.toUpperCase().replaceAll("\\s+", "")) // Normalize type name
              .isNullable(true) // Cassandra columns are nullable by default
              .isPrimaryKey("partition_key".equals(kind) || "clustering".equals(kind));

      if ("partition_key".equals(kind) || "clustering".equals(kind)) {
        primaryKeyColumns.add(columnName);
      }

      columns.add(columnBuilder.build());
    }

    return SourceTable.builder(SourceDatabaseType.CASSANDRA)
        .name(tableName)
        .schema(keyspaceName)
        .columns(ImmutableList.copyOf(columns))
        .primaryKeyColumns(ImmutableList.copyOf(primaryKeyColumns))
        .build();
  }
}
