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
package com.google.cloud.teleport.bigtable;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.google.cloud.teleport.bigtable.CassandraKeyUtils.COLUMN_NAME_COLUMN;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates a Cassandra column schema. The column schema is generated via a cqlsh
 * command and is expected to be a string.
 *
 * <p>cqlsh -e "select json * from system_schema.columns where keyspace_name='$CASSANDRA_KEYSPACE'
 * and table_name='$CASSANDRA_TABLE'"
 *
 * <p>This class expects only one table's columns as input.
 *
 * <p>This class is currently used to generate Cassandra writeTime queries.
 */
class CassandraColumnSchema {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnSchema.class);
  protected static final String KEYSPACE_NAME = "keyspace_name";
  protected static final String TABLE_NAME = "table_name";
  protected static final String POSITION = "position";

  private String keyspace = "";
  private String table = "";
  private List<String> primaryKeyColumns;
  private List<String> regularColumns;

  /**
   * Generate Cassandra column schema from a parsed schema string for use in creating a WriteTime
   * query. The schema string format is expected to be the output of the following cqlsh command:
   *
   * <p>cqlsh -e "select json * from system_schema.columns where keyspace_name='$CASSANDRA_KEYSPACE'
   * and table_name='$CASSANDRA_TABLE'"
   *
   * <p>This class expects only one table's columns as input.
   *
   * @param schemaString The parsed Cassandra columns schema string for one table.
   */
  CassandraColumnSchema(String schemaString) {
    primaryKeyColumns = new ArrayList<>();
    regularColumns = new ArrayList<>();

    for (String line : schemaString.split("\\r?\\n")) {
      // The schema file may not be a strict JSON. Skip lines that are not JSON rows.
      if (!(line.contains(KEYSPACE_NAME)
          && line.contains(TABLE_NAME)
          && line.contains(COLUMN_NAME_COLUMN))) {
        continue;
      }
      LOG.debug("Parsing JSON line " + line);

      // Parse and set keyspace and table names.
      JsonObject row = JsonParser.parseString(line).getAsJsonObject();
      String rowKeyspace = row.get(KEYSPACE_NAME).getAsString();
      String rowTable = row.get(TABLE_NAME).getAsString();
      if (keyspace.isEmpty()) {
        keyspace = rowKeyspace;
      } else if (!keyspace.equals(rowKeyspace)) {
        throw new IllegalArgumentException("Cassandra schema contains more than 1 keyspace_name");
      }
      if (table.isEmpty()) {
        table = rowTable;
      } else if (!table.equals(rowTable)) {
        throw new IllegalArgumentException("Cassandra schema contains more than 1 table_name.");
      }

      // Parse primary and regular columns.
      if (!row.has(POSITION) || !row.has(COLUMN_NAME_COLUMN)) {
        throw new IllegalArgumentException("Cassandra schema row has no position or column_name.");
      }
      String rowColumnName = row.get(COLUMN_NAME_COLUMN).getAsString();
      if (row.get(POSITION).getAsInt() > -1) {
        primaryKeyColumns.add(rowColumnName);
      } else {
        regularColumns.add(rowColumnName);
      }

      LOG.debug(
          String.format(
              "Line parsed, current schema class members %s %s %s %s",
              keyspace, table, primaryKeyColumns, regularColumns));
    }
  }

  /**
   * Convenience function to create a writetime query from class members.
   *
   * @return CQL query string for all columns including regular column writetimes.
   */
  public String createWritetimeQuery() {
    return createWritetimeQuery(keyspace, table, primaryKeyColumns, regularColumns);
  }

  /**
   * Create a CQL query that collects all data including regular column writetimes, i.e.:
   *
   * <p>SELECT *, writetime(regularColumn1) writetime(regularColumn2)... from keyspace.table
   *
   * @param keyspace Cassandra keyspace.
   * @param table Cassandra table.
   * @param primaryKeyColumns List of Cassandra table primary key columns.
   * @param regularColumns List of Cassandra table regular columns.
   * @return CQL query string for all columns including regular column writetimes.
   */
  private String createWritetimeQuery(
      String keyspace, String table, List<String> primaryKeyColumns, List<String> regularColumns) {

    Select query = selectFrom(keyspace, table).columns(primaryKeyColumns).columns(regularColumns);

    for (String regularColumn : regularColumns) {
      query = query.writeTime(regularColumn);
    }
    return query.asCql();
  }
}
