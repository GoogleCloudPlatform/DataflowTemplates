/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.it.common.ResourceManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/** Interface for managing JDBC resources in integration tests. */
public interface JDBCResourceManager extends ResourceManager {

  /** Returns the URI connection string to the JDBC Database. */
  String getUri();

  /**
   * Returns the username used to log in to the JDBC database.
   *
   * @return the database username.
   */
  String getUsername();

  /**
   * Returns the password used to log in to the JDBC database.
   *
   * @return the database password.
   */
  String getPassword();

  /**
   * Returns the name of the Database that this JDBC manager will operate in.
   *
   * @return the name of the JDBC Database.
   */
  String getDatabaseName();

  /**
   * Creates a table within the current database given a table name and JDBC schema.
   *
   * @param tableName The name of the table.
   * @param schema A JDBCSchema object that defines the table.
   * @return A boolean indicating whether the resource was created.
   * @throws JDBCResourceManagerException if there is an error creating the table.
   */
  boolean createTable(String tableName, JDBCSchema schema);

  /**
   * Writes the given mapped rows into the specified columns. This method requires {@link
   * JDBCResourceManager#createTable(String, JDBCSchema)} to be called for the target table
   * beforehand.
   *
   * <p>The maps in the rows list must use the column name as the key. i.e. [{col1: val1, col2:
   * val2, ...}, {col1: val3, col2: val4, ...}, ...]
   *
   * @param tableName The name of the table to insert the given rows into.
   * @param rows A list of maps representing the rows to be inserted into the table.
   * @throws JDBCResourceManagerException if method is called after resources have been cleaned up,
   *     if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  boolean write(String tableName, List<Map<String, Object>> rows);

  /**
   * Reads all the rows in a table and returns in the format of a list of Maps, which contain all
   * the columns (including ID).
   *
   * @param tableName The name of the table to read rows from.
   * @return a list containing the table rows.
   */
  List<Map<String, Object>> readTable(String tableName);

  /**
   * Returns the schema of the given table as a list of strings.
   *
   * @param tableName the name of the table to fetch the schema of.
   * @return the list of column names.
   */
  List<String> getTableSchema(String tableName);

  /**
   * Run the given SQL query.
   *
   * @param sql The SQL query to run.
   * @return A ResultSet containing the result of the execution.
   */
  ResultSet runSQLQuery(String sql);

  /**
   * Run the given SQL DML statement (INSERT, UPDATE and DELETE).
   *
   * @param sql The SQL DML statement to run.
   */
  void runSQLUpdate(String sql);

  /** Object for managing JDBC table schemas in {@link JDBCResourceManager} instances. */
  class JDBCSchema {

    private static final String DEFAULT_ID_COLUMN = "id";

    private final Map<String, String> columns;
    private final String idColumn;

    /**
     * Creates a JDBCSchema object using the map given and assigns the unique id column to the given
     * idColumn.
     *
     * <p>The columns map should map column name to SQL type. For example, {{"example":
     * "VARCHAR(200)}, {"example2": "INTEGER"}, {"example3": "BOOLEAN"}}
     *
     * @param columns a map containing the schema columns.
     * @param idColumn the unique id column.
     */
    public JDBCSchema(Map<String, String> columns, String idColumn) {
      checkArgument(
          columns.get(idColumn) != null,
          String.format("%s must be one of the columns passed in the columns map.", idColumn));
      this.columns = columns;
      this.idColumn = idColumn;
    }

    /**
     * Returns the name of the column used as the unique ID column.
     *
     * @return the id column.
     */
    public String getIdColumn() {
      return idColumn;
    }

    /**
     * Return this schema object as a SQL statement.
     *
     * @return this schema object as a SQL statement.
     */
    public String toSqlStatement() {
      StringBuilder sql = new StringBuilder(idColumn + " " + columns.get(idColumn));
      if (!columns.get(idColumn).toUpperCase().contains(" NOT NULL")) {
        sql.append(" NOT NULL");
      }
      for (String colKey : columns.keySet()) {
        if (colKey.contains(idColumn)) {
          continue;
        }
        sql.append(", ");
        sql.append(colKey).append(" ").append(columns.get(colKey).toUpperCase());
      }
      sql.append(", PRIMARY KEY ( ").append(idColumn).append(" )");
      return sql.toString();
    }
  }
}
