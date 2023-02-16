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

import com.google.cloud.teleport.it.ResourceManager;
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
   * Writes a given row into a table at the given id. This method requires {@link
   * JDBCResourceManager#createTable(String, JDBCSchema)} to be called for the target table
   * beforehand.
   *
   * <p>The values given in the row must match the schema by which the table is defined by.
   *
   * @param tableName The name of the table to insert the given row into.
   * @param id The row id to add or edit
   * @param row The values to write to the row.
   * @throws JDBCResourceManagerException if method is called after resources have been cleaned up,
   *     if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  boolean write(String tableName, Integer id, Object... row);

  /**
   * Writes the given mapped rows into a table. This method requires {@link
   * JDBCResourceManager#createTable(String, JDBCSchema)} to be called for the target table
   * beforehand.
   *
   * <p>The rows map must use the row id as the key, and the values will be inserted into the row at
   * that id. i.e. {0: [val1, val2, ...], 1: [val1, val2, ...], ...}
   *
   * @param tableName The name of the table to insert the given rows into.
   * @param rows A map representing the rows to be inserted into the table.
   * @throws JDBCResourceManagerException if method is called after resources have been cleaned up,
   *     if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  boolean write(String tableName, Map<Integer, List<Object>> rows);

  /**
   * Reads all the rows in a table and returns a map that maps the row id to the values in that row.
   *
   * <p>For example, the table:<br>
   * ID,first,last<br>
   * 1,John,Doe<br>
   * 2,Jane,Doe
   *
   * <p>Would return the following map:<br>
   * {{1: ["John","Doe"]}, {2: ["Jane","Doe"]}}
   *
   * @param tableName The name of the table to read rows from.
   * @return a map containing the table rows.
   */
  Map<Integer, List<String>> readTable(String tableName);

  /**
   * Returns the schema of the given table as a list of strings.
   *
   * @param tableName the name of the table to fetch the schema of.
   * @return the list of column names.
   */
  List<String> getTableSchema(String tableName);

  /**
   * Run the given SQL statement.
   *
   * @param sql The SQL statement to run.
   * @return A ResultSet containing the result of the execution.
   */
  ResultSet runSQLStatement(String sql);

  /** Object for managing JDBC table schemas in {@link JDBCResourceManager} instances. */
  class JDBCSchema {

    private static final String DEFAULT_ID_COLUMN = "id";

    private final Map<String, String> columns;
    private final String idColumn;

    /**
     * Creates a JDBCSchema object using the map given and adds a unique ID column using the default
     * name "id".
     *
     * <p>The columns map should map column name to SQL type. For example, {{"example" ->
     * "VARCHAR(200)} {"example2" -> "INTEGER"} {"example3" -> "BOOLEAN"}}
     *
     * <p>Note: The ID column should not be included as an entry in the map passed to this
     * constructor.
     *
     * @param columns a map containing the schema columns.
     */
    public JDBCSchema(Map<String, String> columns) {
      this(columns, DEFAULT_ID_COLUMN);
    }

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
      StringBuilder sql = new StringBuilder(idColumn + " INTEGER not NULL");
      for (String colKey : columns.keySet()) {
        if (colKey.equals(idColumn)) {
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
