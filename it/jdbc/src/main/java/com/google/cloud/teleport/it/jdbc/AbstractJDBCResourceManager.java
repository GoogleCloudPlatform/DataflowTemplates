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

import static com.google.cloud.teleport.it.jdbc.JDBCResourceManagerUtils.checkValidTableName;
import static com.google.cloud.teleport.it.jdbc.JDBCResourceManagerUtils.generateDatabaseName;
import static com.google.cloud.teleport.it.jdbc.JDBCResourceManagerUtils.generatePassword;

import com.google.cloud.teleport.it.common.testcontainers.TestContainerResourceManager;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Abstract class for implementation of {@link JDBCResourceManager} interface.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is
 * created when the container first spins up, if one is not given.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public abstract class AbstractJDBCResourceManager<T extends JdbcDatabaseContainer<?>>
    extends TestContainerResourceManager<JdbcDatabaseContainer<?>> implements JDBCResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCResourceManager.class);

  protected static final String DEFAULT_JDBC_USERNAME = "root";

  protected final JDBCDriverFactory driver;
  protected final String databaseName;
  protected final String username;
  protected final String password;

  private final Map<String, String> tableIds;

  @VisibleForTesting
  AbstractJDBCResourceManager(
      T container, AbstractJDBCResourceManager.Builder<T> builder, JDBCDriverFactory driver) {
    super(
        container
            .withUsername(builder.username)
            .withPassword(builder.password)
            .withDatabaseName(builder.databaseName),
        builder);

    this.databaseName = container.getDatabaseName();
    this.username = container.getUsername();
    this.password = container.getPassword();
    this.tableIds = new HashMap<>();
    this.driver = driver;
  }

  protected AbstractJDBCResourceManager(
      T container, AbstractJDBCResourceManager.Builder<T> builder) {
    this(container, builder, new JDBCDriverFactory());
  }

  /**
   * Return the default port that this JDBC implementation listens on.
   *
   * @return the JDBC port.
   */
  protected abstract int getJDBCPort();

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public synchronized String getUri() {
    return String.format(
        "jdbc:%s://%s:%d/%s",
        getJDBCPrefix(), this.getHost(), this.getPort(getJDBCPort()), this.getDatabaseName());
  }

  public abstract String getJDBCPrefix();

  @Override
  public synchronized String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean createTable(String tableName, JDBCSchema schema) {
    // Check table ID
    checkValidTableName(tableName);

    // Check if table already exists
    if (tableIds.containsKey(tableName)) {
      throw new IllegalStateException(
          "Table " + tableName + " already exists for database " + databaseName + ".");
    }

    LOG.info("Creating table using tableName '{}'.", tableName);

    StringBuilder sql = new StringBuilder();
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();
      sql.append("CREATE TABLE ")
          .append(tableName)
          .append(" (")
          .append(schema.toSqlStatement())
          .append(")");

      stmt.executeUpdate(sql.toString());

    } catch (Exception e) {
      throw new JDBCResourceManagerException(
          "Error creating table with SQL statement: "
              + sql
              + " (for connection with URL "
              + getUri()
              + ")",
          e);
    }

    tableIds.put(tableName, schema.getIdColumn());
    LOG.info("Successfully created table {}.{}", databaseName, tableName);

    return true;
  }

  /**
   * Writes the given mapped rows into the specified columns. This method requires {@link
   * JDBCResourceManager#createTable(String, JDBCSchema)} to be called for the target table
   * beforehand.
   *
   * <p>The rows map must use the row id as the key, and the values will be inserted into the
   * columns at the row with that id. i.e. {0: [val1, val2, ...], 1: [val1, val2, ...], ...}
   *
   * @param tableName The name of the table to insert the given rows into.
   * @param rows A map representing the rows to be inserted into the table.
   * @throws JDBCResourceManagerException if method is called after resources have been cleaned up,
   *     if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  @Override
  public boolean write(String tableName, List<Map<String, Object>> rows) {
    if (rows.size() == 0) {
      return false;
    }

    LOG.info("Attempting to write {} rows to {}.{}.", rows.size(), databaseName, tableName);

    String insertStatement = "";
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();

      for (Map<String, Object> row : rows) {
        List<String> columns = new ArrayList<>(row.keySet());
        String sql = "INSERT INTO " + tableName + "(" + String.join(",", columns) + ") VALUES ";

        StringBuilder rowToInsert = new StringBuilder("(");
        List<String> valueList = new ArrayList<>();
        for (String colName : columns) {
          Object value = row.get(colName);
          if (NumberUtils.isCreatable(value.toString())
              || "true".equalsIgnoreCase(value.toString())
              || "false".equalsIgnoreCase(value.toString())) {
            valueList.add(String.valueOf(value));
          } else {
            valueList.add("'" + value + "'");
          }
        }

        rowToInsert.append(String.join(",", valueList)).append(")");
        insertStatement = sql + rowToInsert;
        LOG.info("Running SQL statement: " + insertStatement);
        stmt.executeUpdate(insertStatement);
      }

    } catch (Exception e) {
      throw new JDBCResourceManagerException(
          "Failed to insert values into table with SQL statement: " + insertStatement, e);
    }

    LOG.info("Successfully wrote {} rows to {}.{}.", rows.size(), databaseName, tableName);

    return true;
  }

  @Override
  public List<Map<String, Object>> readTable(String tableName) {
    LOG.info("Reading all rows from {}.{}", databaseName, tableName);

    List<Map<String, Object>> resultSet = new ArrayList<>();

    StringBuilder sql = new StringBuilder();
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();

      sql.append("SELECT * FROM ").append(tableName);
      ResultSet result = stmt.executeQuery(sql.toString());

      while (result.next()) {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData metadata = result.getMetaData();
        // Columns list in table metadata is 1-indexed
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          row.put(metadata.getColumnName(i), result.getObject(i));
        }
        resultSet.add(row);
      }

    } catch (Exception e) {
      throw new JDBCResourceManagerException(
          "Failed to fetch rows from table. SQL statement: " + sql, e);
    }

    LOG.info("Successfully loaded rows from {}.{}", databaseName, tableName);
    return resultSet;
  }

  @Override
  public synchronized List<String> getTableSchema(String tableName) {
    String sql = "";
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();
      sql = getFirstRow(tableName);
      ResultSet result = stmt.executeQuery(sql);

      ResultSetMetaData metadata = result.getMetaData();
      List<String> columnNames = new ArrayList<>();
      // Columns list in table metadata is 1-indexed
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        columnNames.add(metadata.getColumnName(i));
      }

      return columnNames;
    } catch (Exception e) {
      throw new JDBCResourceManagerException(
          "Failed to fetch table schema. SQL statement: " + sql, e);
    }
  }

  /**
   * Retrieves the first row from the table.
   *
   * @param tableName the name of the table to query.
   * @return the first row from the table.
   */
  protected String getFirstRow(String tableName) {
    return "SELECT * FROM " + tableName + " LIMIT 1";
  }

  @Override
  public synchronized ResultSet runSQLQuery(String sql) {
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();
      return stmt.executeQuery(sql);
    } catch (Exception e) {
      throw new JDBCResourceManagerException("Failed to execute SQL statement: " + sql, e);
    }
  }

  @Override
  public synchronized void runSQLUpdate(String sql) {
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();
      stmt.executeUpdate(sql);
    } catch (Exception e) {
      throw new JDBCResourceManagerException("Failed to execute SQL statement: " + sql, e);
    }
  }

  /**
   * Builder for {@link AbstractJDBCResourceManager}.
   *
   * @param <T> A class that extends {@link JdbcDatabaseContainer} for specific JDBC
   *     implementations.
   */
  public abstract static class Builder<T extends JdbcDatabaseContainer<?>>
      extends TestContainerResourceManager.Builder<AbstractJDBCResourceManager<T>> {

    protected String databaseName;
    protected String username;
    protected String password;

    public Builder(String testId) {
      super(testId);

      this.username = DEFAULT_JDBC_USERNAME;
      this.password = generatePassword();
      this.databaseName = generateDatabaseName(testId);
    }

    /**
     * Sets the database name to that of a static database instance. Use this method only when
     * attempting to operate on a pre-existing JDBC database.
     *
     * @param databaseName The database name.
     * @return this builder object with the database name set.
     */
    public AbstractJDBCResourceManager.Builder<T> setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    /**
     * Manually set the JDBC database username to the given username. This username will be used by
     * the resource manager to authenticate with the JDBC database.
     *
     * @param username the username for the JDBC database.
     * @return this builder with the username manually set.
     */
    public AbstractJDBCResourceManager.Builder<T> setUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Manually set the JDBC database password to the given password. This password will be used by
     * the resource manager to authenticate with the JDBC database.
     *
     * @param password the password for the JDBC database.
     * @return this builder with the password manually set.
     */
    public AbstractJDBCResourceManager.Builder<T> setPassword(String password) {
      this.password = password;
      return this;
    }
  }
}
