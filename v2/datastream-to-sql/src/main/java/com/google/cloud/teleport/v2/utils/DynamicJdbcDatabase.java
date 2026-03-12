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
package com.google.cloud.teleport.v2.utils;

import com.google.api.services.datastream.v1.model.MysqlColumn;
import com.google.api.services.datastream.v1.model.MysqlTable;
import com.google.api.services.datastream.v1.model.OracleColumn;
import com.google.api.services.datastream.v1.model.OracleTable;
import com.google.api.services.datastream.v1.model.PostgresqlColumn;
import com.google.api.services.datastream.v1.model.PostgresqlTable;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.teleport.v2.datastream.utils.DataStreamClient;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DynamicJdbcDatabase} provides utilities for fetching source schemas from Datastream
 * and applying DDL to a target JDBC database.
 */
public class DynamicJdbcDatabase implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcDatabase.class);

  private final transient DataSource dataSource;
  private final DataStreamClient datastreamClient;

  public DynamicJdbcDatabase(DataSource dataSource, DataStreamClient datastreamClient) {
    this.dataSource = dataSource;
    this.datastreamClient = datastreamClient;
  }

  public void createTable(
      String streamName,
      String schemaName,
      String tableName,
      String targetSchema,
      String targetTable,
      String databaseType)
      throws IOException, SQLException {
    SourceConfig sourceConfig = datastreamClient.getSourceConnectionProfile(streamName);

    if (targetSchema != null
        && !targetSchema.isEmpty()
        && databaseType.equalsIgnoreCase("postgres")) {
      executeDdl("CREATE SCHEMA IF NOT EXISTS \"" + targetSchema + "\";");
    }

    String ddl = "";
    if (databaseType.equalsIgnoreCase("postgres")) {
      if (sourceConfig.getMysqlSourceConfig() != null) {
        MysqlTable sourceTable =
            datastreamClient.discoverMysqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generatePostgresCreateTableFromMysql(targetSchema, targetTable, sourceTable);
      } else if (sourceConfig.getOracleSourceConfig() != null) {
        OracleTable sourceTable =
            datastreamClient.discoverOracleTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generatePostgresCreateTableFromOracle(targetSchema, targetTable, sourceTable);
      } else if (sourceConfig.getPostgresqlSourceConfig() != null) {
        PostgresqlTable sourceTable =
            datastreamClient.discoverPostgresqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generatePostgresCreateTableFromPostgres(targetSchema, targetTable, sourceTable);
      }
    } else if (databaseType.equalsIgnoreCase("mysql")) {
      if (sourceConfig.getMysqlSourceConfig() != null) {
        MysqlTable sourceTable =
            datastreamClient.discoverMysqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generateMysqlCreateTableFromMysql(targetSchema, targetTable, sourceTable);
      } else if (sourceConfig.getOracleSourceConfig() != null) {
        OracleTable sourceTable =
            datastreamClient.discoverOracleTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generateMysqlCreateTableFromOracle(targetSchema, targetTable, sourceTable);
      } else if (sourceConfig.getPostgresqlSourceConfig() != null) {
        PostgresqlTable sourceTable =
            datastreamClient.discoverPostgresqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        ddl = generateMysqlCreateTableFromPostgres(targetSchema, targetTable, sourceTable);
      }
    }

    if (!ddl.isEmpty()) {
      LOG.info("Executing DDL: {}", ddl);
      executeDdl(ddl);
    }
  }

  public void addColumn(
      String streamName,
      String schemaName,
      String tableName,
      String targetSchema,
      String targetTable,
      String columnName,
      String databaseType)
      throws IOException, SQLException {
    SourceConfig sourceConfig = datastreamClient.getSourceConnectionProfile(streamName);

    String ddl = "";
    if (databaseType.equalsIgnoreCase("postgres")) {
      if (sourceConfig.getMysqlSourceConfig() != null) {
        MysqlTable sourceTable =
            datastreamClient.discoverMysqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (MysqlColumn column : sourceTable.getMysqlColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generatePostgresAddColumnFromMysql(targetSchema, targetTable, column);
            break;
          }
        }
      } else if (sourceConfig.getOracleSourceConfig() != null) {
        OracleTable sourceTable =
            datastreamClient.discoverOracleTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (OracleColumn column : sourceTable.getOracleColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generatePostgresAddColumnFromOracle(targetSchema, targetTable, column);
            break;
          }
        }
      } else if (sourceConfig.getPostgresqlSourceConfig() != null) {
        PostgresqlTable sourceTable =
            datastreamClient.discoverPostgresqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (PostgresqlColumn column : sourceTable.getPostgresqlColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generatePostgresAddColumnFromPostgres(targetSchema, targetTable, column);
            break;
          }
        }
      }
    } else if (databaseType.equalsIgnoreCase("mysql")) {
      if (sourceConfig.getMysqlSourceConfig() != null) {
        MysqlTable sourceTable =
            datastreamClient.discoverMysqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (MysqlColumn column : sourceTable.getMysqlColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generateMysqlAddColumnFromMysql(targetSchema, targetTable, column);
            break;
          }
        }
      } else if (sourceConfig.getOracleSourceConfig() != null) {
        OracleTable sourceTable =
            datastreamClient.discoverOracleTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (OracleColumn column : sourceTable.getOracleColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generateMysqlAddColumnFromOracle(targetSchema, targetTable, column);
            break;
          }
        }
      } else if (sourceConfig.getPostgresqlSourceConfig() != null) {
        PostgresqlTable sourceTable =
            datastreamClient.discoverPostgresqlTableSchema(
                streamName, schemaName, tableName, sourceConfig);
        for (PostgresqlColumn column : sourceTable.getPostgresqlColumns()) {
          if (column.getColumn().equalsIgnoreCase(columnName)) {
            ddl = generateMysqlAddColumnFromPostgres(targetSchema, targetTable, column);
            break;
          }
        }
      }
    }

    if (!ddl.isEmpty()) {
      LOG.info("Executing DDL: {}", ddl);
      executeDdl(ddl);
    }
  }

  private void executeDdl(String ddl) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(ddl);
    }
  }

  // --- Postgres Target Generation ---

  private String generatePostgresCreateTableFromMysql(
      String schema, String table, MysqlTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS \"")
        .append(schema)
        .append("\".\"")
        .append(table)
        .append("\" (");
    List<String> columnDefinitions = new ArrayList<>();
    for (MysqlColumn column : sourceTable.getMysqlColumns()) {
      columnDefinitions.add("\"" + column.getColumn() + "\" " + mysqlToPostgresType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getMysqlColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(MysqlColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(
              primaryKeys.stream().map(pk -> "\"" + pk + "\"").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generatePostgresCreateTableFromOracle(
      String schema, String table, OracleTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS \"")
        .append(schema)
        .append("\".\"")
        .append(table)
        .append("\" (");
    List<String> columnDefinitions = new ArrayList<>();
    for (OracleColumn column : sourceTable.getOracleColumns()) {
      columnDefinitions.add("\"" + column.getColumn() + "\" " + oracleToPostgresType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getOracleColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(OracleColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(
              primaryKeys.stream().map(pk -> "\"" + pk + "\"").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generatePostgresCreateTableFromPostgres(
      String schema, String table, PostgresqlTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS \"")
        .append(schema)
        .append("\".\"")
        .append(table)
        .append("\" (");
    List<String> columnDefinitions = new ArrayList<>();
    for (PostgresqlColumn column : sourceTable.getPostgresqlColumns()) {
      columnDefinitions.add("\"" + column.getColumn() + "\" " + postgresToPostgresType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getPostgresqlColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(PostgresqlColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(
              primaryKeys.stream().map(pk -> "\"" + pk + "\"").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generatePostgresAddColumnFromMysql(
      String schema, String table, MysqlColumn column) {
    return "ALTER TABLE \""
        + schema
        + "\".\""
        + table
        + "\" ADD COLUMN IF NOT EXISTS \""
        + column.getColumn()
        + "\" "
        + mysqlToPostgresType(column)
        + ";";
  }

  private String generatePostgresAddColumnFromOracle(
      String schema, String table, OracleColumn column) {
    return "ALTER TABLE \""
        + schema
        + "\".\""
        + table
        + "\" ADD COLUMN IF NOT EXISTS \""
        + column.getColumn()
        + "\" "
        + oracleToPostgresType(column)
        + ";";
  }

  private String generatePostgresAddColumnFromPostgres(
      String schema, String table, PostgresqlColumn column) {
    return "ALTER TABLE \""
        + schema
        + "\".\""
        + table
        + "\" ADD COLUMN IF NOT EXISTS \""
        + column.getColumn()
        + "\" "
        + postgresToPostgresType(column)
        + ";";
  }

  // --- MySQL Target Generation ---

  private String generateMysqlCreateTableFromMysql(
      String schema, String table, MysqlTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS `").append(table).append("` (");
    List<String> columnDefinitions = new ArrayList<>();
    for (MysqlColumn column : sourceTable.getMysqlColumns()) {
      columnDefinitions.add("`" + column.getColumn() + "` " + mysqlToMysqlType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getMysqlColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(MysqlColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(primaryKeys.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generateMysqlCreateTableFromOracle(
      String schema, String table, OracleTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS `").append(table).append("` (");
    List<String> columnDefinitions = new ArrayList<>();
    for (OracleColumn column : sourceTable.getOracleColumns()) {
      columnDefinitions.add("`" + column.getColumn() + "` " + oracleToMysqlType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getOracleColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(OracleColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(primaryKeys.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generateMysqlCreateTableFromPostgres(
      String schema, String table, PostgresqlTable sourceTable) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE IF NOT EXISTS `").append(table).append("` (");
    List<String> columnDefinitions = new ArrayList<>();
    for (PostgresqlColumn column : sourceTable.getPostgresqlColumns()) {
      columnDefinitions.add("`" + column.getColumn() + "` " + postgresToMysqlType(column));
    }
    ddl.append(String.join(", ", columnDefinitions));

    List<String> primaryKeys =
        sourceTable.getPostgresqlColumns().stream()
            .filter(c -> Boolean.TRUE.equals(c.getPrimaryKey()))
            .map(PostgresqlColumn::getColumn)
            .collect(Collectors.toList());

    if (!primaryKeys.isEmpty()) {
      ddl.append(", PRIMARY KEY (")
          .append(primaryKeys.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", ")))
          .append(")");
    }
    ddl.append(");");
    return ddl.toString();
  }

  private String generateMysqlAddColumnFromMysql(String schema, String table, MysqlColumn column) {
    return "ALTER TABLE `"
        + table
        + "` ADD COLUMN `"
        + column.getColumn()
        + "` "
        + mysqlToMysqlType(column)
        + ";";
  }

  private String generateMysqlAddColumnFromOracle(
      String schema, String table, OracleColumn column) {
    return "ALTER TABLE `"
        + table
        + "` ADD COLUMN `"
        + column.getColumn()
        + "` "
        + oracleToMysqlType(column)
        + ";";
  }

  private String generateMysqlAddColumnFromPostgres(
      String schema, String table, PostgresqlColumn column) {
    return "ALTER TABLE `"
        + table
        + "` ADD COLUMN `"
        + column.getColumn()
        + "` "
        + postgresToMysqlType(column)
        + ";";
  }

  // --- Type Mapping ---

  private String mysqlToPostgresType(MysqlColumn column) {
    String type = column.getDataType().toUpperCase();
    switch (type) {
      case "TINYINT":
        return "SMALLINT";
      case "SMALLINT":
        return "SMALLINT";
      case "MEDIUMINT":
        return "INTEGER";
      case "INT":
      case "INTEGER":
        return "INTEGER";
      case "BIGINT":
        return "BIGINT";
      case "FLOAT":
        return "REAL";
      case "DOUBLE":
      case "DOUBLE PRECISION":
        return "DOUBLE PRECISION";
      case "DECIMAL":
      case "NUMERIC":
        return "NUMERIC";
      case "BIT":
        return "BOOLEAN";
      case "DATE":
        return "DATE";
      case "DATETIME":
      case "TIMESTAMP":
        return "TIMESTAMP";
      case "TIME":
        return "TIME";
      case "YEAR":
        return "INTEGER";
      case "CHAR":
        return "CHAR(" + column.getLength() + ")";
      case "VARCHAR":
        return "VARCHAR(" + column.getLength() + ")";
      case "BINARY":
        return "BYTEA";
      case "VARBINARY":
        return "BYTEA";
      case "TINYBLOB":
      case "BLOB":
      case "MEDIUMBLOB":
      case "LONGBLOB":
        return "BYTEA";
      case "TINYTEXT":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
        return "TEXT";
      case "ENUM":
      case "SET":
        return "TEXT";
      default:
        return "TEXT";
    }
  }

  private String oracleToPostgresType(OracleColumn column) {
    String type = column.getDataType().toUpperCase();
    if (type.contains("TIMESTAMP")) {
      return "TIMESTAMP";
    }
    switch (type) {
      case "NUMBER":
        if (column.getScale() != null && column.getScale() > 0) {
          return "NUMERIC";
        }
        if (column.getPrecision() != null && column.getPrecision() < 5) {
          return "SMALLINT";
        }
        if (column.getPrecision() != null && column.getPrecision() < 10) {
          return "INTEGER";
        }
        return "BIGINT";
      case "FLOAT":
        return "DOUBLE PRECISION";
      case "BINARY_FLOAT":
        return "REAL";
      case "BINARY_DOUBLE":
        return "DOUBLE PRECISION";
      case "DATE":
        return "TIMESTAMP";
      case "CHAR":
      case "NCHAR":
        return "CHAR(" + column.getLength() + ")";
      case "VARCHAR2":
      case "NVARCHAR2":
        return "VARCHAR(" + column.getLength() + ")";
      case "CLOB":
      case "NCLOB":
        return "TEXT";
      case "BLOB":
      case "RAW":
      case "LONG RAW":
        return "BYTEA";
      default:
        return "TEXT";
    }
  }

  private String postgresToPostgresType(PostgresqlColumn column) {
    return column.getDataType();
  }

  private String mysqlToMysqlType(MysqlColumn column) {
    return column.getDataType()
        + (column.getLength() != null ? "(" + column.getLength() + ")" : "");
  }

  private String oracleToMysqlType(OracleColumn column) {
    String type = column.getDataType().toUpperCase();
    if (type.contains("TIMESTAMP")) {
      return "TIMESTAMP";
    }
    switch (type) {
      case "NUMBER":
        if (column.getScale() != null && column.getScale() > 0) {
          return "DECIMAL";
        }
        return "BIGINT";
      case "FLOAT":
        return "DOUBLE";
      case "DATE":
        return "DATETIME";
      case "CHAR":
      case "NCHAR":
        return "CHAR(" + column.getLength() + ")";
      case "VARCHAR2":
      case "NVARCHAR2":
        return "VARCHAR(" + column.getLength() + ")";
      case "CLOB":
      case "NCLOB":
        return "TEXT";
      case "BLOB":
      case "RAW":
      case "LONG RAW":
        return "BLOB";
      default:
        return "TEXT";
    }
  }

  private String postgresToMysqlType(PostgresqlColumn column) {
    String type = column.getDataType().toUpperCase();
    if (type.contains("TIMESTAMP")) {
      return "TIMESTAMP";
    }
    switch (type) {
      case "SMALLINT":
        return "SMALLINT";
      case "INTEGER":
        return "INT";
      case "BIGINT":
        return "BIGINT";
      case "REAL":
        return "FLOAT";
      case "DOUBLE PRECISION":
        return "DOUBLE";
      case "NUMERIC":
      case "DECIMAL":
        return "DECIMAL";
      case "BOOLEAN":
        return "TINYINT(1)";
      case "DATE":
        return "DATE";
      case "TIME":
        return "TIME";
      case "VARCHAR":
        return "VARCHAR(" + column.getLength() + ")";
      case "TEXT":
        return "TEXT";
      case "BYTEA":
        return "BLOB";
      default:
        return "TEXT";
    }
  }
}
