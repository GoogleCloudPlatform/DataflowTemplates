/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.oracle;

import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.OracleInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceIndex;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/** Schema fetcher implementation for Oracle databases. */
public class OracleSchemaFetcher implements SinkSchemaFetcher {

  @FunctionalInterface
  interface ConnectionProvider {
    Connection get() throws SQLException;
  }

  private String driverClassName;
  private String connectionUrl;
  private String username;
  private String password;
  private String namespace;
  private String connectionProperties;

  private final OracleTypeMapper typeMapper = new OracleTypeMapper();

  private final ConnectionProvider connectionProvider;

  public OracleSchemaFetcher() {
    this(null);
  }

  @VisibleForTesting
  OracleSchemaFetcher(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @VisibleForTesting
  protected OracleInformationSchemaScanner createScanner(Connection connection, String schemaName) {
    return new OracleInformationSchemaScanner(connection, schemaName);
  }

  @Override
  public void init(SinkConfig sinkConfig) {
    if (sinkConfig == null) {
      throw new IllegalArgumentException("SinkConfig cannot be null");
    }
    if (!(sinkConfig instanceof OracleSinkConfig)) {
      throw new IllegalArgumentException(
          "Expected OracleSinkConfig, got " + sinkConfig.getClass().getName());
    }
    OracleSinkConfig oracleSinkConfig = (OracleSinkConfig) sinkConfig;
    List<Shard> shards = oracleSinkConfig.getShards();
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided Oracle sink configuration.");
    }
    // Use the first shard to extract schema
    Shard firstShard = shards.get(0);
    this.username = firstShard.getUserName();
    this.password = firstShard.getPassword();
    this.namespace = firstShard.getNamespace();
    this.connectionProperties = firstShard.getConnectionProperties();

    String host = firstShard.getHost();
    String port = firstShard.getPort();
    String dbName = firstShard.getDbName() != null ? firstShard.getDbName() : "";

    this.connectionUrl =
        String.format("%s%s:%s/%s", Constants.JDBC_ORACLE_URL_PREFIX, host, port, dbName);
    this.driverClassName = Constants.ORACLE_JDBC_DRIVER;
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    try {
      if (driverClassName != null && !driverClassName.isEmpty()) {
        Class.forName(driverClassName);
      }
      try (Connection connection = getConnection()) {
        return scanOracleSchema(connection);
      }
    } catch (ClassNotFoundException | SQLException e) {
      throw new IOException("Failed to fetch Oracle schema", e);
    }
  }

  private DataGeneratorSchema scanOracleSchema(Connection connection) throws SQLException {
    String schemaName = (namespace != null && !namespace.isBlank()) ? namespace : null;
    if (schemaName == null) {
      try {
        schemaName = connection.getSchema();
      } catch (Exception e) {
        // Ignore and let scanner fall back to metadata username.
      }
    }
    OracleInformationSchemaScanner scanner = createScanner(connection, schemaName);
    SourceSchema sourceSchema = scanner.scan();

    Map<String, DataGeneratorTable> tables =
        sourceSchema.tables().values().stream()
            .collect(Collectors.toMap(SourceTable::name, this::mapSourceTable));

    return DataGeneratorSchema.builder().tables(ImmutableMap.copyOf(tables)).build();
  }

  private DataGeneratorTable mapSourceTable(SourceTable table) {
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (SourceColumn column : table.columns()) {
      columnsBuilder.add(mapSourceColumn(column, table.primaryKeyColumns()));
    }

    ImmutableList.Builder<DataGeneratorForeignKey> foreignKeysBuilder = ImmutableList.builder();
    if (table.foreignKeys() != null) {
      for (SourceForeignKey fk : table.foreignKeys()) {
        foreignKeysBuilder.add(
            DataGeneratorForeignKey.builder()
                .name(fk.name())
                .referencedTable(fk.referencedTable())
                .keyColumns(fk.keyColumns())
                .referencedColumns(fk.referencedColumns())
                .build());
      }
    }

    ImmutableList.Builder<DataGeneratorUniqueKey> uniqueKeysBuilder = ImmutableList.builder();
    if (table.indexes() != null) {
      for (SourceIndex index : table.indexes()) {
        if (index.isUnique() && !index.isPrimary()) {
          uniqueKeysBuilder.add(
              DataGeneratorUniqueKey.builder().name(index.name()).columns(index.columns()).build());
        }
      }
    }

    return DataGeneratorTable.builder()
        .name(table.name())
        .columns(columnsBuilder.build())
        .primaryKeys(table.primaryKeyColumns())
        .foreignKeys(foreignKeysBuilder.build())
        .uniqueKeys(uniqueKeysBuilder.build())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private DataGeneratorColumn mapSourceColumn(SourceColumn column, List<String> primaryKeys) {
    LogicalType logicalType = typeMapper.getLogicalType(column.type(), null, column.size());
    Long effectiveSize = column.size();
    if (effectiveSize != null && effectiveSize > 0) {
      effectiveSize = Math.min(effectiveSize, 20L);
    }

    DataGeneratorColumn.Builder builder =
        DataGeneratorColumn.builder()
            .name(column.name())
            .logicalType(logicalType)
            .isNullable(column.isNullable())
            .isGenerated(column.isGenerated())
            .isPrimaryKey(primaryKeys.contains(column.name()))
            .size(effectiveSize);

    populateNumericBounds(column, logicalType, builder);

    if (column.type() != null) {
      String upperType =
          column
              .type()
              .toUpperCase(Locale.ROOT)
              .replaceAll("\\s*\\([^)]*\\)", "")
              .replaceAll("\\s+", " ")
              .trim();
      if ("INTERVAL YEAR TO MONTH".equals(upperType)) {
        builder.fakerExpression("#{regexify '[+][0][0]-[0][0-9]'}");
      } else if ("INTERVAL DAY TO SECOND".equals(upperType)) {
        builder.fakerExpression("#{regexify '[+][0][0] [0-1][0-9]:[0-5][0-9]:[0-5][0-9]'}");
      } else if ("ROWID".equals(upperType) || "UROWID".equals(upperType)) {
        builder.fakerExpression("#{regexify 'AAA[A-Z]{3}AA[A-H]AAA[A-Z]{3}AA[A-Z]'}");
      } else if ("XMLTYPE".equals(upperType)) {
        builder.fakerExpression("#{regexify '<root>[a-z0-9]{8}</root>'}");
      } else if (!primaryKeys.contains(column.name()) && isUnsupportedObjectOrFileType(upperType)) {
        builder.isSkipped(true);
      }
    }

    return builder.build();
  }

  private static void populateNumericBounds(
      SourceColumn column, LogicalType logicalType, DataGeneratorColumn.Builder builder) {
    Integer rawPrecision = column.precision();
    Integer rawScale = column.scale();
    if (logicalType != LogicalType.NUMERIC) {
      builder.precision(rawPrecision).scale(rawScale);
      return;
    }
    if (rawScale != null && rawScale < 0) {
      int prec = (rawPrecision != null && rawPrecision > 0) ? Math.min(rawPrecision, 10) : 10;
      builder.precision(prec).scale(0);
      return;
    }
    if (rawScale != null && rawPrecision != null && rawPrecision > 0 && rawScale > rawPrecision) {
      int leadingZeros = rawScale - rawPrecision;
      int sigDigits = Math.min(rawPrecision, 10);
      String digitPattern = sigDigits <= 1 ? "[1-9]" : "[1-9][0-9]{" + (sigDigits - 1) + "}";
      builder
          .precision(rawScale)
          .scale(rawScale)
          .fakerExpression("#{regexify '0[.]" + "0".repeat(leadingZeros) + digitPattern + "'}");
      return;
    }
    if (rawScale != null) {
      int rawPrec =
          (rawPrecision != null && rawPrecision > 0) ? rawPrecision : Math.max(10, rawScale + 5);
      int effectivePrec = Math.min(rawPrec, Math.max(10, rawScale + 5));
      builder.precision(effectivePrec).scale(rawScale);
      return;
    }
    if (rawPrecision != null && rawPrecision > 0) {
      builder.precision(Math.min(rawPrecision, 10)).scale(0);
    }
  }

  private static boolean isUnsupportedObjectOrFileType(String upperType) {
    switch (upperType) {
      case "BFILE":
      case "SDO_GEOMETRY":
      case "ANYDATA":
      case "ORDIMAGE":
      case "ORDAUDIO":
      case "ORDVIDEO":
      case "ORDDOC":
      case "ORDDICOM":
        return true;
      default:
        return false;
    }
  }

  protected Connection getConnection() throws SQLException {
    if (this.connectionProvider != null) {
      return this.connectionProvider.get();
    }
    if (this.connectionProperties != null && !this.connectionProperties.isEmpty()) {
      Properties props = JdbcConnectionHelper.parseProperties(this.connectionProperties);
      if (username != null) {
        props.setProperty("user", username);
      }
      if (password != null) {
        props.setProperty("password", password);
      }
      return DriverManager.getConnection(connectionUrl, props);
    }
    return DriverManager.getConnection(connectionUrl, username, password);
  }
}
