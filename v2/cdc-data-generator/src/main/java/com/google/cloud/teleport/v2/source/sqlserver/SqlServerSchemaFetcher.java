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
package com.google.cloud.teleport.v2.source.sqlserver;

import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SQLServerInformationSchemaScanner;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema fetcher implementation for SQL Server databases.
 *
 * <p>Tables, columns, primary keys, foreign keys and unique indexes come from {@link
 * SQLServerInformationSchemaScanner}. A second query on {@code sys.columns} / {@code sys.types}
 * adds SQL Server specific flags that JDBC metadata does not expose (IDENTITY, sparse column sets,
 * typed XML, alias and CLR user-defined types). These flags decide which columns are written,
 * skipped or treated as server-generated.
 */
public class SqlServerSchemaFetcher implements SinkSchemaFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaFetcher.class);

  @FunctionalInterface
  interface ConnectionProvider {
    Connection get() throws SQLException;
  }

  /** SQL Server specific column flags read from {@code sys.columns} and {@code sys.types}. */
  @VisibleForTesting
  static final class ColumnInfo {
    final boolean isIdentity;
    final boolean isColumnSet;
    final boolean isTypedXml;
    final boolean isUserDefinedType;
    final boolean isAssemblyType;

    ColumnInfo(
        boolean isIdentity,
        boolean isColumnSet,
        boolean isTypedXml,
        boolean isUserDefinedType,
        boolean isAssemblyType) {
      this.isIdentity = isIdentity;
      this.isColumnSet = isColumnSet;
      this.isTypedXml = isTypedXml;
      this.isUserDefinedType = isUserDefinedType;
      this.isAssemblyType = isAssemblyType;
    }
  }

  @VisibleForTesting
  static final String COLUMN_INFO_QUERY =
      "SELECT t.name AS table_name, c.name AS column_name, c.is_identity, c.is_column_set,"
          + " CASE WHEN c.xml_collection_id <> 0 THEN 1 ELSE 0 END AS is_typed_xml,"
          + " ty.is_user_defined, ty.is_assembly_type"
          + " FROM sys.columns c"
          + " JOIN sys.tables t ON c.object_id = t.object_id"
          + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
          + " JOIN sys.types ty ON c.user_type_id = ty.user_type_id"
          + " WHERE s.name = ?";

  /** Columns larger than this (e.g. VARCHAR(MAX), TEXT, IMAGE) fall back to the default size. */
  private static final long MAX_NON_LOB_SIZE = 8000L;

  // MONEY is reported as DECIMAL(19,4) but tops out at 922,337,203,685,477.5807, and SMALLMONEY
  // as DECIMAL(10,4) but tops out at 214,748.3647. Generating one digit less keeps values in range.
  private static final int MONEY_MAX_PRECISION = 18;
  private static final int SMALLMONEY_MAX_PRECISION = 9;

  // INT64 values are generated with a 32-bit random int, which overflows TINYINT and SMALLINT.
  @VisibleForTesting static final String TINYINT_EXPRESSION = "#{number.numberBetween '0','256'}";

  @VisibleForTesting
  static final String SMALLINT_EXPRESSION = "#{number.numberBetween '-32768','32768'}";

  // DATETIME (1/300 s) and SMALLDATETIME (1 minute) round the stored value. Whole-second /
  // whole-minute values are stored exactly, so UPDATE / DELETE by primary key still match.
  @VisibleForTesting
  static final String DATETIME_EXPRESSION =
      "#{regexify '20[0-2][0-9]-0[1-9]-[0-2][1-8]T[01][0-9]:[0-5][0-9]:[0-5][0-9]Z'}";

  @VisibleForTesting
  static final String SMALLDATETIME_EXPRESSION =
      "#{regexify '20[0-2][0-9]-0[1-9]-[0-2][1-8]T[01][0-9]:[0-5][0-9]:00Z'}";

  // Random text is not valid for these types; generate small valid literals that SQL Server
  // converts implicitly from a string parameter.
  @VisibleForTesting static final String XML_EXPRESSION = "#{regexify '<root>[a-z0-9]{8}</root>'}";

  @VisibleForTesting
  static final String GEOMETRY_EXPRESSION = "#{regexify 'POINT[(][1-9][0-9] [1-9][0-9][)]'}";

  @VisibleForTesting
  static final String GEOGRAPHY_EXPRESSION = "#{regexify 'POINT[(][1-9][0-9] [1-8][0-9][)]'}";

  @VisibleForTesting static final String HIERARCHYID_EXPRESSION = "#{regexify '/[1-9]/[1-9]/'}";

  private String connectionUrl;
  private String username;
  private String password;
  private String namespace;
  private String dbName;
  private String connectionProperties;

  private final SqlServerTypeMapper typeMapper = new SqlServerTypeMapper();

  private final ConnectionProvider connectionProvider;

  public SqlServerSchemaFetcher() {
    this(null);
  }

  @VisibleForTesting
  SqlServerSchemaFetcher(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @VisibleForTesting
  protected SQLServerInformationSchemaScanner createScanner(
      Connection connection, String databaseName, String schemaName) {
    return new SQLServerInformationSchemaScanner(connection, databaseName, schemaName);
  }

  @Override
  public void init(SinkConfig sinkConfig) {
    if (sinkConfig == null) {
      throw new IllegalArgumentException("SinkConfig cannot be null");
    }
    if (!(sinkConfig instanceof SqlServerSinkConfig)) {
      throw new IllegalArgumentException(
          "Expected SqlServerSinkConfig, got " + sinkConfig.getClass().getName());
    }
    List<Shard> shards = ((SqlServerSinkConfig) sinkConfig).getShards();
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided SQL Server sink configuration.");
    }
    // Use the first shard to extract schema
    Shard firstShard = shards.get(0);
    this.username = firstShard.getUserName();
    this.password = firstShard.getPassword();
    this.namespace = firstShard.getNamespace();
    this.dbName = firstShard.getDbName();
    this.connectionProperties = firstShard.getConnectionProperties();
    this.connectionUrl = SqlServerDataWriter.buildConnectionUrl(firstShard);
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    try {
      Class.forName(Constants.SQLSERVER_JDBC_DRIVER);
      try (Connection connection = getConnection()) {
        return scanSqlServerSchema(connection);
      }
    } catch (ClassNotFoundException | SQLException e) {
      throw new IOException("Failed to fetch SQL Server schema", e);
    }
  }

  private DataGeneratorSchema scanSqlServerSchema(Connection connection) throws SQLException {
    String schemaName =
        (namespace != null && !namespace.isBlank())
            ? SqlServerDataWriter.unquoteIdentifier(namespace.trim())
            : connection.getSchema();
    SourceSchema sourceSchema = createScanner(connection, dbName, schemaName).scan();
    Map<String, Map<String, ColumnInfo>> columnInfos = fetchColumnInfo(connection, schemaName);

    Map<String, DataGeneratorTable> tables =
        sourceSchema.tables().values().stream()
            .collect(
                Collectors.toMap(
                    SourceTable::name,
                    t -> mapSourceTable(t, columnInfos.getOrDefault(t.name(), Map.of()))));

    return DataGeneratorSchema.builder().tables(ImmutableMap.copyOf(tables)).build();
  }

  /**
   * Reads SQL Server specific column flags for all tables in {@code schemaName}. Returns table name
   * -> column name -> flags. Failures are logged and an empty map is returned, in which case the
   * fetcher falls back to the JDBC {@code TYPE_NAME} only.
   */
  @VisibleForTesting
  protected Map<String, Map<String, ColumnInfo>> fetchColumnInfo(
      Connection connection, String schemaName) {
    Map<String, Map<String, ColumnInfo>> result = new HashMap<>();
    try (PreparedStatement statement = connection.prepareStatement(COLUMN_INFO_QUERY)) {
      statement.setString(1, schemaName);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          result
              .computeIfAbsent(rs.getString("table_name"), k -> new HashMap<>())
              .put(
                  rs.getString("column_name"),
                  new ColumnInfo(
                      rs.getBoolean("is_identity"),
                      rs.getBoolean("is_column_set"),
                      rs.getInt("is_typed_xml") == 1,
                      rs.getBoolean("is_user_defined"),
                      rs.getBoolean("is_assembly_type")));
        }
      }
    } catch (SQLException | RuntimeException e) {
      LOG.warn(
          "Could not read sys.columns metadata for schema {}; using JDBC metadata only.",
          schemaName,
          e);
      return new HashMap<>();
    }
    return result;
  }

  private DataGeneratorTable mapSourceTable(SourceTable table, Map<String, ColumnInfo> infos) {
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (SourceColumn column : table.columns()) {
      columnsBuilder.add(
          mapSourceColumn(column, table.primaryKeyColumns(), infos.get(column.name())));
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

  @VisibleForTesting
  DataGeneratorColumn mapSourceColumn(
      SourceColumn column, List<String> primaryKeys, ColumnInfo info) {
    String rawType = column.type() == null ? "" : column.type();
    String type = SqlServerTypeMapper.normalizeTypeName(rawType);
    LogicalType logicalType = typeMapper.getLogicalType(rawType, null, column.size());
    boolean isPrimaryKey = primaryKeys.contains(column.name());

    boolean isIdentity =
        SqlServerTypeMapper.isIdentityTypeName(rawType) || (info != null && info.isIdentity);
    // ROWVERSION (reported as "timestamp") is always server-generated and cannot be written.
    boolean isRowVersion = "TIMESTAMP".equals(type) || "ROWVERSION".equals(type);
    // Non-PK IDENTITY columns are left to the server. IDENTITY primary keys are written with
    // SET IDENTITY_INSERT ON by SqlServerDataWriter so that UPDATE / DELETE can find the row.
    boolean isGenerated = column.isGenerated() || isRowVersion || (isIdentity && !isPrimaryKey);

    DataGeneratorColumn.Builder builder =
        DataGeneratorColumn.builder()
            .name(column.name())
            .logicalType(logicalType)
            .isNullable(column.isNullable())
            .isGenerated(isGenerated)
            .isPrimaryKey(isPrimaryKey)
            .size(effectiveSize(type, column.size()));

    populateNumericBounds(type, logicalType, column, builder);

    switch (type) {
      case "TINYINT":
        builder.fakerExpression(TINYINT_EXPRESSION);
        break;
      case "SMALLINT":
        builder.fakerExpression(SMALLINT_EXPRESSION);
        break;
      case "DATETIME":
        builder.fakerExpression(DATETIME_EXPRESSION);
        break;
      case "SMALLDATETIME":
        builder.fakerExpression(SMALLDATETIME_EXPRESSION);
        break;
      case "XML":
        builder.fakerExpression(XML_EXPRESSION);
        break;
      case "GEOMETRY":
        builder.fakerExpression(GEOMETRY_EXPRESSION);
        break;
      case "GEOGRAPHY":
        builder.fakerExpression(GEOGRAPHY_EXPRESSION);
        break;
      case "HIERARCHYID":
        builder.fakerExpression(HIERARCHYID_EXPRESSION);
        break;
      default:
        break;
    }

    if (!isPrimaryKey && isUnsupported(type, info)) {
      builder.isSkipped(true);
    }
    return builder.build();
  }

  /**
   * Types that cannot be generated: VECTOR (no ARRAY logical type), sparse column sets (computed
   * from the sparse columns), typed XML (must match an XML schema collection), alias types (base
   * type is not resolved) and CLR user-defined types.
   */
  private static boolean isUnsupported(String type, ColumnInfo info) {
    if ("VECTOR".equals(type)) {
      return true;
    }
    if (info == null) {
      return false;
    }
    return info.isColumnSet || info.isTypedXml || info.isUserDefinedType;
  }

  private static Long effectiveSize(String type, Long size) {
    if ("SQL_VARIANT".equals(type)) {
      // sql_variant holds at most 8016 bytes; its reported size would overflow it with nvarchar.
      return null;
    }
    if (size != null && size > MAX_NON_LOB_SIZE) {
      // (N)VARCHAR(MAX), VARBINARY(MAX), TEXT, NTEXT, IMAGE, XML: use the default length.
      return null;
    }
    return size;
  }

  private static void populateNumericBounds(
      String type, LogicalType logicalType, SourceColumn column, DataGeneratorColumn.Builder b) {
    if (logicalType != LogicalType.NUMERIC) {
      // Precision / scale reported for integer, float and temporal types are not used.
      return;
    }
    Integer precision = column.precision();
    Integer scale = column.scale();
    if (precision == null || precision <= 0) {
      return;
    }
    if ("MONEY".equals(type)) {
      precision = Math.min(precision, MONEY_MAX_PRECISION);
    } else if ("SMALLMONEY".equals(type)) {
      precision = Math.min(precision, SMALLMONEY_MAX_PRECISION);
    }
    int effectiveScale = scale == null ? 0 : Math.min(scale, precision);
    b.precision(precision).scale(effectiveScale);
  }

  protected Connection getConnection() throws SQLException {
    if (this.connectionProvider != null) {
      return this.connectionProvider.get();
    }
    Properties props = JdbcConnectionHelper.parseProperties(this.connectionProperties);
    if (username != null) {
      props.setProperty("user", username);
    }
    if (password != null) {
      props.setProperty("password", password);
    }
    return DriverManager.getConnection(connectionUrl, props);
  }
}
