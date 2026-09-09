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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.SQLServerInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLServerSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerSpToSrcSourceConnector.class);
  private static final String JDBC_URL_PREFIX = "jdbc:sqlserver://";

  private final IConnectionHelper connectionHelper;

  public SQLServerSpToSrcSourceConnector() {
    this.connectionHelper = new JdbcConnectionHelper();
  }

  @VisibleForTesting
  SQLServerSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new SQLServerDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    StringBuilder url =
        new StringBuilder(
            JDBC_URL_PREFIX
                + shard.getHost()
                + ":"
                + shard.getPort()
                + ";databaseName="
                + shard.getDbName());
    String connectionProperties = shard.getConnectionProperties();
    String propsLower = (connectionProperties != null) ? connectionProperties.toLowerCase() : "";
    if (!propsLower.contains("trustservercertificate")) {
      url.append(";trustServerCertificate=true");
    }
    if (!propsLower.contains("encrypt")) {
      url.append(";encrypt=false");
    }
    if (connectionProperties != null && !connectionProperties.trim().isEmpty()) {
      String props = connectionProperties.trim();
      if (!props.startsWith(";")) {
        url.append(";");
      }
      url.append(props);
    }
    return url.toString();
  }

  @Override
  public IDao getDao(Shard shard) {
    return new JdbcDao(getConnectionUrl(shard), shard.getUserName(), getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      for (Shard shard : shards) {
        shard.setConnectionUrl(getConnectionUrl(shard));
      }
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              shards, null, maxConnections, "com.microsoft.sqlserver.jdbc.SQLServerDriver", null);
      connectionHelper.init(request);
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("sqlserver", shardFilePath);
    if (sourceConnectionConfig instanceof JdbcShardConfig) {
      return ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs();
    }
    throw new IllegalArgumentException(
        "Expected JdbcShardConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) throws Exception {
    for (Shard shard : shards) {
      try (Connection conn = createConnection(shard)) {
        if (conn != null) {
          try (Statement stmt = conn.createStatement();
              ResultSet rs =
                  stmt.executeQuery(
                      "SELECT CASE WHEN DATABASEPROPERTYEX(DB_NAME(), 'Updateability') = 'READ_ONLY' THEN 1 ELSE 0 END")) {
            if (rs != null && rs.next() && rs.getInt(1) == 1) {
              throw new RuntimeException(
                  "SQL Server destination is in read-only mode for shard: "
                      + shard.getLogicalShardId());
            }
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Error checking SQL Server read-only status for shard {}: {}",
            shard.getLogicalShardId(),
            e.getMessage());
        throw new RuntimeException("Error checking SQL Server read-only status", e);
      }
    }
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    try (Connection connection = createConnection(shards.get(0))) {
      return new SQLServerInformationSchemaScanner(connection, shards.get(0).getDbName()).scan();
    }
  }

  @VisibleForTesting
  Connection createConnection(Shard shard) throws Exception {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    return DriverManager.getConnection(
        getConnectionUrl(shard), shard.getUserName(), shard.getPassword());
  }

  @Override
  public boolean supportsSharding() {
    return true;
  }

  @Override
  public boolean shouldUpdateReadValuesToSpannerRecord() {
    return true;
  }

  @Override
  public TupleTag<String> classifyException(Throwable cause) {
    if (cause instanceof SQLSyntaxErrorException || cause instanceof SQLDataException) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    if (cause instanceof SQLNonTransientConnectionException) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    if (cause instanceof SQLException sqlEx) {
      int errorCode = sqlEx.getErrorCode();
      String sqlState = sqlEx.getSQLState();
      // Permanent error codes in SQL Server:
      // 102: Incorrect syntax near '...'
      // 207: Invalid column name '...'
      // 208: Invalid object name '...'
      // 245: Conversion failed when converting the varchar value '...' to data type ...
      // 547: The INSERT/UPDATE/DELETE statement conflicted with the CHECK/FOREIGN KEY constraint
      // 2601: Cannot insert duplicate key row in object '...' with unique index '...'
      // 2627: Violation of PRIMARY KEY constraint '...'
      // 8114: Error converting data type varchar to ...
      // 8152: String or binary data would be truncated
      if (errorCode == 102
          || errorCode == 207
          || errorCode == 208
          || errorCode == 245
          || errorCode == 547
          || errorCode == 2601
          || errorCode == 2627
          || errorCode == 8114
          || errorCode == 8152) {
        return Constants.PERMANENT_ERROR_TAG;
      }
      if (sqlState != null
          && (sqlState.startsWith("42")
              || sqlState.startsWith("22")
              || sqlState.startsWith("23"))) {
        return Constants.PERMANENT_ERROR_TAG;
      }
    }
    return null;
  }
}
