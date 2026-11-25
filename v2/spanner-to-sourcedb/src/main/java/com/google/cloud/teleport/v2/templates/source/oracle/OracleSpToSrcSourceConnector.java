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
package com.google.cloud.teleport.v2.templates.source.oracle;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.OracleInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import org.apache.beam.sdk.options.PipelineOptions;

public class OracleSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private static final String JDBC_URL_PREFIX = "jdbc:oracle:thin:@//";

  /**
   * Oracle JDBC connection property that enables per-connection implicit {@link
   * java.sql.PreparedStatement} caching.
   *
   * <p>When unset in a shard's {@code connectionProperties}, {@link #initConnectionHelper} sets
   * {@code oracle.jdbc.implicitStatementCacheSize=100} by default so repeated {@code
   * prepareStatement(...)} calls reuse open server-side cursors without incurring per-row soft
   * parses in Oracle's Shared Pool.
   *
   * <p>To override the cache size (for example, when replicating more than 50 active tables at 2
   * statements per table, or to stay below a custom Oracle {@code OPEN_CURSORS} per-session limit)
   * or disable caching ({@code 0}), specify {@code oracle.jdbc.implicitStatementCacheSize=<N>} in
   * the shard's {@code connectionProperties} field inside the {@code sourceShardsFilePath} JSON
   * configuration.
   */
  public static final String IMPLICIT_STATEMENT_CACHE_PROP =
      "oracle.jdbc.implicitStatementCacheSize";

  static final String SHORT_IMPLICIT_STATEMENT_CACHE_PROP = "implicitStatementCacheSize";

  /**
   * Default per-connection Oracle implicit statement cache size ({@code 100} cursors, well below
   * Oracle's standard {@code OPEN_CURSORS=300} session limit).
   */
  public static final String DEFAULT_IMPLICIT_STATEMENT_CACHE_SIZE = "100";

  private final IConnectionHelper connectionHelper;

  public OracleSpToSrcSourceConnector() {
    this.connectionHelper = new JdbcConnectionHelper();
  }

  @VisibleForTesting
  OracleSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new OracleDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return this.connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    return JDBC_URL_PREFIX + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
  }

  @Override
  public IDao getDao(Shard shard) {
    return new OracleJdbcDao(getConnectionUrl(shard), shard.getUserName(), getConnectionHelper());
  }

  /**
   * Initializes the per-worker HikariCP connection pool for all configured Oracle shards.
   *
   * <p>Applies {@code oracle.jdbc.implicitStatementCacheSize=100} by default on each shard unless
   * {@link #IMPLICIT_STATEMENT_CACHE_PROP} is already present in {@link
   * Shard#getConnectionProperties()}. Users can tune or disable ({@code =0}) this setting via
   * {@code connectionProperties} in the shard JSON file.
   */
  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      for (Shard shard : shards) {
        shard.setConnectionUrl(getConnectionUrl(shard));
        shard.setConnectionProperties(withDefaultStatementCache(shard.getConnectionProperties()));
      }
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              shards, null, maxConnections, "oracle.jdbc.OracleDriver", null);
      connectionHelper.init(request);
    }
  }

  static String withDefaultStatementCache(String existingProps) {
    String defaultEntry =
        IMPLICIT_STATEMENT_CACHE_PROP + "=" + DEFAULT_IMPLICIT_STATEMENT_CACHE_SIZE;
    if (existingProps == null || existingProps.trim().isEmpty()) {
      return defaultEntry;
    }
    String trimmed = existingProps.trim();
    Properties parsed = JdbcConnectionHelper.parseProperties(trimmed);
    if (parsed.containsKey(IMPLICIT_STATEMENT_CACHE_PROP)
        || parsed.containsKey(SHORT_IMPLICIT_STATEMENT_CACHE_PROP)) {
      return existingProps;
    }
    if (trimmed.contains("&")) {
      return trimmed.endsWith("&") ? trimmed + defaultEntry : trimmed + "&" + defaultEntry;
    }
    if (trimmed.contains(";")) {
      return trimmed.endsWith(";") ? trimmed + defaultEntry : trimmed + ";" + defaultEntry;
    }
    return trimmed + "\n" + defaultEntry;
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    // Passing "oracle" to parseConfiguration assumes it supports it; fallback to just string
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("oracle", shardFilePath);
    if (sourceConnectionConfig instanceof JdbcShardConfig) {
      return ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs();
    }
    throw new IllegalArgumentException(
        "Expected JdbcShardConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) {}

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    try (Connection connection = createConnection(shards.get(0))) {
      return new OracleInformationSchemaScanner(connection).scan();
    }
  }

  Connection createConnection(Shard shard) throws Exception {
    Class.forName("oracle.jdbc.OracleDriver");
    return java.sql.DriverManager.getConnection(
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
  public org.apache.beam.sdk.values.TupleTag<String> classifyException(Throwable cause) {
    Throwable current = cause;
    while (current != null) {
      if (current instanceof java.sql.SQLSyntaxErrorException
          || current instanceof java.sql.SQLDataException) {
        return com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
      }
      current = current.getCause();
    }
    return null;
  }
}
