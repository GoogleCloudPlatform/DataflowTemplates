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
package com.google.cloud.teleport.v2.source.mysql;

import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.iowrapper.config.defaults.MySqlConfigDefaults;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link AbstractJdbcSrcToSpSourceConnector}. */
public class MySqlSrcToSpSourceConnector extends AbstractJdbcSrcToSpSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlSrcToSpSourceConnector.class);

  // Implementation Detail, ImmutableMap.of(...) supports only upto 10 arguments.
  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("BIGINT", UnifiedMappingProvider.Type.LONG)
          .put("BIGINT UNSIGNED", UnifiedMappingProvider.Type.NUMBER)
          .put("BINARY", UnifiedMappingProvider.Type.STRING)
          .put("BIT", UnifiedMappingProvider.Type.LONG)
          .put("BLOB", UnifiedMappingProvider.Type.STRING)
          .put("BOOL", UnifiedMappingProvider.Type.INTEGER)
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DATETIME", UnifiedMappingProvider.Type.DATETIME)
          .put("DECIMAL", UnifiedMappingProvider.Type.DECIMAL)
          .put("DOUBLE", UnifiedMappingProvider.Type.DOUBLE)
          .put("ENUM", UnifiedMappingProvider.Type.STRING)
          .put("FLOAT", UnifiedMappingProvider.Type.FLOAT)
          .put("INTEGER", UnifiedMappingProvider.Type.INTEGER)
          .put("INTEGER UNSIGNED", UnifiedMappingProvider.Type.LONG)
          .put("JSON", UnifiedMappingProvider.Type.JSON)
          .put("LONGBLOB", UnifiedMappingProvider.Type.STRING)
          .put("LONGTEXT", UnifiedMappingProvider.Type.STRING)
          .put("MEDIUMBLOB", UnifiedMappingProvider.Type.STRING)
          .put("MEDIUMINT", UnifiedMappingProvider.Type.INTEGER)
          .put("MEDIUMTEXT", UnifiedMappingProvider.Type.STRING)
          .put("SET", UnifiedMappingProvider.Type.STRING)
          .put("SMALLINT", UnifiedMappingProvider.Type.INTEGER)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          .put("TIME", UnifiedMappingProvider.Type.TIME_INTERVAL)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TINYBLOB", UnifiedMappingProvider.Type.STRING)
          .put("TINYINT", UnifiedMappingProvider.Type.INTEGER)
          .put("TINYTEXT", UnifiedMappingProvider.Type.STRING)
          .put("VARBINARY", UnifiedMappingProvider.Type.STRING)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("YEAR", UnifiedMappingProvider.Type.INTEGER)
          .put("UNSUPPORTED", UnifiedMappingProvider.Type.UNSUPPORTED)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  /**
   * Returns the map of Source Schema to {@link UnifiedTypeMapping} for all supported MySQL types.
   *
   * @return MySQL mapping.
   */
  @Override
  public ImmutableMap<String, UnifiedTypeMapping> getTypeMapping() {
    return MAPPING;
  }

  public String getSourceType() {
    return Constants.MYSQL_SOURCE_TYPE;
  }

  @Override
  public JdbcValueMappingsProvider getJdbcValueMappingsProvider() {
    return MySqlConfigDefaults.DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER;
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builder()
        .setSourceDbDialect(SQLDialect.MYSQL)
        .setUnifiedTypeMapper(new UnifiedTypeMapper(getTypeMapping()))
        .setDialectAdapter(MySqlConfigDefaults.DEFAULT_MYSQL_DIALECT_ADAPTER)
        .setValueMappingsProvider(MySqlConfigDefaults.DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(MySqlConfigDefaults.DEFAULT_MYSQL_MAX_CONNECTIONS)
        .setSqlInitSeq(MySqlConfigDefaults.DEFAULT_MYSQL_INIT_SEQ)
        .setSchemaDiscoveryBackOff(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF)
        .setTables(ImmutableList.of())
        .setTableVsPartitionColumns(ImmutableMap.of())
        .setMaxPartitions(null)
        .setWaitOn(null)
        .setDbParallelizationForReads(null)
        .setDbParallelizationForSplitProcess(
            JdbcIOWrapperConfig.DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS)
        .setReadWithUniformPartitionsFeatureEnabled(true)
        .setTestOnBorrow(JdbcIOWrapperConfig.DEFAULT_TEST_ON_BORROW)
        .setTestOnCreate(JdbcIOWrapperConfig.DEFAULT_TEST_ON_CREATE)
        .setTestOnReturn(JdbcIOWrapperConfig.DEFAULT_TEST_ON_RETURN)
        .setTestWhileIdle(JdbcIOWrapperConfig.DEFAULT_TEST_WILE_IDLE)
        .setValidationQuery(JdbcIOWrapperConfig.DEFAULT_VALIDATEION_QUERY)
        .setRemoveAbandonedTimeout(JdbcIOWrapperConfig.DEFAULT_REMOVE_ABANDONED_TIMEOUT)
        .setMinEvictableIdleTimeMillis(JdbcIOWrapperConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS)
        .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(
            JdbcIOWrapperConfig.DEFAULT_SCHEMA_DISCOVERY_CONNECTIVITY_TIMEOUT_MILLISECONDS)
        .setSplitStageCountHint(-1L)
        .setWorkerMemoryBytes(null)
        .setWorkerCores(null);
  }

  @Override
  public SourceSchemaReference getSourceSchemaReference(String dbName, String namespace) {
    // Namespaces are not supported for MySQL
    return SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName(dbName).build());
  }

  @Override
  public String getJdbcUrl(
      String jdbcUrl,
      String host,
      int port,
      String dbName,
      String connectionProperties,
      String namespace,
      Integer fetchSize) {
    if (jdbcUrl == null) {
      jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
      if (StringUtils.isNotBlank(connectionProperties)) {
        jdbcUrl = jdbcUrl + "?" + connectionProperties;
      }
    }
    for (Entry<String, String> entry :
        MySqlConfigDefaults.DEFAULT_MYSQL_URL_PROPERTIES.entrySet()) {
      jdbcUrl = OptionsToConfigBuilder.addParamToJdbcUrl(jdbcUrl, entry.getKey(), entry.getValue());
    }
    jdbcUrl = mysqlSetCursorModeIfNeeded(jdbcUrl, fetchSize);
    return jdbcUrl;
  }

  /**
   * For MySQL Dialect, if Fetchsize is explicitly set by the user or if it's auto-inferred (null),
   * enables `useCursorFetch`. It is disabled only if user explicitly sets FetchSize to 0.
   *
   * @param url DB Url from passed configs.
   * @param fetchSize FetchSize Setting (Null if user has not explicitly set)
   * @return Updated URL with `useCursorFetch` if Fetchsize is not 0. Same as input URL if 0.
   */
  @VisibleForTesting
  String mysqlSetCursorModeIfNeeded(String url, Integer fetchSize) {
    // For MySQL, to enable streaming/cursor mode, useCursorFetch must be true.
    // We enable it if fetchSize is NULL (Auto-infer) or > 0.
    // We only disable it if fetchSize is explicitly 0 (Fetch All).
    if (fetchSize != null && fetchSize == 0) {
      LOG.info(
          "FetchSize is explicitly 0. MySQL cursor mode (useCursorFetch) will not be enabled explicitly.");
      return url;
    }

    LOG.info(
        "FetchSize is {}. Setting MySQL `useCursorFetch=true`.",
        fetchSize == null ? "Auto" : fetchSize);
    return OptionsToConfigBuilder.addParamToJdbcUrl(url, "useCursorFetch", "true");
  }
}
