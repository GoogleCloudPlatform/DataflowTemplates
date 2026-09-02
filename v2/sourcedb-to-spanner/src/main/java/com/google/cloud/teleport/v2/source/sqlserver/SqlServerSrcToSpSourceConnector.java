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

import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.iowrapper.config.defaults.SqlServerConfigDefaults;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerSrcToSpSourceConnector extends AbstractJdbcSrcToSpSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSrcToSpSourceConnector.class);

  // Based on sqlserver-datatype.csv mapping matrix
  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedTypeMapping>builder()
          .putAll(
              ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
                  .put("TINYINT", UnifiedMappingProvider.Type.LONG)
                  .put("SMALLINT", UnifiedMappingProvider.Type.LONG)
                  .put("INT", UnifiedMappingProvider.Type.LONG)
                  .put("BIGINT", UnifiedMappingProvider.Type.LONG)
                  .put("BIT", UnifiedMappingProvider.Type.BOOLEAN)
                  .put("DECIMAL", UnifiedMappingProvider.Type.DECIMAL)
                  .put("NUMERIC", UnifiedMappingProvider.Type.DECIMAL)
                  .put("MONEY", UnifiedMappingProvider.Type.DECIMAL)
                  .put("SMALLMONEY", UnifiedMappingProvider.Type.DECIMAL)
                  .put("FLOAT", UnifiedMappingProvider.Type.DOUBLE)
                  .put("REAL", UnifiedMappingProvider.Type.FLOAT)
                  .put("DATE", UnifiedMappingProvider.Type.DATE)
                  .put("TIME", UnifiedMappingProvider.Type.STRING)
                  .put("DATETIME2", UnifiedMappingProvider.Type.TIMESTAMP)
                  .put("DATETIMEOFFSET", UnifiedMappingProvider.Type.TIMESTAMP)
                  .put("DATETIME", UnifiedMappingProvider.Type.TIMESTAMP)
                  .put("SMALLDATETIME", UnifiedMappingProvider.Type.TIMESTAMP)
                  .put("CHAR", UnifiedMappingProvider.Type.STRING)
                  .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
                  .put("TEXT", UnifiedMappingProvider.Type.STRING)
                  .put("NCHAR", UnifiedMappingProvider.Type.STRING)
                  .put("NVARCHAR", UnifiedMappingProvider.Type.STRING)
                  .put("NTEXT", UnifiedMappingProvider.Type.STRING)
                  .put("BINARY", UnifiedMappingProvider.Type.BYTES)
                  .put("VARBINARY", UnifiedMappingProvider.Type.BYTES)
                  .put("IMAGE", UnifiedMappingProvider.Type.BYTES)
                  .put("ROWVERSION", UnifiedMappingProvider.Type.BYTES)
                  .put("TIMESTAMP", UnifiedMappingProvider.Type.BYTES)
                  .put(
                      "UNIQUEIDENTIFIER",
                      UnifiedMappingProvider.Type
                          .STRING) // UUID mapped to STRING in mapping provider? UUID is
                  // BYTES/STRING in
                  // Spanner. We can map to STRING
                  .put("XML", UnifiedMappingProvider.Type.STRING)
                  .put("JSON", UnifiedMappingProvider.Type.JSON)
                  .build()
                  .entrySet()
                  .stream()
                  .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
                  .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue)))
          .put("VECTOR", UnifiedMappingProvider.getArrayMapping(UnifiedMappingProvider.Type.DOUBLE))
          .build();

  @Override
  public ImmutableMap<String, UnifiedTypeMapping> getTypeMapping() {
    return MAPPING;
  }

  public String getSourceType() {
    return Constants.SQLSERVER_SOURCE_TYPE;
  }

  @Override
  public JdbcValueMappingsProvider getJdbcValueMappingsProvider() {
    return SqlServerConfigDefaults.DEFAULT_SQLSERVER_VALUE_MAPPING_PROVIDER;
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builder()
        .setSourceDbDialect(SQLDialect.SQLSERVER)
        .setUnifiedTypeMapper(new UnifiedTypeMapper(getTypeMapping()))
        .setDialectAdapter(SqlServerConfigDefaults.DEFAULT_SQLSERVER_DIALECT_ADAPTER)
        .setValueMappingsProvider(SqlServerConfigDefaults.DEFAULT_SQLSERVER_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(SqlServerConfigDefaults.DEFAULT_SQLSERVER_MAX_CONNECTIONS)
        .setSqlInitSeq(SqlServerConfigDefaults.DEFAULT_SQLSERVER_INIT_SEQ)
        .setSchemaDiscoveryBackOff(
            SqlServerConfigDefaults.DEFAULT_SQLSERVER_SCHEMA_DISCOVERY_BACKOFF)
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
    return SourceSchemaReference.ofJdbc(
        JdbcSchemaReference.builder().setDbName(dbName).setNamespace(namespace).build());
  }

  @Override
  public String getJdbcUrl(
      String host,
      int port,
      String dbName,
      String connectionProperties,
      String namespace,
      Integer fetchSize) {
    String jdbcUrl =
        "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + dbName + ";encrypt=false";
    if (StringUtils.isNotBlank(connectionProperties)) {
      jdbcUrl = jdbcUrl + ";" + connectionProperties;
    }
    return jdbcUrl;
  }
}
