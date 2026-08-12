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

import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.iowrapper.config.defaults.OracleConfigDefaults;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;

public class OracleSrcToSpSourceConnector extends AbstractJdbcSrcToSpSourceConnector {

  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("VARCHAR2", UnifiedMappingProvider.Type.STRING)
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          .put("NVARCHAR2", UnifiedMappingProvider.Type.STRING)
          .put("NCHAR", UnifiedMappingProvider.Type.STRING)
          .put("NUMBER", UnifiedMappingProvider.Type.NUMBER)
          .put("DECIMAL", UnifiedMappingProvider.Type.DECIMAL)
          .put("FLOAT", UnifiedMappingProvider.Type.FLOAT)
          .put("DOUBLE PRECISION", UnifiedMappingProvider.Type.DOUBLE)
          .put("BINARY_FLOAT", UnifiedMappingProvider.Type.FLOAT)
          .put("BINARY_DOUBLE", UnifiedMappingProvider.Type.DOUBLE)
          .put("INTEGER", UnifiedMappingProvider.Type.LONG)
          .put("INT", UnifiedMappingProvider.Type.LONG)
          .put("SMALLINT", UnifiedMappingProvider.Type.LONG)
          .put("DATE", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TIMESTAMP WITH TIME ZONE", UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put(
              "TIMESTAMP WITH LOCAL TIME ZONE",
              UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put("RAW", UnifiedMappingProvider.Type.BYTES)
          .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
          .put("CLOB", UnifiedMappingProvider.Type.STRING)
          .put("NCLOB", UnifiedMappingProvider.Type.STRING)
          .put("BLOB", UnifiedMappingProvider.Type.BYTES)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  @Override
  public ImmutableMap<String, UnifiedTypeMapping> getTypeMapping() {
    return MAPPING;
  }

  public String getSourceType() {
    return Constants.ORACLE_SOURCE_TYPE;
  }

  @Override
  public JdbcValueMappingsProvider getJdbcValueMappingsProvider() {
    return OracleConfigDefaults.DEFAULT_ORACLE_VALUE_MAPPING_PROVIDER;
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builder()
        .setSourceDbDialect(SQLDialect.ORACLE)
        .setUnifiedTypeMapper(new UnifiedTypeMapper(getTypeMapping()))
        .setDialectAdapter(OracleConfigDefaults.DEFAULT_ORACLE_DIALECT_ADAPTER)
        .setValueMappingsProvider(OracleConfigDefaults.DEFAULT_ORACLE_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(OracleConfigDefaults.DEFAULT_ORACLE_MAX_CONNECTIONS)
        .setSqlInitSeq(OracleConfigDefaults.DEFAULT_ORACLE_INIT_SEQ)
        .setSchemaDiscoveryBackOff(OracleConfigDefaults.DEFAULT_ORACLE_SCHEMA_DISCOVERY_BACKOFF)
        .setTables(ImmutableList.of())
        .setTableVsPartitionColumns(ImmutableMap.of())
        .setReadWithUniformPartitionsFeatureEnabled(true)
        .setMaxPartitions(null)
        .setWaitOn(null)
        .setDbParallelizationForReads(null)
        .setDbParallelizationForSplitProcess(
            JdbcIOWrapperConfig.DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS)
        .setTestOnBorrow(JdbcIOWrapperConfig.DEFAULT_TEST_ON_BORROW)
        .setTestOnCreate(JdbcIOWrapperConfig.DEFAULT_TEST_ON_CREATE)
        .setTestOnReturn(JdbcIOWrapperConfig.DEFAULT_TEST_ON_RETURN)
        .setTestWhileIdle(JdbcIOWrapperConfig.DEFAULT_TEST_WILE_IDLE)
        .setValidationQuery("SELECT 1 FROM DUAL")
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
    String url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName;
    if (StringUtils.isNotBlank(connectionProperties)) {
      url = url + "?" + connectionProperties;
    }
    // Set explicit fetchSize via options builder mapping...
    return url;
  }
}
