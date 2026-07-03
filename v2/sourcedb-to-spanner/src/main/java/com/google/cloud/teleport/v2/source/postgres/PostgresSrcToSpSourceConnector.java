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
package com.google.cloud.teleport.v2.source.postgres;

import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.iowrapper.config.defaults.PostgreSQLConfigDefaults;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

/** PostgreSQL implementation of {@link AbstractJdbcSrcToSpSourceConnector}. */
public class PostgresSrcToSpSourceConnector extends AbstractJdbcSrcToSpSourceConnector {

  @Override
  public JdbcValueMappingsProvider getJdbcValueMappingsProvider() {
    return PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_VALUE_MAPPING_PROVIDER;
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builder()
        .setSourceDbDialect(SQLDialect.POSTGRESQL)
        .setSchemaMapperType(PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_SCHEMA_MAPPER_TYPE)
        .setDialectAdapter(PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_DIALECT_ADAPTER)
        .setValueMappingsProvider(
            PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_MAX_CONNECTIONS)
        .setSqlInitSeq(PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_INIT_SEQ)
        .setSchemaDiscoveryBackOff(
            PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_SCHEMA_DISCOVERY_BACKOFF)
        .setTables(ImmutableList.of())
        .setTableVsPartitionColumns(ImmutableMap.of())
        .setMaxPartitions(null)
        .setWaitOn(null)
        .setMaxFetchSize(null)
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
    JdbcSchemaReference.Builder builder = JdbcSchemaReference.builder().setDbName(dbName);
    if (StringUtils.isBlank(namespace)) {
      builder.setNamespace("public");
    } else {
      builder.setNamespace(namespace);
    }
    return SourceSchemaReference.ofJdbc(builder.build());
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
      jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    }
    if (StringUtils.isBlank(namespace)) {
      namespace = "public";
    }
    // TODO check for scenarios where URL already has a ?
    jdbcUrl = jdbcUrl + "?currentSchema=" + namespace;
    if (StringUtils.isNotBlank(connectionProperties)) {
      jdbcUrl = jdbcUrl + "&" + connectionProperties;
    }
    return jdbcUrl;
  }
}
