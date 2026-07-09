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
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.iowrapper.config.defaults.PostgreSQLConfigDefaults;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;

/** PostgreSQL implementation of {@link AbstractJdbcSrcToSpSourceConnector}. */
public class PostgresSrcToSpSourceConnector extends AbstractJdbcSrcToSpSourceConnector {

  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("BIGINT", UnifiedMappingProvider.Type.LONG)
          .put("BIGSERIAL", UnifiedMappingProvider.Type.LONG)
          .put("BIT", UnifiedMappingProvider.Type.BYTES)
          .put("BIT VARYING", UnifiedMappingProvider.Type.BYTES)
          .put("BOOL", UnifiedMappingProvider.Type.BOOLEAN)
          .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
          .put("BYTEA", UnifiedMappingProvider.Type.BYTES)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHARACTER", UnifiedMappingProvider.Type.STRING)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHARACTER VARYING", UnifiedMappingProvider.Type.STRING)
          .put("CITEXT", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a
          // precision and scale are >= 0, map to DECIMAL)
          .put("DECIMAL", UnifiedMappingProvider.Type.NUMBER)
          .put("DOUBLE PRECISION", UnifiedMappingProvider.Type.DOUBLE)
          .put("FLOAT4", UnifiedMappingProvider.Type.FLOAT)
          .put("FLOAT8", UnifiedMappingProvider.Type.DOUBLE)
          .put("INT", UnifiedMappingProvider.Type.INTEGER)
          .put("INTEGER", UnifiedMappingProvider.Type.INTEGER)
          .put("INT2", UnifiedMappingProvider.Type.INTEGER)
          .put("INT4", UnifiedMappingProvider.Type.INTEGER)
          .put("INT8", UnifiedMappingProvider.Type.LONG)
          .put("JSON", UnifiedMappingProvider.Type.JSON)
          .put("JSONB", UnifiedMappingProvider.Type.JSON)
          .put("MONEY", UnifiedMappingProvider.Type.DOUBLE)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a
          // precision and scale are >= 0, map to DECIMAL)
          .put("NUMERIC", UnifiedMappingProvider.Type.NUMBER)
          .put("OID", UnifiedMappingProvider.Type.LONG)
          .put("REAL", UnifiedMappingProvider.Type.FLOAT)
          .put("SERIAL", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL2", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL4", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL8", UnifiedMappingProvider.Type.LONG)
          .put("SMALLINT", UnifiedMappingProvider.Type.INTEGER)
          .put("SMALLSERIAL", UnifiedMappingProvider.Type.INTEGER)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          .put("TIME", UnifiedMappingProvider.Type.TIME)
          .put("TIME WITHOUT TIME ZONE", UnifiedMappingProvider.Type.TIME)
          .put("TIMETZ", UnifiedMappingProvider.Type.TIME_WITH_TIME_ZONE)
          .put("TIME WITH TIME ZONE", UnifiedMappingProvider.Type.TIME_WITH_TIME_ZONE)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TIMESTAMPTZ", UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put("TIMESTAMP WITH TIME ZONE", UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put("TIMESTAMP WITHOUT TIME ZONE", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("UUID", UnifiedMappingProvider.Type.STRING)
          .put("VARBIT", UnifiedMappingProvider.Type.BYTES)
          // TODO: Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("UNSUPPORTED", UnifiedMappingProvider.Type.UNSUPPORTED)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  /**
   * Returns the map of Source Schema to {@link UnifiedTypeMapping} for all supported PostgreSQL
   * types.
   *
   * @return PostgreSQL mapping.
   */
  @Override
  public ImmutableMap<String, UnifiedTypeMapping> getTypeMapping() {
    return MAPPING;
  }

  public String getSourceType() {
    return Constants.POSTGRES_SOURCE_TYPE;
  }

  @Override
  public JdbcValueMappingsProvider getJdbcValueMappingsProvider() {
    return PostgreSQLConfigDefaults.DEFAULT_POSTGRESQL_VALUE_MAPPING_PROVIDER;
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builder()
        .setSourceDbDialect(SQLDialect.POSTGRESQL)
        .setUnifiedTypeMapper(new UnifiedTypeMapper(getTypeMapping()))
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
