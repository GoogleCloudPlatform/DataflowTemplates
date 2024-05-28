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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.DbAuth;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults.MySqlConfigDefaults;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.FluentBackoff;

/**
 * Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
@AutoValue
public abstract class JdbcIOWrapperConfig {

  /** Source URL. */
  public abstract String sourceDbURL();

  /** {@link SourceSchemaReference}. */
  public abstract SourceSchemaReference sourceSchemaReference();

  /** List of Tables to migrate. Auto-inferred if emtpy. */
  public abstract ImmutableList<String> tables();

  /** Configured Partition Column. If unspecified for a table, it's auto-inferred. */
  public abstract ImmutableMap<String, ImmutableList<String>> tableVsPartitionColumns();

  /** Shard ID. */
  public abstract String shardID();

  /** DB credentials. */
  public abstract DbAuth dbAuth();

  /*
   * A comma-separated list of driver JAR files. (Example:
   * "gs://bucket/driver_jar1.jar,gs://bucket/driver_jar2.jar")
   */
  public abstract String jdbcDriverJars();

  /* Name of the JDbc Driver Class. */
  public abstract String jdbcDriverClassName();

  /** Schema Mapper Type, defaults to MySQl. */
  public abstract MapperType schemaMapperType();

  /** Dialect Adapter. */
  public abstract DialectAdapter dialectAdapter();

  /** Source Row Mapping Provider. */
  public abstract JdbcValueMappingsProvider valueMappingsProvider();

  /** Max Number of connections. */
  public abstract Long maxConnections();

  /** BackOff Strategy for Schema Discovery retries. Defaults to {@link FluentBackoff#DEFAULT}. */
  public abstract FluentBackoff schemaDiscoveryBackOff();

  /**
   * Max number of read partitions. If not-null uses the user supplied maxPartitions, instead of
   * auto-inference. defaults to null.
   */
  @Nullable
  public abstract Integer maxPartitions();

  /**
   * Configures the size of data read in db, per db read call. Defaults to beam's DEFAULT_FETCH_SIZE
   * of 50_000. For manually fine-tuning this, take into account the read ahead buffer pool settings
   * (innodb_read_ahead_threshold) and the worker memory.
   */
  @Nullable
  public abstract Integer maxFetchSize();

  public static Builder builderWithMySqlDefaults() {
    return new AutoValue_JdbcIOWrapperConfig.Builder()
        .setSchemaMapperType(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_MAPPER_TYPE)
        .setDialectAdapter(MySqlConfigDefaults.DEFAULT_MYSQL_DIALECT_ADAPTER)
        .setValueMappingsProvider(MySqlConfigDefaults.DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(MySqlConfigDefaults.DEFAULT_MYSQL_MAX_CONNECTIONS)
        .setSchemaDiscoveryBackOff(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF)
        .setTables(ImmutableList.of())
        .setTableVsPartitionColumns(ImmutableMap.of())
        .setMaxPartitions(null)
        .setMaxFetchSize(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceDbURL(String value);

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public abstract Builder setTables(ImmutableList<String> value);

    public abstract Builder setTableVsPartitionColumns(
        ImmutableMap<String, ImmutableList<String>> value);

    public abstract Builder setShardID(String value);

    public abstract Builder setDbAuth(DbAuth value);

    public abstract Builder setSchemaMapperType(MapperType value);

    public abstract Builder setDialectAdapter(DialectAdapter value);

    public abstract Builder setValueMappingsProvider(JdbcValueMappingsProvider value);

    public abstract Builder setJdbcDriverJars(String value);

    public abstract Builder setJdbcDriverClassName(String value);

    public abstract Builder setSchemaDiscoveryBackOff(FluentBackoff value);

    public abstract Builder setMaxPartitions(Integer value);

    public abstract Builder setMaxFetchSize(Integer value);

    public abstract Builder setMaxConnections(Long value);

    public abstract JdbcIOWrapperConfig build();
  }
}
