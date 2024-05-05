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
import org.apache.beam.sdk.util.FluentBackoff;

/**
 * Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
@AutoValue
public abstract class JdbcIOWrapperConfig {

  /** Source Endpoint. */
  public abstract String sourceHost();

  /** Source Port. */
  public abstract String sourcePort();

  /** {@link SourceSchemaReference}. */
  public abstract SourceSchemaReference sourceSchemaReference();

  /** Table Configurations. */
  public abstract ImmutableList<TableConfig> tableConfigs();

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

  /*
   * Properties string to use for the JDBC connection.
   * Format of the string must be [propertyName=property;]
   * Defaults to a vetted configuration based on benchmarking results.
   * Example:
   *    "maxTotal=160;maxpoolsize=160;maxIdle=160;minIdle=160"
   *       + ";wait_timeout=57600"
   *        + ";interactive_timeout=57600"
   *        + ";idletimeout=3600"
   *        + ";maxwaittime=600_000"
   *        + ";maxWaitMillis=600_000"
   *        + ";maxConnLifetimeMillis=600_000"
   *        +
   * ";testOnCreate=true;testOnBorrow=true;testOnReturn=true;testWhileIdle=true"
   */
  public abstract String connectionProperties();

  /** Auto Reconnect for dropped connections. */
  public abstract Boolean autoReconnect();

  /** Reconnect Attempts for Auto Reconnect default 10. */
  public abstract Long reconnectAttempts();

  /** Max Number of connections. */
  public abstract Long maxConnections();

  /** BackOff Strategy for Schema Discovery retries. Defaults to {@link FluentBackoff#DEFAULT}. */
  public abstract FluentBackoff schemaDiscoveryBackOff();

  public static Builder builderWithMySqlDefualts() {
    return new AutoValue_JdbcIOWrapperConfig.Builder()
        .setSchemaMapperType(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_MAPPER_TYPE)
        .setDialectAdapter(MySqlConfigDefaults.DEFAULT_MYSQL_DIALECT_ADAPTER)
        .setValueMappingsProvider(MySqlConfigDefaults.DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER)
        .setAutoReconnect(MySqlConfigDefaults.DEFAULT_MYSQL_AUTO_RECONNECT)
        .setReconnectAttempts(MySqlConfigDefaults.DEFAULT_MYSQL_RECONNECT_ATTEMPTS)
        .setConnectionProperties(MySqlConfigDefaults.DEFAULT_MYSQL_CONNECTION_PROPERTIES)
        .setMaxConnections(MySqlConfigDefaults.DEFAULT_MYSQL_MAX_CONNECTIONS)
        .setSchemaDiscoveryBackOff(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceHost(String value);

    public abstract Builder setSourcePort(String value);

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public abstract Builder setTableConfigs(ImmutableList<TableConfig> value);

    public abstract Builder setShardID(String value);

    public abstract Builder setDbAuth(DbAuth value);

    public abstract Builder setSchemaMapperType(MapperType value);

    public abstract Builder setDialectAdapter(DialectAdapter value);

    public abstract Builder setValueMappingsProvider(JdbcValueMappingsProvider value);

    public abstract Builder setJdbcDriverJars(String value);

    public abstract Builder setJdbcDriverClassName(String value);

    public abstract Builder setConnectionProperties(String value);

    public abstract Builder setReconnectAttempts(Long value);

    public abstract Builder setAutoReconnect(Boolean value);

    public abstract Builder setSchemaDiscoveryBackOff(FluentBackoff value);

    public abstract Builder setMaxConnections(Long value);

    public abstract JdbcIOWrapperConfig build();
  }
}
