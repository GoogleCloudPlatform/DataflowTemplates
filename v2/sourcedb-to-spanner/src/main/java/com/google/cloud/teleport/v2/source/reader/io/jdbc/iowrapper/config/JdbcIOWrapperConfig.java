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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.DbAuth;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults.MySqlConfigDefaults;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults.PostgreSQLConfigDefaults;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.ReadWithUniformPartitions;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference.Kind;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
@AutoValue
public abstract class JdbcIOWrapperConfig {
  /** Dialect of the database. */
  public abstract SQLDialect sourceDbDialect();

  /** Source URL. */
  public abstract String sourceDbURL();

  /** {@link SoucreSchemaReference}. */
  public abstract SourceSchemaReference sourceSchemaReference();

  public JdbcSchemaReference jdbcSourceSchemaReference() {
    return sourceSchemaReference().jdbc();
  }

  /** List of Tables to migrate. Auto-inferred if emtpy. */
  public abstract ImmutableList<String> tables();

  /** Configured Partition Column. If unspecified for a table, it's auto-inferred. */
  public abstract ImmutableMap<String, ImmutableList<String>> tableVsPartitionColumns();

  /** Shard ID. */
  @Nullable
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

  /** Sequence of Sql Init statements for the connection. */
  public abstract ImmutableList<String> sqlInitSeq();

  /**
   * Temporary, internal feature flag for reader to enable or disable {@link
   * ReadWithUniformPartitions}. Defaults to true.
   */
  public abstract Boolean readWithUniformPartitionsFeatureEnabled();

  /**
   * PCollections to wait on before doing the read of configured tables. Ignored if {@link
   * JdbcIOWrapperConfig#readWithUniformPartitionsFeatureEnabled()} is false. Defaults to null.
   */
  @Nullable
  public abstract OnSignal<?> waitOn();

  /**
   * If not null, maximum number of parallel queries issued to the DB during split process. Ignored
   * if {@link JdbcIOWrapperConfig#readWithUniformPartitionsFeatureEnabled()} is false. It's best to
   * set this to a number close to number of cores available on mySql server. Defaults to {@link
   * JdbcIOWrapperConfig#DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS}.
   *
   * <p><b>Performance</b>
   *
   * <ul>
   *   <li>Ensure that <a
   *       href=https://dev.mysql.com/doc/refman/8.4/en/innodb-performance-multiple_io_threads.html>innodb_read_io_threads</a>
   *       is set to the default value as recommended by Mysql or higher. If the partitioning is
   *       slow due to too many queries timing out in each stage, and if the {@code SHOW ENGINE
   *       INNODB STATUS} shows pending queries close to innodb_read_io_threads, it's an indication
   *       to increase this setting.
   *   <li>Ensure that <a
   *       href=https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_buffer_pool_size>sysvar_innodb_buffer_pool_size</a>
   *       is set to the default value as recommended by Mysql or higher.
   * </ul>
   */
  @Nullable
  public abstract Integer dbParallelizationForSplitProcess();

  private static final int DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS = 100;

  /**
   * If not null, maximum number of parallel queries issued to the DB for reads. Ignored if {@link
   * JdbcIOWrapperConfig#readWithUniformPartitionsFeatureEnabled()} is false. Defaults to null.
   */
  @Nullable
  public abstract Integer dbParallelizationForReads();

  /**
   * A transform that can be injected to make use of the discovered splits for additional use case
   * like creating split points on spanner before the actual read. Ignored if {@link
   * JdbcIOWrapperConfig#readWithUniformPartitionsFeatureEnabled()} is false. Defaults to null.
   */
  @Nullable
  public abstract PTransform<PCollection<ImmutableList<Range>>, ?> additionalOperationsOnRanges();

  /**
   * Sets the {@code testOnBorrow} property. This property determines whether or not the pool will
   * validate objects before they are borrowed from the pool. Defaults to True.
   */
  public abstract Boolean testOnBorrow();

  private static final Boolean DEFAULT_TEST_ON_BORROW = true;

  /**
   * Sets the {@code testOnCreate} property. This property determines whether or not the pool will
   * validate objects immediately after they are created by the pool. Defaults to True.
   */
  public abstract Boolean testOnCreate();

  private static final Boolean DEFAULT_TEST_ON_CREATE = true;

  /**
   * Sets the {@code testOnReturn} property. This property determines whether or not the pool will
   * validate objects before they are returned to the pool. Defaults to True.
   */
  public abstract Boolean testOnReturn();

  private static final Boolean DEFAULT_TEST_ON_RETURN = true;

  /**
   * Sets the {@code testWhileIdle} property. This property determines whether or not the idle
   * object evictor will validate connections. Defaults to True.
   */
  public abstract Boolean testWhileIdle();

  private static final Boolean DEFAULT_TEST_WILE_IDLE = true;

  /** Sets the {@code validationQuery}. */
  public abstract String validationQuery();

  private static final String DEFAULT_VALIDATEION_QUERY = "SELECT 1";

  /** Sets the connectivity timeout in seconds during schema discovery. * */
  public abstract Integer schemaDiscoveryConnectivityTimeoutMilliSeconds();

  private static final Integer DEFAULT_SCHEMA_DISCOVERY_CONNECTIVITY_TIMEOUT_MILLISECONDS =
      30 * 1000;

  /**
   * The timeout in seconds before an abandoned connection can be removed.
   *
   * <p>Creating a Statement, PreparedStatement or CallableStatement or using one of these to
   * execute a query (using one of the execute methods) resets the lastUsed property of the parent
   * connection.
   *
   * <p>Abandoned connection cleanup happens when:
   *
   * <ul>
   *   <li>{@link BasicDataSource#getRemoveAbandonedOnBorrow()} or {@link
   *       BasicDataSource#getRemoveAbandonedOnMaintenance()} = true
   *   <li>{@link BasicDataSource#getNumIdle() numIdle} &lt; 2
   *   <li>{@link BasicDataSource#getNumActive() numActive} &gt; {@link
   *       BasicDataSource#getMaxTotal() maxTotal} - 3
   * </ul>
   *
   * Defaults to hours.
   *
   * <p>
   */
  public abstract Integer removeAbandonedTimeout();

  private static final Integer DEFAULT_REMOVE_ABANDONED_TIMEOUT = 8 * 3600;

  /**
   * The minimum amount of time an object may sit idle in the pool before it is eligible for
   * eviction by the idle object evictor. Defaults to hours.
   */
  public abstract Integer minEvictableIdleTimeMillis();

  private static final Integer DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 8 * 3600 * 1000;

  public abstract Builder toBuilder();

  public static Builder builderWithMySqlDefaults() {
    return new AutoValue_JdbcIOWrapperConfig.Builder()
        .setSourceDbDialect(SQLDialect.MYSQL)
        .setSchemaMapperType(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_MAPPER_TYPE)
        .setDialectAdapter(MySqlConfigDefaults.DEFAULT_MYSQL_DIALECT_ADAPTER)
        .setValueMappingsProvider(MySqlConfigDefaults.DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER)
        .setMaxConnections(MySqlConfigDefaults.DEFAULT_MYSQL_MAX_CONNECTIONS)
        .setSqlInitSeq(MySqlConfigDefaults.DEFAULT_MYSQL_INIT_SEQ)
        .setSchemaDiscoveryBackOff(MySqlConfigDefaults.DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF)
        .setTables(ImmutableList.of())
        .setTableVsPartitionColumns(ImmutableMap.of())
        .setMaxPartitions(null)
        .setWaitOn(null)
        .setMaxFetchSize(null)
        .setDbParallelizationForReads(null)
        .setDbParallelizationForSplitProcess(DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS)
        .setReadWithUniformPartitionsFeatureEnabled(true)
        .setTestOnBorrow(DEFAULT_TEST_ON_BORROW)
        .setTestOnCreate(DEFAULT_TEST_ON_CREATE)
        .setTestOnReturn(DEFAULT_TEST_ON_RETURN)
        .setTestWhileIdle(DEFAULT_TEST_WILE_IDLE)
        .setValidationQuery(DEFAULT_VALIDATEION_QUERY)
        .setRemoveAbandonedTimeout(DEFAULT_REMOVE_ABANDONED_TIMEOUT)
        .setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS)
        .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(
            DEFAULT_SCHEMA_DISCOVERY_CONNECTIVITY_TIMEOUT_MILLISECONDS);
  }

  public static Builder builderWithPostgreSQLDefaults() {
    return new AutoValue_JdbcIOWrapperConfig.Builder()
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
        .setDbParallelizationForSplitProcess(DEFAULT_PARALLELIZATION_FOR_SLIT_PROCESS)
        .setReadWithUniformPartitionsFeatureEnabled(true)
        .setTestOnBorrow(DEFAULT_TEST_ON_BORROW)
        .setTestOnCreate(DEFAULT_TEST_ON_CREATE)
        .setTestOnReturn(DEFAULT_TEST_ON_RETURN)
        .setTestWhileIdle(DEFAULT_TEST_WILE_IDLE)
        .setValidationQuery(DEFAULT_VALIDATEION_QUERY)
        .setRemoveAbandonedTimeout(DEFAULT_REMOVE_ABANDONED_TIMEOUT)
        .setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS)
        .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(
            DEFAULT_SCHEMA_DISCOVERY_CONNECTIVITY_TIMEOUT_MILLISECONDS);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSourceDbDialect(SQLDialect value);

    public abstract Builder setSourceDbURL(String value);

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public Builder setSourceSchemaReference(JdbcSchemaReference value) {
      return setSourceSchemaReference(SourceSchemaReference.ofJdbc(value));
    }

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

    public abstract Builder setSqlInitSeq(ImmutableList<String> value);

    public abstract Builder setReadWithUniformPartitionsFeatureEnabled(Boolean value);

    public abstract Builder setWaitOn(@Nullable OnSignal<?> value);

    public abstract Builder setDbParallelizationForSplitProcess(@Nullable Integer value);

    public abstract Builder setDbParallelizationForReads(@Nullable Integer value);

    public abstract Builder setAdditionalOperationsOnRanges(
        @Nullable PTransform<PCollection<ImmutableList<Range>>, ?> value);

    public abstract Builder setTestOnBorrow(Boolean value);

    public abstract Builder setTestOnCreate(Boolean value);

    public abstract Builder setTestOnReturn(Boolean value);

    public abstract Builder setTestWhileIdle(Boolean value);

    public abstract Builder setValidationQuery(String value);

    public abstract Builder setSchemaDiscoveryConnectivityTimeoutMilliSeconds(Integer value);

    public abstract Builder setRemoveAbandonedTimeout(Integer value);

    public abstract Builder setMinEvictableIdleTimeMillis(Integer value);

    public abstract Builder setMaxConnections(Long value);

    public abstract JdbcIOWrapperConfig autoBuild();

    public JdbcIOWrapperConfig build() {
      JdbcIOWrapperConfig config = autoBuild();
      checkState(config.sourceSchemaReference().getKind() == Kind.JDBC);
      return config;
    }
  }
}
