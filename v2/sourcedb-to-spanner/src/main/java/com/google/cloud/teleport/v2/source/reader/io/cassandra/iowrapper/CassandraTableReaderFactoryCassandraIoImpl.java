/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraSourceRowMapperFactoryFn;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.localcassandra.CassandraIO;
import org.apache.beam.sdk.io.localcassandra.CassandraIO.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Todo(vardhanvthigle)
 * Switch to upstream cassandra IO once the fix for https://github.com/apache/beam/issues/34160 is available in dataflow.
 */

/**
 * Generate Table Reader For Cassandra using the upstream {@link CassandraIO.Read} implementation.
 */
public class CassandraTableReaderFactoryCassandraIoImpl implements CassandraTableReaderFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(CassandraTableReaderFactoryCassandraIoImpl.class);

  /* Default Connection Timeout in Milliseconds */
  @VisibleForTesting protected static final Integer DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10 * 1000;
  /* Default Read timeout. */
  @VisibleForTesting protected static final Integer DEFAULT_READ_TIMEOUT_MILLIS = 3600 * 1000;

  @VisibleForTesting
  protected static final String DEFAULT_CONSISTENCY = ConsistencyLevel.QUORUM.name();

  /**
   * Returns a Table Reader for given Cassandra Source using the upstream {@link CassandraIO.Read}.
   *
   * @param cassandraDataSource
   * @param sourceSchemaReference
   * @param sourceTableSchema
   * @return table reader for the source.
   */
  @Override
  public PTransform<PBegin, PCollection<SourceRow>> getTableReader(
      CassandraDataSource cassandraDataSource,
      SourceSchemaReference sourceSchemaReference,
      SourceTableSchema sourceTableSchema) {
    CassandraSourceRowMapper cassandraSourceRowMapper =
        getSourceRowMapper(sourceSchemaReference, sourceTableSchema);
    DriverExecutionProfile profile =
        cassandraDataSource.driverConfigLoader().getInitialConfig().getDefaultProfile();
    final Read<SourceRow> tableReader =
        CassandraIO.<SourceRow>read()
            .withTable(sourceTableSchema.tableName())
            .withHosts(
                cassandraDataSource.contactPoints().stream()
                    .map(p -> p.getHostString())
                    .collect(Collectors.toList()))
            .withPort(cassandraDataSource.contactPoints().get(0).getPort())
            .withKeyspace(cassandraDataSource.loggedKeySpace())
            .withLocalDc(cassandraDataSource.localDataCenter())
            .withConsistencyLevel(
                profile.getString(TypedDriverOption.REQUEST_CONSISTENCY.getRawOption()))
            .withConnectTimeout(getConnectionTimeout(profile))
            .withReadTimeout(getReadTimeout(profile))
            .withEntity(SourceRow.class)
            .withCoder(SerializableCoder.of(SourceRow.class))
            .withMapperFactoryFn(
                CassandraSourceRowMapperFactoryFn.create(cassandraSourceRowMapper));
    return setNumPartitions(
        setCredentials(tableReader, profile), cassandraDataSource, sourceTableSchema.tableName());
  }

  @VisibleForTesting
  protected static CassandraIO.Read<SourceRow> setNumPartitions(
      CassandraIO.Read<SourceRow> tableReader, CassandraDataSource dataSource, String tableName) {
    Integer numPartitions = dataSource.numPartitions();
    if (numPartitions != null && numPartitions > 0) {
      LOG.info(
          "Setting numPartitions as {} for DataSource {}, tableName {}",
          numPartitions,
          dataSource,
          tableName);
      return tableReader.withMinNumberOfSplits(numPartitions);
    } else {
      LOG.info(
          "numPartitions would be auto Inferred to number of hosts, for DataSource {}, tableName {}",
          dataSource,
          tableName);
      return tableReader;
    }
  }

  @VisibleForTesting
  protected static String getConsistencyLevel(DriverExecutionProfile profile) {
    String consistencyLevel =
        profile.isDefined(TypedDriverOption.REQUEST_CONSISTENCY.getRawOption())
            ? profile.getString(TypedDriverOption.REQUEST_CONSISTENCY.getRawOption())
            : DEFAULT_CONSISTENCY;
    LOG.info("Set Consistency Level {}", consistencyLevel);
    return consistencyLevel;
  }

  @VisibleForTesting
  protected static Integer getConnectionTimeout(DriverExecutionProfile profile) {
    int timeout =
        profile.isDefined(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption())
            ? (int)
                profile
                    .getDuration(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption())
                    .toMillis()
            : DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    LOG.info("Set Connection Timeout = {} milliseconds", timeout);
    return timeout;
  }

  @VisibleForTesting
  protected static Integer getReadTimeout(DriverExecutionProfile profile) {
    int timeout =
        profile.isDefined(TypedDriverOption.REQUEST_TIMEOUT.getRawOption())
            ? (int) profile.getDuration(TypedDriverOption.REQUEST_TIMEOUT.getRawOption()).toMillis()
            : DEFAULT_READ_TIMEOUT_MILLIS;
    LOG.info("Set Read Timeout = {} milliseconds", timeout);
    return timeout;
  }

  @VisibleForTesting
  protected CassandraIO.Read<SourceRow> setCredentials(
      CassandraIO.Read<SourceRow> tableReader, DriverExecutionProfile profile) {
    if (profile.isDefined(TypedDriverOption.AUTH_PROVIDER_USER_NAME.getRawOption())) {
      tableReader =
          tableReader.withUsername(
              profile.getString(TypedDriverOption.AUTH_PROVIDER_USER_NAME.getRawOption()));
    }
    if (profile.isDefined(TypedDriverOption.AUTH_PROVIDER_PASSWORD.getRawOption())) {
      tableReader =
          tableReader.withPassword(
              profile.getString(TypedDriverOption.AUTH_PROVIDER_PASSWORD.getRawOption()));
    }
    return tableReader;
  }

  private CassandraSourceRowMapper getSourceRowMapper(
      SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
    return CassandraSourceRowMapper.builder()
        .setSourceTableSchema(sourceTableSchema)
        .setSourceSchemaReference(sourceSchemaReference)
        .build();
  }
}
