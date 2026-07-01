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
package com.google.cloud.teleport.v2.source.cassandra.reader.io.cassandra.iowrapper;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.reader.auth.dbauth.GuardedStringValueProvider;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.cassandra.reader.io.cassandra.iowrapper.CassandraDataSource.CassandraDialect;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.AstraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.CassandraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Wait.OnSignal;

@AutoValue
public abstract class CassandraIOWrapperFactory implements IoWrapperFactory {

  /** Options Map for Cassandra Driver Config. */
  @Nullable
  public abstract OptionsMap optionsMap();

  /**
   * Number of partitions to read from. Defaults to Null.
   *
   * @see CassandraDataSource#numPartitions()
   */
  @Nullable
  public abstract Integer numPartitions();

  /** Cassandra Dialect. */
  public abstract CassandraDataSource.CassandraDialect cassandraDialect();

  /** Astra DB Token. * */
  public abstract GuardedStringValueProvider astraDBToken();

  /** Astra DB Database ID. * */
  public abstract String astraDBDatabaseId();

  /** Astra DB Keyspace. * */
  public abstract String astraDBKeyspace();

  /** Astra DB Region. * */
  public abstract String astraDBRegion();

  private static CassandraIOWrapperFactory create(
      OptionsMap optionsMap,
      Integer numPartions,
      String sourceDialect,
      GuardedStringValueProvider astraDBToken,
      String astraDBDatabaseId,
      String astraDBKeyspace,
      String astraDBRegion) {
    CassandraDataSource.CassandraDialect cassandraDialect =
        switch (sourceDialect) {
          case SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT -> CassandraDialect.ASTRA;
          default -> CassandraDialect.OSS;
        };
    return new AutoValue_CassandraIOWrapperFactory(
        optionsMap,
        numPartions,
        cassandraDialect,
        astraDBToken,
        astraDBDatabaseId,
        astraDBKeyspace,
        astraDBRegion);
  }

  public static CassandraIOWrapperFactory fromConfig(
      SourceDbToSpannerOptions options, SourceConnectionConfig sourceConnectionConfig) {
    Preconditions.checkArgument(
        options.getSourceDbDialect().equals(SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT)
            || options
                .getSourceDbDialect()
                .equals(SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT),
        "Unexpected Dialect " + options.getSourceDbDialect() + " for Cassandra Source");

    GuardedStringValueProvider astraDBToken = GuardedStringValueProvider.create("");
    String astraDBDatabaseId = "";
    String astraDBKeyspace = "";
    String astraDBRegion = "";
    OptionsMap optionsMap = null;

    if (sourceConnectionConfig instanceof AstraConnectionConfig) {
      AstraConnectionConfig astraConfig = (AstraConnectionConfig) sourceConnectionConfig;
      astraDBToken = GuardedStringValueProvider.create(astraConfig.getAstraToken());
      astraDBDatabaseId = astraConfig.getDatabaseId();
      astraDBKeyspace = astraConfig.getKeySpace();
      astraDBRegion = astraConfig.getAstraDbRegion();
    } else if (sourceConnectionConfig instanceof CassandraConnectionConfig) {
      optionsMap = ((CassandraConnectionConfig) sourceConnectionConfig).getOptionsMap();
    } else {
      throw new IllegalArgumentException(
          "Unsupported source connection config type: " + sourceConnectionConfig);
    }

    return CassandraIOWrapperFactory.create(
        optionsMap,
        options.getNumPartitions(),
        options.getSourceDbDialect(),
        astraDBToken,
        astraDBDatabaseId,
        astraDBKeyspace,
        astraDBRegion);
  }

  /** Create an {@link IoWrapper} instance for a list of SourceTables. */
  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, OnSignal<?> waitOnSignal) {
    /** TODO(vardhanvthigle@) incorporate waitOnSignal */
    return new CassandraIoWrapper(
        optionsMap(),
        sourceTables,
        numPartitions(),
        cassandraDialect(),
        astraDBToken(),
        astraDBDatabaseId(),
        astraDBKeyspace(),
        astraDBRegion());
  }
}
