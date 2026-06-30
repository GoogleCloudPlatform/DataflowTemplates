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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.reader.auth.dbauth.GuardedStringValueProvider;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.cassandra.reader.io.cassandra.iowrapper.CassandraDataSource.CassandraDialect;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.AstraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.commons.lang3.StringUtils;

@AutoValue
public abstract class CassandraIOWrapperFactory implements IoWrapperFactory {

  /** GCS Path for Cassandra Driver Config. */
  public abstract String gcsConfigPath();

  /**
   * Number of partitions to read from. Defaults to Null.
   *
   * @see CassandraDataSource#numPartitions()
   */
  @Nullable
  public abstract Integer numPartitions();

  /** Cassandra Dialect. */
  public abstract CassandraDataSource.CassandraDialect cassandraDialect();

  /** Astra DB options. Empty for OSS dialect. */
  /** Astra DB Token. * */
  public abstract GuardedStringValueProvider astraDBToken();

  /** Astra DB Database ID. * */
  public abstract String astraDBDatabaseId();

  /** Astra DB Keyspace. * */
  public abstract String astraDBKeyspace();

  /** Astra DB Keyspace. * */
  public abstract String astraDBRegion();

  private static CassandraIOWrapperFactory create(
      String gcsConfigPath,
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
        gcsConfigPath,
        numPartions,
        cassandraDialect,
        astraDBToken,
        astraDBDatabaseId,
        astraDBKeyspace,
        astraDBRegion);
  }

  public static CassandraIOWrapperFactory fromConfig(
      SourceDbToSpannerOptions options, SourceConnectionConfig sourceConnectionConfig) {
    String gcsPath = options.getSourceConfigURL();
    // Implementation Details. the pipeline options are strings.
    Preconditions.checkArgument(
        options.getSourceDbDialect().equals(SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT)
            || options
                .getSourceDbDialect()
                .equals(SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT),
        "Unexpected Dialect " + options.getSourceDbDialect() + " for Cassandra Source");
    Preconditions.checkArgument(
        options.getSourceDbDialect().equals(SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT)
            || StringUtils.startsWith(gcsPath, "gs://"),
        "GCS path Expected in place of `" + gcsPath + "`.");

    GuardedStringValueProvider astraDBToken = GuardedStringValueProvider.create("");
    String astraDBDatabaseId = "";
    String astraDBKeyspace = "";
    String astraDBRegion = "";

    if (sourceConnectionConfig instanceof AstraConnectionConfig) {
      AstraConnectionConfig astraConfig = (AstraConnectionConfig) sourceConnectionConfig;
      astraDBToken = GuardedStringValueProvider.create(astraConfig.getAstraToken());
      astraDBDatabaseId = astraConfig.getDatabaseId();
      astraDBKeyspace = astraConfig.getKeySpace();
      astraDBRegion = astraConfig.getAstraDbRegion();
    }

    return CassandraIOWrapperFactory.create(
        options.getSourceConfigURL(),
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
        gcsConfigPath(),
        sourceTables,
        numPartitions(),
        cassandraDialect(),
        astraDBToken(),
        astraDBDatabaseId(),
        astraDBKeyspace(),
        astraDBRegion());
  }
}
