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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.DbAuth;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Encapsulates details of a Cassandra Cluster. Cassandra Cluster can connect to multiple KeySpaces,
 * just like a Mysql instance can have multiple databases. TODO(vardhanvthigle): Take
 * DriverConfiguration as a GCS file for advanced overrides.
 */
@AutoValue
public abstract class CassandraDataSource implements Serializable {

  /** Name of the Cassandra Cluster. */
  public abstract String clusterName();

  /** Name of local Datacenter. Must be specified if contactPoints are not empty */
  @Nullable
  public abstract String localDataCenter();

  /** Contact points for connecting to a Cassandra Cluster. */
  public abstract ImmutableList<InetSocketAddress> contactPoints();

  /** Cassandra Auth details. */
  @Nullable
  public abstract DbAuth dbAuth();

  /** Retry Policy for Cassandra Driver. Defaults to {@link DefaultRetryPolicy}. */
  public abstract Class retryPolicy();

  /** Consistency level for reading the source. Defaults to {@link ConsistencyLevel#QUORUM} */
  public abstract ConsistencyLevel consistencyLevel();

  /** Connection timeout for Cassandra driver. Set null for driver default. */
  @Nullable
  public abstract Duration connectTimeout();

  /** Read timeout for Cassandra driver. Set null for driver default. */
  @Nullable
  public abstract Duration requestTimeout();

  public static Builder builder() {
    return new AutoValue_CassandraDataSource.Builder()
        .setRetryPolicy(DefaultRetryPolicy.class)
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setClusterName(String value);

    public abstract Builder setLocalDataCenter(@Nullable String value);

    public abstract Builder setContactPoints(ImmutableList<InetSocketAddress> value);

    public Builder setContactPoints(List<InetSocketAddress> value) {
      return setContactPoints(ImmutableList.copyOf(value));
    }

    public abstract Builder setDbAuth(@Nullable DbAuth value);

    public abstract Builder setRetryPolicy(Class value);

    public abstract Builder setConsistencyLevel(ConsistencyLevel value);

    public abstract Builder setConnectTimeout(@Nullable Duration value);

    public abstract Builder setRequestTimeout(@Nullable Duration value);

    public abstract CassandraDataSource build();
  }
}
