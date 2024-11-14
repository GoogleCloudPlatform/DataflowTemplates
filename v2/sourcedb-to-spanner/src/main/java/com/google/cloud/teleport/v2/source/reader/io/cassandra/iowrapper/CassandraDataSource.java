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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Encapsulates details of a Cassandra Cluster. Cassandra Cluster can connect to multiple KeySpaces,
 * just like a Mysql instance can have multiple databases.
 */
@AutoValue
public abstract class CassandraDataSource implements Serializable {

  /** Name of the Cassandra Cluster. */
  public abstract String clusterName();

  /** Contact points for connecting to a Cassandra Cluster. */
  public abstract ImmutableList<InetSocketAddress> contactPoints();

  public static Builder builder() {
    return new AutoValue_CassandraDataSource.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setClusterName(String value);

    public abstract Builder setContactPoints(ImmutableList<InetSocketAddress> value);

    public Builder setContactPoints(List<InetSocketAddress> value) {
      return setContactPoints(ImmutableList.copyOf(value));
    }

    public abstract CassandraDataSource build();
  }
}
