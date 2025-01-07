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
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Encapsulates details of a Cassandra Cluster. Cassandra Cluster can connect to multiple KeySpaces,
 * just like a Mysql instance can have multiple databases.
 */
@AutoValue
public abstract class CassandraDataSource implements Serializable {

  /** Options Map. * */
  abstract OptionsMap optionsMap();

  @Nullable
  public abstract String clusterName();

  public DriverConfigLoader driverConfigLoader() {
    return CassandraDriverConfigLoader.fromOptionsMap(optionsMap());
  }

  /** returns List of ContactPoints. Added for easier compatibility with 3.0 cluster creation. */
  public ImmutableList<InetSocketAddress> contactPoints() {
    return driverConfigLoader()
        .getInitialConfig()
        .getDefaultProfile()
        .getStringList(TypedDriverOption.CONTACT_POINTS.getRawOption())
        .stream()
        .map(
            contactPoint -> {
              String[] ipPort = contactPoint.split(":");
              return new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
            })
        .collect(ImmutableList.toImmutableList());
  }

  /** Returns local datacenter. Added for easier compatibility with 3.0 cluster creation. */
  public String localDataCenter() {
    return driverConfigLoader()
        .getInitialConfig()
        .getDefaultProfile()
        .getString(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getRawOption());
  }

  /** Returns the logged Keyspace. */
  public String loggedKeySpace() {
    return driverConfigLoader()
        .getInitialConfig()
        .getDefaultProfile()
        .getString(TypedDriverOption.SESSION_KEYSPACE.getRawOption());
  }

  public static Builder builder() {
    return new AutoValue_CassandraDataSource.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setOptionsMap(OptionsMap value);

    public abstract Builder setClusterName(@Nullable String value);

    abstract OptionsMap optionsMap();

    public Builder setOptionsMapFromGcsFile(String gcsPath) throws FileNotFoundException {
      return this.setOptionsMap(CassandraDriverConfigLoader.getOptionsMapFromFile(gcsPath));
    }

    public <ValueT> Builder overrideOptionInOptionsMap(
        TypedDriverOption<ValueT> option, ValueT value) {
      DriverConfigLoader.fromMap(optionsMap())
          .getInitialConfig()
          .getProfiles()
          .keySet()
          .forEach(profile -> this.optionsMap().put(profile, option, value));
      return this;
    }

    /**
     * Allowing UT to set the contact points. In UT environment, the port is dynamically determined.
     * We can't use a static GCS file to provide the contact points.
     */
    @VisibleForTesting
    public Builder setContactPoints(List<InetSocketAddress> contactPoints) {
      overrideOptionInOptionsMap(
          TypedDriverOption.CONTACT_POINTS,
          contactPoints.stream()
              .map(p -> p.getAddress().getHostAddress() + ":" + p.getPort())
              .collect(ImmutableList.toImmutableList()));
      return this;
    }

    /** Set the local Datacenter. */
    @VisibleForTesting
    public Builder setLocalDataCenter(String localDataCenter) {
      overrideOptionInOptionsMap(
          TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, localDataCenter);
      return this;
    }

    abstract CassandraDataSource autoBuild();

    public CassandraDataSource build() {
      /* Prefer to use quorum read until we encounter a strong use case to not do so. */
      this.overrideOptionInOptionsMap(
          TypedDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.QUORUM.toString());
      return autoBuild();
    }
  }
}
