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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import java.util.List;
import java.util.Objects;

public class CassandraShard extends Shard {
  private final DriverConfigLoader configLoader;

  public CassandraShard(DriverConfigLoader configLoader) {
    super();
    this.configLoader = configLoader;
    validateFields();
    extractAndSetHostAndPort();
  }

  private void validateFields() {
    if (getContactPoints() == null || getContactPoints().isEmpty()) {
      throw new IllegalArgumentException("CONTACT_POINTS cannot be null or empty.");
    }
    if (getKeySpaceName() == null || getKeySpaceName().isEmpty()) {
      throw new IllegalArgumentException("SESSION_KEYSPACE cannot be null or empty.");
    }
  }

  private void extractAndSetHostAndPort() {
    String firstContactPoint = getContactPoints().get(0);
    String[] parts = firstContactPoint.split(":");

    if (parts.length < 2) {
      throw new IllegalArgumentException("Invalid contact point format: " + firstContactPoint);
    }

    String host = parts[0];
    String port = parts[1];

    setHost(host);
    setPort(port);
  }

  private DriverExecutionProfile getProfile() {
    return configLoader.getInitialConfig().getDefaultProfile();
  }

  // Getters that fetch data from DriverConfigLoader
  public List<String> getContactPoints() {
    return getProfile().getStringList(DefaultDriverOption.CONTACT_POINTS);
  }

  public String getKeySpaceName() {
    return getProfile().getString(DefaultDriverOption.SESSION_KEYSPACE);
  }

  public String getConsistencyLevel() {
    return getProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM");
  }

  public boolean isSslEnabled() {
    return getProfile().getBoolean(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, false);
  }

  public String getProtocolVersion() {
    return getProfile().getString(DefaultDriverOption.PROTOCOL_VERSION, "V5");
  }

  public String getDataCenter() {
    return getProfile()
        .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1");
  }

  public int getLocalPoolSize() {
    return getProfile().getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1024);
  }

  public int getRemotePoolSize() {
    return getProfile().getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 256);
  }

  public DriverConfigLoader getConfigLoader() {
    return configLoader;
  }

  @Override
  public String toString() {
    return String.format(
        "CassandraShard{logicalShardId='%s', contactPoints=%s, keyspace='%s', consistencyLevel='%s', sslOptions=%b, protocolVersion='%s', dataCenter='%s', localPoolSize=%d, remotePoolSize=%d, host='%s', port='%s'}",
        getLogicalShardId(),
        getContactPoints(),
        getKeySpaceName(),
        getConsistencyLevel(),
        isSslEnabled(),
        getProtocolVersion(),
        getDataCenter(),
        getLocalPoolSize(),
        getRemotePoolSize(),
        getHost(),
        getPort());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CassandraShard)) return false;
    CassandraShard that = (CassandraShard) o;
    return Objects.equals(getContactPoints(), that.getContactPoints())
        && Objects.equals(getKeySpaceName(), that.getKeySpaceName())
        && Objects.equals(getConsistencyLevel(), that.getConsistencyLevel())
        && Objects.equals(isSslEnabled(), that.isSslEnabled())
        && Objects.equals(getProtocolVersion(), that.getProtocolVersion())
        && Objects.equals(getDataCenter(), that.getDataCenter())
        && Objects.equals(getLocalPoolSize(), that.getLocalPoolSize())
        && Objects.equals(getRemotePoolSize(), that.getRemotePoolSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getContactPoints(),
        getKeySpaceName(),
        getConsistencyLevel(),
        isSslEnabled(),
        getProtocolVersion(),
        getDataCenter(),
        getLocalPoolSize(),
        getRemotePoolSize());
  }
}
