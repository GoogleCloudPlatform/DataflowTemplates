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

import java.util.Objects;

public class CassandraShard extends Shard {
  private String keyspace;
  private String consistencyLevel = "LOCAL_QUORUM";
  private boolean sslOptions = false;
  private String protocolVersion = "v5";
  private String dataCenter = "datacenter1";
  private int localPoolSize = 1024;
  private int remotePoolSize = 256;

  public String getKeySpaceName() {
    return keyspace;
  }

  public String getConsistencyLevel() {
    return consistencyLevel;
  }

  public Boolean getSSLOptions() {
    return sslOptions;
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public Integer getLocalPoolSize() {
    return localPoolSize;
  }

  public Integer getRemotePoolSize() {
    return remotePoolSize;
  }

  public void setKeySpaceName(String keySpaceName) {
    this.keyspace = keySpaceName;
  }

  public void setConsistencyLevel(String consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  public void setSslOptions(boolean sslOptions) {
    this.sslOptions = sslOptions;
  }

  public void setProtocolVersion(String protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public void setLocalPoolSize(int localPoolSize) {
    this.localPoolSize = localPoolSize;
  }

  public void setRemotePoolSize(int remotePoolSize) {
    this.remotePoolSize = remotePoolSize;
  }

  public void validate() throws IllegalArgumentException {
    if (getHost() == null || getHost().isEmpty()) {
      throw new IllegalArgumentException("Host is required");
    }
    if (getPort() == null || getPort().isEmpty()) {
      throw new IllegalArgumentException("Port is required");
    }
    if (getUserName() == null || getUserName().isEmpty()) {
      throw new IllegalArgumentException("Username is required");
    }
    if (getPassword() == null || getUserName().isEmpty()) {
      throw new IllegalArgumentException("Password is required");
    }
    if (keyspace == null || keyspace.isEmpty()) {
      throw new IllegalArgumentException("Keyspace is required");
    }
  }

  @Override
  public String toString() {
    return "CassandraShard{"
        + "logicalShardId='"
        + getLogicalShardId()
        + '\''
        + ", host='"
        + getHost()
        + '\''
        + ", port='"
        + getPort()
        + '\''
        + ", user='"
        + getUserName()
        + '\''
        + ", keySpaceName='"
        + keyspace
        + '\''
        + ", datacenter='"
        + dataCenter
        + '\''
        + ", consistencyLevel='"
        + consistencyLevel
        + '\''
        + ", protocolVersion="
        + protocolVersion
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CassandraShard)) {
      return false;
    }
    CassandraShard cassandraShard = (CassandraShard) o;
    return Objects.equals(getLogicalShardId(), cassandraShard.getLogicalShardId())
        && Objects.equals(getHost(), cassandraShard.getHost())
        && Objects.equals(getPort(), cassandraShard.getPort())
        && Objects.equals(getUserName(), cassandraShard.getUserName())
        && Objects.equals(getPassword(), cassandraShard.getPassword())
        && Objects.equals(keyspace, cassandraShard.keyspace)
        && Objects.equals(dataCenter, cassandraShard.dataCenter)
        && Objects.equals(consistencyLevel, cassandraShard.consistencyLevel)
        && Objects.equals(protocolVersion, cassandraShard.protocolVersion)
        && Objects.equals(sslOptions, cassandraShard.sslOptions)
        && Objects.equals(localPoolSize, cassandraShard.localPoolSize)
        && Objects.equals(remotePoolSize, cassandraShard.remotePoolSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getLogicalShardId(),
        getHost(),
        getPort(),
        getUserName(),
        getPassword(),
        keyspace,
        dataCenter,
        consistencyLevel,
        protocolVersion,
        sslOptions,
        localPoolSize,
        remotePoolSize);
  }
}
