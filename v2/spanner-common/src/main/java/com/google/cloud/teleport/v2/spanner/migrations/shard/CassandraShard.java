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

  // Getters
  public String getKeySpaceName() {
    return keyspace;
  }

  public String getConsistencyLevel() {
    return consistencyLevel;
  }

  public boolean getSSLOptions() {
    return sslOptions;
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public int getLocalPoolSize() {
    return localPoolSize;
  }

  public int getRemotePoolSize() {
    return remotePoolSize;
  }

  // Setters
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

  public void validate() {
    validateField(getHost(), "Host");
    validateField(getPort(), "Port");
    validateField(getUserName(), "Username");
    validateField(getPassword(), "Password");
    validateField(getKeySpaceName(), "Keyspace");
  }

  private void validateField(String value, String fieldName) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
  }

  @Override
  public String toString() {
    return String.format(
        "CassandraShard{logicalShardId='%s', host='%s', port='%s', user='%s', keySpaceName='%s', datacenter='%s', consistencyLevel='%s', protocolVersion='%s'}",
        getLogicalShardId(),
        getHost(),
        getPort(),
        getUserName(),
        getKeySpaceName(),
        getDataCenter(),
        getConsistencyLevel(),
        getProtocolVersion());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CassandraShard)) return false;
    CassandraShard that = (CassandraShard) o;
    return sslOptions == that.sslOptions
        && localPoolSize == that.localPoolSize
        && remotePoolSize == that.remotePoolSize
        && Objects.equals(getLogicalShardId(), that.getLogicalShardId())
        && Objects.equals(getHost(), that.getHost())
        && Objects.equals(getPort(), that.getPort())
        && Objects.equals(getUserName(), that.getUserName())
        && Objects.equals(getPassword(), that.getPassword())
        && Objects.equals(keyspace, that.keyspace)
        && Objects.equals(dataCenter, that.dataCenter)
        && Objects.equals(consistencyLevel, that.consistencyLevel)
        && Objects.equals(protocolVersion, that.protocolVersion);
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
