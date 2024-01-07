/*
 * Copyright (C) 2023 Google LLC
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

import java.io.Serializable;
import java.util.Objects;

public class Shard implements Serializable {

  private String logicalShardId;
  private String host;
  private String port;
  private String user;
  private String password;
  private String dbName;
  private String secretManagerUri;

  public Shard(
      String logicalShardId,
      String host,
      String port,
      String user,
      String password,
      String dbName,
      String secretManagerUri) {
    this.logicalShardId = logicalShardId;
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.dbName = dbName;
    this.secretManagerUri = secretManagerUri;
  }

  public Shard() {}

  public String getLogicalShardId() {
    return logicalShardId;
  }

  public void setLogicalShardId(String input) {
    this.logicalShardId = input;
  }

  public String getUserName() {
    return user;
  }

  public void setUser(String input) {
    this.user = input;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String input) {
    this.password = input;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String input) {
    this.host = input;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String input) {
    this.port = input;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String input) {
    this.dbName = input;
  }

  public String getSecretManagerUri() {
    return secretManagerUri;
  }

  public void setSecretManagerUri(String input) {
    this.secretManagerUri = input;
  }

  @Override
  public String toString() {
    return "{ logicalShardId: "
        + logicalShardId
        + ", host: "
        + host
        + ", port: "
        + port
        + " , dbName: "
        + dbName
        + " , user: "
        + user
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Shard)) {
      return false;
    }
    final Shard other = (Shard) o;
    return this.logicalShardId.equals(other.logicalShardId)
        && this.host.equals(other.host)
        && this.port.equals(other.port)
        && this.user.equals(other.user)
        && this.dbName.equals(other.dbName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalShardId, host, port, user, dbName);
  }
}
