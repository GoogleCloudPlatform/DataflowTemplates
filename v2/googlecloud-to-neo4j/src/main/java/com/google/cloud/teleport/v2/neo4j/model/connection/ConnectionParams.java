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
package com.google.cloud.teleport.v2.neo4j.model.connection;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.io.Serializable;
import java.util.Objects;
import org.neo4j.driver.AuthToken;

@JsonTypeInfo(use = Id.NAME, property = "auth_type", defaultImpl = BasicConnectionParams.class)
@JsonSubTypes({
  @JsonSubTypes.Type(value = BasicConnectionParams.class, name = "basic"),
  @JsonSubTypes.Type(value = NoAuthConnectionParams.class, name = "none"),
  @JsonSubTypes.Type(value = KerberosConnectionParams.class, name = "kerberos"),
  @JsonSubTypes.Type(value = BearerConnectionParams.class, name = "bearer"),
  @JsonSubTypes.Type(value = CustomConnectionParams.class, name = "custom")
})
public abstract class ConnectionParams implements Serializable {

  private final String serverUrl;
  private final String database;
  private final Long maxConnectionLifetimeMillis;
  private final Long connectionLivenessCheckTimeoutMillis;

  public ConnectionParams(
      String serverUrl,
      String database,
      Long maxConnectionLifetimeMillis,
      Long connectionLivenessCheckTimeoutMillis) {
    this.serverUrl = serverUrl;
    this.database = database == null ? "neo4j" : database;
    this.maxConnectionLifetimeMillis = maxConnectionLifetimeMillis;
    this.connectionLivenessCheckTimeoutMillis = connectionLivenessCheckTimeoutMillis;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  public String getDatabase() {
    return database;
  }

  public Long getMaxConnectionLifetimeMillis() {
    return maxConnectionLifetimeMillis;
  }

  public Long getConnectionLivenessCheckTimeoutMillis() {
    return connectionLivenessCheckTimeoutMillis;
  }

  public abstract AuthToken asAuthToken();

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ConnectionParams that)) {
      return false;
    }
    return Objects.equals(serverUrl, that.serverUrl)
        && Objects.equals(database, that.database)
        && Objects.equals(maxConnectionLifetimeMillis, that.maxConnectionLifetimeMillis)
        && Objects.equals(
            connectionLivenessCheckTimeoutMillis, that.connectionLivenessCheckTimeoutMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        serverUrl, database, maxConnectionLifetimeMillis, connectionLivenessCheckTimeoutMillis);
  }
}
