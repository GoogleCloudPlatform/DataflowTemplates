/*
 * Copyright (C) 2022 Google LLC
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

class BasicConnectionParams extends ConnectionParams {

  private final String username;
  private final String password;

  @JsonCreator
  public BasicConnectionParams(
      @JsonProperty("server_url") String serverUrl,
      @JsonProperty("database") String database,
      @JsonProperty("custom_ca_certificate_path") String customCertificatePath,
      @JsonProperty("username") String username,
      @JsonProperty("pwd") String password) {
    super(serverUrl, database, customCertificatePath);
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public AuthToken asAuthToken() {
    return AuthTokens.basic(username, password);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    BasicConnectionParams that = (BasicConnectionParams) o;
    return Objects.equals(username, that.username) && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), username, password);
  }

  @Override
  public String toString() {
    return "BasicConnectionParams{"
        + "server_url='"
        + getServerUrl()
        + '\''
        + ", database='"
        + getDatabase()
        + '\''
        + ", username='"
        + username
        + '\''
        + ", password=<redacted>"
        + "}";
  }
}
