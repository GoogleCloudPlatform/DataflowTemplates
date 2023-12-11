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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

class BearerConnectionParams extends ConnectionParams {
  private final String token;

  @JsonCreator
  public BearerConnectionParams(
      @JsonProperty("server_url") String serverUrl,
      @JsonProperty("database") String database,
      @JsonProperty("token") String token) {
    super(serverUrl, database);
    this.token = token;
  }

  @Override
  public AuthToken asAuthToken() {
    return AuthTokens.bearer(token);
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

    BearerConnectionParams that = (BearerConnectionParams) o;
    return Objects.equals(token, that.token);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), token);
  }

  @Override
  public String toString() {
    return "BearerConnectionParams{"
        + "server_url='"
        + getServerUrl()
        + '\''
        + ", database='"
        + getDatabase()
        + '\''
        + ", token='"
        + token
        + '\''
        + "}";
  }
}
