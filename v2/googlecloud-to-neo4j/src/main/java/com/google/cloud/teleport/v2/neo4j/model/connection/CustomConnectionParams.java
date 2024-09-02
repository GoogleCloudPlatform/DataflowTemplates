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
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

class CustomConnectionParams extends ConnectionParams {
  private final String principal;
  private final String credentials;
  private final String realm;
  private final String scheme;
  private final Map<String, Object> parameters;

  @JsonCreator
  public CustomConnectionParams(
      @JsonProperty("server_url") String serverUrl,
      @JsonProperty("database") String database,
      @JsonProperty("custom_ca_certificate_path") String customCertificatePath,
      @JsonProperty("principal") String principal,
      @JsonProperty("credentials") String credentials,
      @JsonProperty("realm") String realm,
      @JsonProperty("scheme") String scheme,
      @JsonProperty("parameters") Map<String, Object> parameters) {
    super(serverUrl, database, customCertificatePath);
    this.principal = principal;
    this.credentials = credentials;
    this.realm = realm;
    this.scheme = scheme;
    this.parameters = parameters;
  }

  @Override
  public AuthToken asAuthToken() {
    return AuthTokens.custom(principal, credentials, realm, scheme, parameters);
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

    CustomConnectionParams that = (CustomConnectionParams) o;
    return Objects.equals(principal, that.principal)
        && Objects.equals(credentials, that.credentials)
        && Objects.equals(realm, that.realm)
        && Objects.equals(scheme, that.scheme)
        && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), principal, credentials, realm, scheme, parameters);
  }

  @Override
  public String toString() {
    return "CustomConnectionParams{"
        + "server_url='"
        + getServerUrl()
        + '\''
        + ", database='"
        + getDatabase()
        + '\''
        + ", principal='"
        + principal
        + '\''
        + ", credentials=<redacted>"
        + ", realm='"
        + realm
        + '\''
        + ", scheme='"
        + scheme
        + '\''
        + ", parameters='"
        + parameters
        + '\''
        + "}";
  }
}
