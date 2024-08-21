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
import java.net.URI;
import java.nio.file.Paths;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Config.TrustStrategy;

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
  private final String customCertificatePath;

  public ConnectionParams(String serverUrl, String database, String customCertificatePath) {
    this.serverUrl = serverUrl;
    this.database = database == null ? "neo4j" : database;
    this.customCertificatePath = customCertificatePath;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  public String getDatabase() {
    return database;
  }

  public String getCustomCertificatePath() {
    return customCertificatePath;
  }

  public abstract AuthToken asAuthToken();

  public TrustStrategy asTrustStrategy() {
    if (customCertificatePath == null) {
      return Config.defaultConfig().trustStrategy();
    }
    return TrustStrategy.trustCustomCertificateSignedBy(
        Paths.get(URI.create(customCertificatePath)).toFile());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConnectionParams)) {
      return false;
    }
    ConnectionParams that = (ConnectionParams) o;
    return Objects.equals(serverUrl, that.serverUrl)
        && Objects.equals(database, that.database)
        && Objects.equals(customCertificatePath, that.customCertificatePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverUrl, database, customCertificatePath);
  }
}
