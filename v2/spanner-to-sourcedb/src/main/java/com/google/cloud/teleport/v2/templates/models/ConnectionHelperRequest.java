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
package com.google.cloud.teleport.v2.templates.models;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.List;

/**
 * Represents a request to initialize a connection helper with the necessary parameters.
 *
 * <p>This class encapsulates the essential information required for establishing connections to a
 * database or a data source. It includes:
 *
 * <ul>
 *   <li>A list of {@link Shard} objects representing the database shards.
 *   <li>Optional connection properties as a {@link String}.
 *   <li>The maximum number of connections allowed.
 *   <li>The name of the driver to connect to source.
 *   <li>Optional connection initialisation queries as a {@link String}.
 * </ul>
 */
public class ConnectionHelperRequest {
  private List<Shard> shards;
  private String properties;
  private int maxConnections;
  private String driver;
  private String connectionInitQuery;

  public List<Shard> getShards() {
    return shards;
  }

  public String getProperties() {
    return properties;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public String getDriver() {
    return driver;
  }

  public String getConnectionInitQuery() {
    return connectionInitQuery;
  }

  public ConnectionHelperRequest(
      List<Shard> shards,
      String properties,
      int maxConnections,
      String driver,
      String connectionInitQuery) {
    this.shards = shards;
    this.properties = properties;
    this.maxConnections = maxConnections;
    this.driver = driver;
    this.connectionInitQuery = connectionInitQuery;
  }
}
