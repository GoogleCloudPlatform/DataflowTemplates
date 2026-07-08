/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.jdbc;

/**
 * Client for static MySQL resource.
 *
 * <p>Subclass of {@link StaticJDBCResource}.
 */
public class StaticMySQLResource extends StaticJDBCResource {

  StaticMySQLResource(Builder builder) {
    super(builder);
  }

  @Override
  public SourceType type() {
    return SourceType.MYSQL;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  public static Builder builder(
      String hostname, String username, String password, int port, String database) {
    return new Builder(hostname, username, password, port, database);
  }

  /** Builder for {@link StaticMySQLResource}. */
  public static class Builder extends StaticJDBCResource.Builder<StaticMySQLResource> {
    public Builder(String hostname, String username, String password, int port, String database) {
      super(hostname, username, password, port, database);
    }

    @Override
    public StaticMySQLResource build() {
      return new StaticMySQLResource(this);
    }
  }
}
