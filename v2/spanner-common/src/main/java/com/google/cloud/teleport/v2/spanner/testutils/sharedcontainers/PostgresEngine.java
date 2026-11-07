/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.testutils.sharedcontainers;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

/** {@link SharedDatabaseServer.Engine} for PostgreSQL. */
public final class PostgresEngine implements SharedDatabaseServer.Engine {

  public static final PostgresEngine INSTANCE = new PostgresEngine();

  // Same image as org.apache.beam.it.jdbc.PostgresResourceManager uses for per-test containers.
  private static final String IMAGE = "postgres";
  private static final String TAG = "15.1";

  // PostgreSQL's default (100) is too low when many tests use the same server in parallel.
  private static final int MAX_CONNECTIONS = 1000;

  // PostgreSQL limit on identifiers, including database names.
  private static final int MAX_DATABASE_NAME_LENGTH = 63;

  private PostgresEngine() {}

  @Override
  public String name() {
    return "postgres";
  }

  @Override
  public JdbcDatabaseContainer<?> newContainer() {
    // Same user as org.apache.beam.it.jdbc.AbstractJDBCResourceManager uses by default. fsync=off
    // is
    // the Testcontainers default command, kept because withCommand replaces it.
    return new PostgreSQLContainer<>(DockerImageName.parse(IMAGE).withTag(TAG))
        .withUsername("root")
        .withPassword(RandomStringUtils.randomAlphanumeric(16))
        .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=" + MAX_CONNECTIONS);
  }

  @Override
  public int containerPort() {
    return PostgreSQLContainer.POSTGRESQL_PORT;
  }

  @Override
  public int maxDatabaseNameLength() {
    return MAX_DATABASE_NAME_LENGTH;
  }

  @Override
  public String createDatabaseSql(String databaseName) {
    return "CREATE DATABASE \"" + databaseName + "\"";
  }

  @Override
  public String dropDatabaseSql(String databaseName) {
    // FORCE closes connections a finished test may have left open.
    return "DROP DATABASE IF EXISTS \"" + databaseName + "\" WITH (FORCE)";
  }

  @Override
  public Map<String, String> usageQueries() {
    // PostgreSQL has no peak-connections counter; SharedDatabaseServer samples the current count.
    Map<String, String> queries = new LinkedHashMap<>();
    queries.put("maxConnections", "SHOW max_connections");
    queries.put("currentConnections", currentConnectionsQuery());
    queries.put("totalConnections", "SELECT sum(numbackends) FROM pg_stat_database");
    return queries;
  }

  @Override
  public String currentConnectionsQuery() {
    return "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'";
  }
}
