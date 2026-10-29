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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

/** {@link SharedDatabaseServer.Engine} for MySQL. */
public final class MySQLEngine implements SharedDatabaseServer.Engine {

  public static final MySQLEngine INSTANCE = new MySQLEngine();

  // Same image as org.apache.beam.it.jdbc.MySQLResourceManager uses for per-test containers.
  private static final String IMAGE = "mysql";
  private static final String TAG = "8.0.30";

  // MySQL's default (151) is too low when many tests use the same server in parallel, and each
  // DirectRunner pipeline opens several connections.
  private static final int MAX_CONNECTIONS = 1000;

  // MySQL limit on database (schema) names.
  private static final int MAX_DATABASE_NAME_LENGTH = 64;

  private MySQLEngine() {}

  @Override
  public String name() {
    return "mysql";
  }

  @Override
  public JdbcDatabaseContainer<?> newContainer() {
    return new MySQLContainer<>(DockerImageName.parse(IMAGE).withTag(TAG))
        .withUsername("root")
        .withPassword(RandomStringUtils.randomAlphanumeric(16))
        .withCommand("--max_connections=" + MAX_CONNECTIONS);
  }

  @Override
  public int containerPort() {
    return MySQLContainer.MYSQL_PORT;
  }

  @Override
  public int maxDatabaseNameLength() {
    return MAX_DATABASE_NAME_LENGTH;
  }

  @Override
  public String createDatabaseSql(String databaseName) {
    return "CREATE DATABASE `" + databaseName + "`";
  }

  @Override
  public String dropDatabaseSql(String databaseName) {
    return "DROP DATABASE IF EXISTS `" + databaseName + "`";
  }

  @Override
  public Map<String, String> usageQueries() {
    Map<String, String> queries = new LinkedHashMap<>();
    queries.put("maxConnections", "SELECT @@max_connections");
    queries.put("maxUsedConnections", "SHOW GLOBAL STATUS LIKE 'Max_used_connections'");
    queries.put(
        "refusedConnections", "SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'");
    queries.put("abortedConnects", "SHOW GLOBAL STATUS LIKE 'Aborted_connects'");
    queries.put("totalConnections", "SHOW GLOBAL STATUS LIKE 'Connections'");
    queries.put("currentConnections", currentConnectionsQuery());
    return queries;
  }

  @Override
  public String currentConnectionsQuery() {
    return "SHOW GLOBAL STATUS LIKE 'Threads_connected'";
  }
}
