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
package com.google.cloud.teleport.v2.templates.shared;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/**
 * A database server container that is started once and shared by all integration tests running in
 * the same JVM. Each test gets its own database on the shared server instead of its own container.
 *
 * <p>Engine specific details (image, startup flags, SQL dialect) are supplied by an {@link Engine},
 * so other engines can reuse this class by adding a new {@link Engine} implementation.
 *
 * <p>The container is started lazily on first use and removed by Testcontainers when the JVM exits.
 * Sharing can be turned off with {@code -DsharedTestContainers=false} or the environment variable
 * {@code SHARED_TEST_CONTAINERS=false}.
 */
public final class SharedDatabaseServer {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDatabaseServer.class);

  /** System property that turns sharing on (default) or off. */
  public static final String ENABLED_PROPERTY = "sharedTestContainers";

  /** Environment variable that turns sharing on (default) or off, if the property is not set. */
  public static final String ENABLED_ENV = "SHARED_TEST_CONTAINERS";

  // Containers occasionally fail to come up on busy CI runners; starting a new one usually works.
  private static final int STARTUP_ATTEMPTS = 3;

  private static final Map<String, SharedDatabaseServer> SERVERS = new HashMap<>();

  /** Engine specific parts of a shared database server. */
  public interface Engine {

    /** Short engine name, used in logs and as the key of the shared server, e.g. {@code mysql}. */
    String name();

    /** Creates, but does not start, a new container for this engine. */
    JdbcDatabaseContainer<?> newContainer();

    /** Port the engine listens on inside the container. */
    int containerPort();

    /** Maximum length of a database name accepted by the engine. */
    int maxDatabaseNameLength();

    /** SQL that creates the given database. */
    String createDatabaseSql(String databaseName);

    /** SQL that drops the given database. */
    String dropDatabaseSql(String databaseName);

    /**
     * Queries reporting how much of the server was used, keyed by a label. Each query must return
     * one row; the value of its last column is logged.
     */
    Map<String, String> usageQueries();
  }

  private final Engine engine;
  private final JdbcDatabaseContainer<?> container;
  private final Set<String> liveDatabases = ConcurrentHashMap.newKeySet();
  private final AtomicInteger databasesCreated = new AtomicInteger();
  private final AtomicInteger databasesDropped = new AtomicInteger();

  private SharedDatabaseServer(Engine engine, JdbcDatabaseContainer<?> container) {
    this.engine = engine;
    this.container = container;
  }

  /** Returns whether tests should use shared database servers. */
  public static boolean isEnabled() {
    String value = System.getProperty(ENABLED_PROPERTY);
    if (value == null) {
      value = System.getenv(ENABLED_ENV);
    }
    return value == null || Boolean.parseBoolean(value);
  }

  /** Returns the shared server for the engine, starting it on first use. */
  public static synchronized SharedDatabaseServer get(Engine engine) {
    SharedDatabaseServer server = SERVERS.get(engine.name());
    if (server == null) {
      server = start(engine);
      SERVERS.put(engine.name(), server);
    }
    return server;
  }

  private static SharedDatabaseServer start(Engine engine) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= STARTUP_ATTEMPTS; attempt++) {
      JdbcDatabaseContainer<?> container = engine.newContainer();
      try {
        container.start();
        SharedDatabaseServer server = new SharedDatabaseServer(engine, container);
        Runtime.getRuntime()
            .addShutdownHook(new Thread(server::logUsage, "shared-" + engine.name() + "-usage"));
        LOG.info(
            "Started shared {} container {} at {}:{}",
            engine.name(),
            container.getDockerImageName(),
            server.getHost(),
            server.getPort());
        return server;
      } catch (RuntimeException e) {
        lastFailure = e;
        LOG.warn(
            "Shared {} container failed to start (attempt {}/{})",
            engine.name(),
            attempt,
            STARTUP_ATTEMPTS,
            e);
        try {
          container.stop();
        } catch (RuntimeException stopFailure) {
          LOG.warn("Failed to stop {} container after failed start", engine.name(), stopFailure);
        }
      }
    }
    throw new IllegalStateException(
        "Shared "
            + engine.name()
            + " container failed to start after "
            + STARTUP_ATTEMPTS
            + " attempts",
        lastFailure);
  }

  public String getHost() {
    return container.getHost();
  }

  public int getPort() {
    return container.getMappedPort(engine.containerPort());
  }

  public String getUsername() {
    return container.getUsername();
  }

  public String getPassword() {
    return container.getPassword();
  }

  /**
   * Creates a new, empty database for a test and returns its name. The name starts with the test
   * name and ends with a random suffix, so parallel tests never collide.
   */
  public String createDatabase(String testName) {
    String databaseName = databaseNameFor(testName);
    execute(engine.createDatabaseSql(databaseName));
    liveDatabases.add(databaseName);
    databasesCreated.incrementAndGet();
    LOG.info("Created database {} on shared {} container", databaseName, engine.name());
    return databaseName;
  }

  /** Drops a database created by {@link #createDatabase}. Failures are logged, not thrown. */
  public void dropDatabase(String databaseName) {
    try {
      execute(engine.dropDatabaseSql(databaseName));
      liveDatabases.remove(databaseName);
      databasesDropped.incrementAndGet();
      LOG.info("Dropped database {} on shared {} container", databaseName, engine.name());
    } catch (RuntimeException e) {
      LOG.warn("Failed to drop database {} on shared {} container", databaseName, engine.name(), e);
    }
    // Logged after every drop because logging may already be shut down when the JVM exits.
    logUsage();
  }

  /**
   * Logs how much the shared server was used so far. Called after every dropped database and when
   * the JVM exits.
   */
  public void logUsage() {
    Map<String, String> usage = new LinkedHashMap<>();
    usage.put("databasesCreated", String.valueOf(databasesCreated.get()));
    usage.put("databasesDropped", String.valueOf(databasesDropped.get()));
    usage.put("databasesLeft", String.valueOf(liveDatabases.size()));
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      for (Map.Entry<String, String> query : engine.usageQueries().entrySet()) {
        try (ResultSet resultSet = statement.executeQuery(query.getValue())) {
          usage.put(
              query.getKey(),
              resultSet.next()
                  ? resultSet.getString(resultSet.getMetaData().getColumnCount())
                  : "n/a");
        }
      }
    } catch (SQLException | RuntimeException e) {
      LOG.warn("Failed to read usage of shared {} container", engine.name(), e);
    }
    LOG.info("Shared {} container usage: {}", engine.name(), usage);
  }

  private String databaseNameFor(String testName) {
    String suffix = "_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase(Locale.ROOT);
    String base = testName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    int maxBaseLength = engine.maxDatabaseNameLength() - suffix.length();
    if (base.length() > maxBaseLength) {
      base = base.substring(0, maxBaseLength);
    }
    return base + suffix;
  }

  private void execute(String sql) {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to run '" + sql + "' on shared " + engine.name() + " container", e);
    }
  }

  private Connection connect() throws SQLException {
    return DriverManager.getConnection(container.getJdbcUrl(), getUsername(), getPassword());
  }
}
