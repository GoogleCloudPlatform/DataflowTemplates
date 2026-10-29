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
package com.google.cloud.teleport.v2.spanner.sqlserverutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A SQL Server Testcontainer meant to be started once per test JVM and shared by all the SQL Server
 * ITs of a template module.
 *
 * <p>When {@code enableCdc} is {@code true}, the container is started with {@code
 * MSSQL_AGENT_ENABLED=true} and waits until SQL Server Agent is running so that Change Data Capture
 * (CDC) capture jobs populate change tables for Datastream.
 *
 * <p>The container is published on {@link TestProperties#hostIp()} (the IP of the machine running
 * the tests, passed as {@code -DhostIp}) and its mapped port, so that it is reachable by Dataflow
 * workers and Datastream, and not only from the test JVM. It is removed by Testcontainers when the
 * JVM exits.
 */
public final class SQLServerITContainer {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerITContainer.class);

  /** System property to override the container image, e.g. to use a mirror. */
  private static final String IMAGE_PROPERTY = "sqlServerContainerImage";

  private static final String DEFAULT_IMAGE = "mcr.microsoft.com/mssql/server:2025-latest";

  // mcr.microsoft.com/mssql/server:2025-latest intermittently dies a few seconds after boot with a
  // SQLPAL assertion ("ASSERT: Expression=(!currentWorker->IsPreemptiveOnEntry())").
  private static final int STARTUP_ATTEMPTS = 3;
  private static final Duration AGENT_READY_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration AGENT_READY_POLL_INTERVAL = Duration.ofSeconds(2);

  private final MSSQLServerContainer<?> container;

  private SQLServerITContainer(MSSQLServerContainer<?> container) {
    this.container = container;
  }

  /**
   * Starts a new SQL Server container and configures it.
   *
   * @param enableCdc whether to enable SQL Server Agent inside the container for change data
   *     capture with Datastream.
   */
  public static SQLServerITContainer start(boolean enableCdc) {
    String image = System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= STARTUP_ATTEMPTS; attempt++) {
      MSSQLServerContainer<?> container = null;
      try {
        LOG.info(
            "Starting SQL Server container {} (CDC enabled: {}, attempt {}/{}).",
            image,
            enableCdc,
            attempt,
            STARTUP_ATTEMPTS);
        container =
            new MSSQLServerContainer<>(
                    DockerImageName.parse(image)
                        .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server"))
                .acceptLicense()
                .withUrlParam("trustServerCertificate", "true");
        if (enableCdc) {
          container.withEnv("MSSQL_AGENT_ENABLED", "true");
        }
        container.start();

        SQLServerITContainer sqlServer = new SQLServerITContainer(container);
        if (enableCdc) {
          sqlServer.waitUntilAgentReady();
        }
        LOG.info("SQL Server container ready at {}:{}.", sqlServer.getHost(), sqlServer.getPort());
        return sqlServer;
      } catch (RuntimeException e) {
        if (container != null) {
          try {
            container.stop();
          } catch (Exception stopEx) {
            LOG.warn("Failed to stop failed SQL Server container", stopEx);
          }
        }
        lastFailure = e;
        LOG.warn(
            "Shared SQL Server container failed to start (attempt {}/{})",
            attempt,
            STARTUP_ATTEMPTS,
            e);
      }
    }
    throw new IllegalStateException(
        "Shared SQL Server container failed to start after " + STARTUP_ATTEMPTS + " attempts",
        lastFailure);
  }

  /** Host on which the container is reachable by the tests, Dataflow workers and Datastream. */
  public String getHost() {
    return TestProperties.hostIp();
  }

  /** Port on which the container is reachable, on {@link #getHost()}. */
  public int getPort() {
    return container.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
  }

  /** Admin username of the SQL Server container. */
  public String getUsername() {
    return container.getUsername();
  }

  /** Admin password of the SQL Server container. */
  public String getPassword() {
    return container.getPassword();
  }

  /** JDBC URL to the SQL Server container on {@link #getHost()}:{@link #getPort()}. */
  public String getJdbcUrl() {
    return String.format(
        "jdbc:sqlserver://%s:%d;encrypt=false;trustServerCertificate=true;", getHost(), getPort());
  }

  /**
   * Creates a {@link CloudSqlServerResourceManager} connected to this container, creating a
   * dedicated database for {@code testId} with CDC enabled.
   *
   * @param testId id of the resource manager.
   */
  public CloudSqlServerResourceManager createResourceManager(String testId) {
    return CloudSqlServerResourceManager.builder(testId)
        .maybeUseStaticInstance(getHost(), getPort(), getUsername(), getPassword())
        .build();
  }

  /** Waits until SQL Server Agent is in the {@code Running} state inside the container. */
  private void waitUntilAgentReady() {
    Instant deadline = Instant.now().plus(AGENT_READY_TIMEOUT);
    Exception lastFailure = null;
    while (Instant.now().isBefore(deadline)) {
      try (Connection conn =
              DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
          Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT COUNT(*) FROM sys.dm_server_services "
                      + "WHERE servicename LIKE 'SQL Server Agent%' AND status_desc = 'Running'")) {
        if (rs.next() && rs.getInt(1) > 0) {
          return;
        }
      } catch (Exception e) {
        lastFailure = e;
        LOG.info("SQL Server Agent not ready yet: {}", e.getMessage());
      }
      try {
        Thread.sleep(AGENT_READY_POLL_INTERVAL.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for SQL Server Agent in container", e);
      }
    }
    throw new IllegalStateException(
        "SQL Server Agent did not reach Running state within " + AGENT_READY_TIMEOUT, lastFailure);
  }
}
