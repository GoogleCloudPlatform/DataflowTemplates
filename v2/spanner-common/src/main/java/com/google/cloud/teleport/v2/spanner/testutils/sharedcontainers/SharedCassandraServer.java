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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.it.common.TestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A Cassandra container that is started once and shared by all integration tests running in the
 * same JVM. Each test uses its own keyspace: {@code
 * org.apache.beam.it.cassandra.CassandraResourceManager} creates a keyspace for the test and drops
 * it in {@code cleanupAll()}, also when pointed at a shared container.
 *
 * <p>Cassandra is not a JDBC database, so this is a separate, smaller variant of {@link
 * SharedDatabaseServer}. Sharing is turned on and off with the same switch ({@link
 * SharedDatabaseServer#isEnabled()}).
 */
public final class SharedCassandraServer {

  private static final Logger LOG = LoggerFactory.getLogger(SharedCassandraServer.class);

  // Same image as org.apache.beam.it.cassandra.CassandraResourceManager uses for per-test
  // containers.
  private static final String IMAGE = "cassandra";
  private static final String TAG = "4.1.0";
  private static final int CQL_PORT = 9042;
  private static final int STARTUP_ATTEMPTS = 3;

  // How often keyspaces and connected clients are sampled and logged.
  private static final long SAMPLE_INTERVAL_SECONDS = 60;

  private static SharedCassandraServer instance;

  private final GenericContainer<?> container;
  private final Set<String> keyspacesSeen = ConcurrentHashMap.newKeySet();
  private final AtomicInteger peakKeyspaces = new AtomicInteger();
  private final AtomicInteger peakClients = new AtomicInteger();

  private SharedCassandraServer(GenericContainer<?> container) {
    this.container = container;
  }

  /** Returns the shared Cassandra server, starting it on first use. */
  public static synchronized SharedCassandraServer get() {
    if (instance == null) {
      instance = start();
    }
    return instance;
  }

  private static SharedCassandraServer start() {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= STARTUP_ATTEMPTS; attempt++) {
      GenericContainer<?> container = newContainer();
      try {
        container.start();
        SharedCassandraServer server = new SharedCassandraServer(container);
        server.startSampling();
        LOG.info(
            "Started shared cassandra container {} at {}:{}",
            container.getDockerImageName(),
            server.getHost(),
            server.getPort());
        return server;
      } catch (RuntimeException e) {
        lastFailure = e;
        LOG.warn(
            "Shared cassandra container failed to start (attempt {}/{})",
            attempt,
            STARTUP_ATTEMPTS,
            e);
        try {
          container.stop();
        } catch (RuntimeException stopFailure) {
          LOG.warn("Failed to stop cassandra container after failed start", stopFailure);
        }
      }
    }
    throw new IllegalStateException(
        "Shared cassandra container failed to start after " + STARTUP_ATTEMPTS + " attempts",
        lastFailure);
  }

  private static GenericContainer<?> newContainer() {
    // Environment as in org.testcontainers.containers.CassandraContainer, with a larger heap
    // because several tests use the same node at once.
    return new GenericContainer<>(DockerImageName.parse(IMAGE).withTag(TAG))
        .withExposedPorts(CQL_PORT)
        .withEnv("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch")
        .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
        .withEnv("CASSANDRA_DC", "datacenter1")
        .withEnv(
            "JVM_OPTS", "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0")
        .withEnv("HEAP_NEWSIZE", "400M")
        .withEnv("MAX_HEAP_SIZE", "2048M")
        .waitingFor(
            Wait.forLogMessage(".*Starting listening for CQL clients.*\\n", 1)
                .withStartupTimeout(Duration.ofMinutes(5)));
  }

  private void startSampling() {
    ScheduledExecutorService sampler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "shared-cassandra-sampler");
              thread.setDaemon(true);
              return thread;
            });
    sampler.scheduleAtFixedRate(
        this::logUsage, SAMPLE_INTERVAL_SECONDS, SAMPLE_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  /** Logs keyspaces (test keyspaces only) and connected clients, current and peak so far. */
  public void logUsage() {
    Map<String, String> usage = new LinkedHashMap<>();
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress(container.getHost(), getPort()))
            .withLocalDatacenter("datacenter1")
            .build()) {
      int keyspaces = 0;
      for (Row row : session.execute("SELECT keyspace_name FROM system_schema.keyspaces")) {
        String name = row.getString("keyspace_name");
        if (!name.startsWith("system")) {
          keyspaces++;
          keyspacesSeen.add(name);
        }
      }
      // Minus 1: this session's own connection.
      int clients = session.execute("SELECT * FROM system_views.clients").all().size() - 1;
      peakKeyspaces.accumulateAndGet(keyspaces, Math::max);
      peakClients.accumulateAndGet(clients, Math::max);
      usage.put("keyspacesSeen", String.valueOf(keyspacesSeen.size()));
      usage.put("keyspacesLeft", String.valueOf(keyspaces));
      usage.put("peakKeyspaces", String.valueOf(peakKeyspaces.get()));
      usage.put("currentConnections", String.valueOf(clients));
      usage.put("peakSampledConnections", String.valueOf(peakClients.get()));
    } catch (RuntimeException e) {
      LOG.warn("Failed to read usage of shared cassandra container", e);
    }
    LOG.info("Shared cassandra container usage [{}]: {}", SharedDatabaseServer.OWNER, usage);
  }

  /**
   * Host that tests and pipelines should connect to. Like Beam's per-test resource managers, this
   * is {@code -DhostIp} (default {@code localhost}), so Dataflow workers can reach the container on
   * the CI machine; the container itself listens on all interfaces of the docker host.
   */
  public String getHost() {
    return TestProperties.hostIp();
  }

  public int getPort() {
    return container.getMappedPort(CQL_PORT);
  }
}
