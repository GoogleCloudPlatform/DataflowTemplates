/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.database;

import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Neo4j connection helper object wraps Neo4j java APIs. */
public class Neo4jConnection implements AutoCloseable, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jConnection.class);
  private final Supplier<Driver> driverSupplier;
  private final String database;
  private Driver driver;
  private Session session;

  /** Constructor. */
  public Neo4jConnection(ConnectionParams settings, String templateVersion) {
    this(
        settings.getDatabase(),
        () ->
            GraphDatabase.driver(
                settings.getServerUrl(),
                settings.asAuthToken(),
                Config.builder().withUserAgent(Neo4jTelemetry.userAgent(templateVersion)).build()));
  }

  @VisibleForTesting
  Neo4jConnection(String database, Supplier<Driver> driverSupplier) {
    this.database = database;
    this.driverSupplier = driverSupplier;
  }

  public Neo4jCapabilities capabilities() {
    try (var session = getSession()) {
      var result =
          session
              .run(
                  "CALL dbms.components() YIELD name, versions, edition WHERE name = $kernel RETURN versions[0], edition",
                  Map.of("kernel", "Neo4j Kernel"))
              .single();

      return new Neo4jCapabilities(result.get(0).asString(), result.get(1).asString());
    }
  }

  /** Helper method to get the Neo4j session. */
  public Session getSession() {
    if (driver == null) {
      this.driver = getDriver();
    }
    if (session == null || !session.isOpen()) {
      SessionConfig.Builder builder = SessionConfig.builder();
      if (StringUtils.isNotEmpty(this.database)) {
        builder = builder.withDatabase(this.database);
      }
      this.session = driver.session(builder.build());
    }
    return this.session;
  }

  /** Write transaction. */
  public <T> T writeTransaction(TransactionWork<T> transactionWork, TransactionConfig txConfig) {
    try (Session session = getSession()) {
      return session.writeTransaction(transactionWork, txConfig);
    }
  }

  /** Completely delete "neo4j" or named database. */
  public void resetDatabase() {
    // Direct connect utility...
    LOG.info("Resetting database");
    try {
      var capabilities = capabilities();

      if (capabilities.hasCreateOrReplaceDatabase()) {
        recreateDatabase(capabilities);
      } else {
        deleteData();
        dropSchema(capabilities);
      }
    } catch (Exception exception) {
      LOG.error(
          "Error resetting database: "
              + "make sure the configured Neo4j user is allowed to run 'CREATE OR REPLACE DATABASE', "
              + "'SHOW CONSTRAINTS', 'DROP CONSTRAINT', 'SHOW INDEXES' and 'DROP INDEXES'.\n"
              + "Alternatively, disable database reset by setting 'reset_db' to false in the job specification.",
          exception);
    }
  }

  private void recreateDatabase(Neo4jCapabilities capabilities) {
    try {
      String database = !StringUtils.isEmpty(this.database) ? this.database : "neo4j";
      String cypher = "CREATE OR REPLACE DATABASE $db WAIT 60 SECONDS";
      LOG.info(
          "Executing CREATE OR REPLACE DATABASE Cypher query: {} against database {}",
          cypher,
          database);
      runAutocommit(
          cypher, Map.of("db", database), databaseResetMetadata("create-replace-database"));
    } catch (Exception ex) {
      deleteData();
      dropSchema(capabilities);
    }
  }

  private void deleteData() {
    String ddeCypher = "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS";
    LOG.info("Executing delete Cypher query: {}", ddeCypher);
    runAutocommit(ddeCypher, databaseResetMetadata("cit-detach-delete"));
  }

  private void dropSchema(Neo4jCapabilities capabilities) {
    try (var session = getSession()) {
      if (capabilities.hasConstraints()) {
        LOG.info("Dropping constraints");
        var constraints =
            session
                .run(
                    "SHOW CONSTRAINTS YIELD name",
                    Map.of(),
                    databaseResetMetadata("show-constraints"))
                .list(r -> r.get(0).asString());
        for (var constraint : constraints) {
          LOG.info("Dropping constraint {}", constraint);

          runAutocommit(
              String.format("DROP CONSTRAINT %s", CypherPatterns.sanitize(constraint)),
              Map.of(),
              databaseResetMetadata("drop-constraint"));
        }
      }

      LOG.info("Dropping indexes");
      var indexes =
          session
              .run(
                  "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name",
                  Map.of(),
                  databaseResetMetadata("show-indexes"))
              .list(r -> r.get(0).asString());
      for (var index : indexes) {
        LOG.info("Dropping index {}", index);

        runAutocommit(
            String.format("DROP INDEX %s", CypherPatterns.sanitize(index)),
            Map.of(),
            databaseResetMetadata("drop-index"));
      }
    }
  }

  /**
   * Execute cypher.
   *
   * @param cypher statement
   */
  public void runAutocommit(String cypher, TransactionConfig transactionConfig) {
    runAutocommit(cypher, Collections.emptyMap(), transactionConfig);
  }

  /**
   * Execute a parameterized cypher statement.
   *
   * @param cypher statement
   */
  public void runAutocommit(
      String cypher, Map<String, Object> parameters, TransactionConfig transactionConfig) {
    try (Session session = getSession()) {
      session.run(cypher, parameters, transactionConfig).consume();
    }
  }

  public void verifyConnectivity() {
    if (this.driver == null) {
      this.driver = getDriver();
    }
    this.driver.verifyConnectivity();
  }

  @Override
  public void close() {
    if (this.session != null && this.session.isOpen()) {
      this.session.close();
      this.session = null;
    }
    if (this.driver != null) {
      this.driver.close();
      this.driver = null;
    }
  }

  /** Helper method to get the Neo4j driver. */
  private Driver getDriver() {
    return driverSupplier.get();
  }

  private static TransactionConfig databaseResetMetadata(String resetMethod) {
    return TransactionConfig.builder()
        .withMetadata(
            Neo4jTelemetry.transactionMetadata(Map.of("sink", "neo4j", "step", resetMethod)))
        .build();
  }
}
