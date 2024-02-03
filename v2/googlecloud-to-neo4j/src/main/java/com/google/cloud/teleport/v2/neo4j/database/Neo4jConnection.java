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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
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
  public Neo4jConnection(ConnectionParams settings) {
    this(
        settings.getDatabase(),
        () -> GraphDatabase.driver(settings.getServerUrl(), settings.asAuthToken()));
  }

  @VisibleForTesting
  Neo4jConnection(String database, Supplier<Driver> driverSupplier) {
    this.database = database;
    this.driverSupplier = driverSupplier;
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
  public <T> T writeTransaction(TransactionWork<T> transactionWork) {
    try (Session session = getSession()) {
      return session.writeTransaction(transactionWork);
    }
  }

  /** Completely delete "neo4j" or named database. */
  public void resetDatabase() {
    // Direct connect utility...
    LOG.info("Resetting database");
    try {
      String database = !StringUtils.isEmpty(this.database) ? this.database : "neo4j";
      String cypher = "CREATE OR REPLACE DATABASE $db";
      LOG.info(
          "Executing CREATE OR REPLACE DATABASE Cypher query: {} against database {}",
          cypher,
          database);
      executeCypher(cypher, Map.of("db", database));
    } catch (Exception ex) {
      fallbackResetDatabase(ex);
    }
  }

  /**
   * Execute cypher.
   *
   * @param cypher statement
   */
  public void executeCypher(String cypher) {
    executeCypher(cypher, Collections.emptyMap());
  }

  /**
   * Execute a parameterized cypher statement.
   *
   * @param cypher statement
   */
  public void executeCypher(String cypher, Map<String, Object> parameters) {
    try (Session session = getSession()) {
      session.run(cypher, parameters).consume();
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

  private void fallbackResetDatabase(Exception initialException) {
    try {
      String ddeCypher = "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS";
      LOG.info("Executing alternative delete Cypher query: {}", ddeCypher);
      executeCypher(ddeCypher);
      String constraintsDeleteCypher = "CALL apoc.schema.assert({}, {}, true)";
      LOG.info("Dropping indices & constraints with APOC: {}", constraintsDeleteCypher);
      executeCypher(constraintsDeleteCypher);
    } catch (Exception exception) {
      exception.addSuppressed(initialException);
      LOG.error(
          "Error resetting database: "
              + "make sure the configured Neo4j user is allowed to run 'CREATE OR REPLACE DATABASE'"
              + " or APOC is installed.\n"
              + "Alternatively, disable database reset by setting 'reset_db' to false in the job specification.",
          exception);
      Throwables.throwIfUnchecked(exception);
      throw new RuntimeException(exception);
    }
  }
}
