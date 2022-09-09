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
import com.google.cloud.teleport.v2.neo4j.model.enums.AuthType;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.AuthTokens;
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
public class Neo4jConnection implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jConnection.class);
  private final String username;
  private final String password;
  private String serverUrl;
  private String database;
  private AuthType authType = AuthType.BASIC;
  private Driver driver;

  /** Constructor. */
  public Neo4jConnection(ConnectionParams connectionParams) {
    this.username = connectionParams.username;
    this.password = connectionParams.password;
    this.database = connectionParams.database;
    this.serverUrl = connectionParams.serverUrl;
  }

  public Neo4jConnection(
      String hostName, int port, String database, String username, String password) {
    this.username = username;
    this.password = password;
    this.database = database;
    this.serverUrl = getUrl(hostName, port);
  }

  /** Constructor. */
  public Neo4jConnection(String serverUrl, String database, String username, String password) {
    this.username = username;
    this.password = password;
    this.database = database;
    this.serverUrl = serverUrl;
  }

  private String getUrl(String hostName, int port) {
    return "neo4j+s://" + hostName + ":" + port;
  }

  /** Helper method to get the Neo4j driver. */
  public Driver getDriver() throws URISyntaxException {
    if (this.authType != AuthType.BASIC) {
      LOG.error("Unsupported authType: {}", this.authType);
      throw new RuntimeException("Unsupported authentication type: " + this.authType);
    }
    if (this.serverUrl.contains("neo4j+s")) {
      return GraphDatabase.driver(
          new URI(this.serverUrl),
          AuthTokens.basic(this.username, this.password),
          Config.builder().build());
    } else {
      return GraphDatabase.routingDriver(
          Arrays.asList(new URI(this.serverUrl)),
          AuthTokens.basic(this.username, this.password),
          Config.builder().build());
    }
  }

  /** Helper method to get the Neo4j session. */
  public Session getSession() throws URISyntaxException {
    if (driver == null) {
      this.driver = getDriver();
    }
    SessionConfig.Builder builder = SessionConfig.builder();
    if (StringUtils.isNotEmpty(this.database)) {
      builder = builder.withDatabase(this.database);
    }
    return driver.session(builder.build());
  }

  /**
   * Execute cypher.
   *
   * @param cypher statement
   */
  public void executeCypher(String cypher) throws URISyntaxException {
    try (Session session = getSession()) {
      session.run(cypher);
    }
  }

  /** Write transaction. */
  public void writeTransaction(
      TransactionWork<Void> transactionWork, TransactionConfig transactionConfig)
      throws URISyntaxException {
    try (Session session = getSession()) {
      session.writeTransaction(transactionWork, transactionConfig);
    }
  }

  /** Completely delete "neo4j" or named database. */
  public void resetDatabase() {
    // Direct connect utility...
    LOG.info("Resetting database");
    String deleteCypher = "CREATE OR REPLACE DATABASE `neo4j`";
    try {

      if (!StringUtils.isEmpty(database)) {
        StringUtils.replace(deleteCypher, "neo4j", database);
      }
      LOG.info("Executing cypher: {}", deleteCypher);
      executeCypher(deleteCypher);

    } catch (Exception e) {
      LOG.error("Error executing cypher: {}, {}", deleteCypher, e.getMessage());
    }
  }
}
