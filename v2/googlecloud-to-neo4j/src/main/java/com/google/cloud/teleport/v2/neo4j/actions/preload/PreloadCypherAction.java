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
package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.CypherAction;
import org.neo4j.importer.v1.actions.CypherExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cypher runner action handler. */
public class PreloadCypherAction implements PreloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(PreloadCypherAction.class);
  private final SerializableBiFunction<ConnectionParams, String, Neo4jConnection>
      connectionProvider;

  private String cypher;
  private ActionContext context;
  private CypherExecutionMode executionMode;

  public PreloadCypherAction() {
    this(Neo4jConnection::new);
  }

  @VisibleForTesting
  PreloadCypherAction(
      SerializableBiFunction<ConnectionParams, String, Neo4jConnection> connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @Override
  public void configure(Action action, ActionContext context) {
    var cypherAction = (CypherAction) action;
    String cypher = cypherAction.getQuery();
    if (StringUtils.isEmpty(cypher)) {
      throw new RuntimeException("Cypher query not provided for preload cypher action.");
    }
    this.context = context;
    this.cypher = cypher;
    this.executionMode = cypherAction.getExecutionMode();
  }

  @Override
  public List<String> execute() {
    try (Neo4jConnection connection =
        connectionProvider.apply(
            this.context.getNeo4jConnectionParams(), this.context.getTemplateVersion())) {
      LOG.info("Executing cypher: {}", cypher);
      try {
        run(connection);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Exception running cypher, %s: %s", cypher, e.getMessage()), e);
      }
      return List.of();
    }
  }

  private void run(Neo4jConnection connection) {
    TransactionConfig txConfig =
        TransactionConfig.builder()
            .withMetadata(
                Neo4jTelemetry.transactionMetadata(
                    Map.of(
                        "sink", "neo4j",
                        "step", "cypher-preload-action",
                        "execution", executionMode.name().toLowerCase(Locale.ROOT))))
            .build();
    switch (executionMode) {
      case TRANSACTION:
        connection.writeTransaction(tx -> tx.run(cypher).consume(), txConfig);
        break;
      case AUTOCOMMIT:
        connection.runAutocommit(cypher, txConfig);
        break;
    }
  }
}
