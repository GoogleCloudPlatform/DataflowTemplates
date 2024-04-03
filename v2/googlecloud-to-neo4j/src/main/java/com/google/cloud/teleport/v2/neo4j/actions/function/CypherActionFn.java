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
package com.google.cloud.teleport.v2.neo4j.actions.function;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.TransactionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cypher action handler. */
public class CypherActionFn extends DoFn<Integer, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CypherActionFn.class);

  private final String cypher;
  private final Supplier<Neo4jConnection> connectionProvider;

  private Neo4jConnection directConnect;

  public CypherActionFn(ActionContext context) {
    this(
        context, () -> new Neo4jConnection(context.neo4jConnectionParams, context.templateVersion));
  }

  @VisibleForTesting
  CypherActionFn(ActionContext context, Supplier<Neo4jConnection> connectionProvider) {
    String cypher = context.action.options.get("cypher");
    if (StringUtils.isEmpty(cypher)) {
      throw new RuntimeException("Options 'cypher' not provided for cypher action transform.");
    }
    this.cypher = cypher;
    this.connectionProvider = connectionProvider;
  }

  @Setup
  public void setup() {
    directConnect = connectionProvider.get();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    LOG.info("Executing cypher action: {}", cypher);
    try {
      TransactionConfig txConfig =
          TransactionConfig.builder()
              .withMetadata(
                  Neo4jTelemetry.transactionMetadata(
                      Map.of(
                          "sink", "neo4j",
                          "step", "cypher-action")))
              .build();
      directConnect.executeCypher(cypher, txConfig);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Exception running cypher action %s: %s", cypher, e.getMessage()), e);
    }
  }

  @Teardown
  public void tearDown() {
    if (directConnect != null) {
      directConnect.close();
    }
  }
}
