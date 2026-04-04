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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Neo4jInitSchemaFn extends DoFn<Integer, Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jInitSchemaFn.class);

  private final Target target;
  private final ReportedSourceType reportedSourceType;
  private final SerializableSupplier<Neo4jConnection> connectionSupplier;

  Neo4jInitSchemaFn(
      Target target,
      ReportedSourceType reportedSourceType,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {
    this.target = target;
    this.reportedSourceType = reportedSourceType;
    this.connectionSupplier = connectionSupplier;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    initSchema(target, reportedSourceType, connectionSupplier);
    context.output(1);
  }

  private static void initSchema(
      Target target,
      ReportedSourceType reportedSourceType,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {
    try (Neo4jConnection connection = connectionSupplier.get()) {
      var capabilities = connection.capabilities();
      var statements = CypherGenerator.getSchemaStatements((EntityTarget) target, capabilities);
      if (statements.isEmpty()) {
        return;
      }

      LOG.info("Adding {} indices and constraints", statements.size());
      for (String statement : statements) {
        LOG.info("Executing cypher: {}", statement);
        try {
          TransactionConfig txConfig =
              TransactionConfig.builder()
                  .withMetadata(
                      Neo4jTelemetry.transactionMetadata(
                          Map.of(
                              "sink",
                              "neo4j",
                              "source",
                              reportedSourceType.format(),
                              "target-type",
                              target.getTargetType().name().toLowerCase(Locale.ROOT),
                              "step",
                              "init-schema")))
                  .build();
          connection.runAutocommit(statement, txConfig);
        } catch (Exception e) {
          LOG.error("Error executing cypher: {}, {}", statement, e.getMessage());
        }
      }
    }
  }
}
