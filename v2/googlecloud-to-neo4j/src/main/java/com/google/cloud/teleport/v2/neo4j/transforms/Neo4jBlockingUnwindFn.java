/*
 * Copyright (C) 2022 Google LLC
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Write to Neo4j synchronously, called from inside @Neo4jRowWriterTransform. */
public class Neo4jBlockingUnwindFn extends DoFn<KV<Integer, Iterable<Row>>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jBlockingUnwindFn.class);
  private final ImportSpecification importSpecification;
  private final Target target;
  private final String staticCypher;
  private final SerializableFunction<Row, Map<String, Object>> parametersFunction;
  private final boolean logCypher;
  private final String unwindMapName;
  private final SerializableSupplier<Neo4jConnection> connectionSupplier;
  private final List<Map<String, Object>> parameters;
  private final ReportedSourceType reportedSourceType;
  private final TargetType targetType;
  private transient String cypher;
  private boolean loggingDone;
  private transient Neo4jConnection neo4jConnection;

  public Neo4jBlockingUnwindFn(
      ReportedSourceType reportedSourceType,
      ImportSpecification importSpecification,
      Target target,
      boolean logCypher,
      String unwindMapName,
      SerializableFunction<Row, Map<String, Object>> parametersFunction,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {

    this(
        reportedSourceType,
        target.getTargetType(),
        null,
        importSpecification,
        target,
        logCypher,
        unwindMapName,
        parametersFunction,
        connectionSupplier);
  }

  public Neo4jBlockingUnwindFn(
      ReportedSourceType reportedSourceType,
      TargetType targetType,
      String cypher,
      boolean logCypher,
      String unwindMapName,
      SerializableFunction<Row, Map<String, Object>> parametersFunction,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {

    this(
        reportedSourceType,
        targetType,
        cypher,
        null,
        null,
        logCypher,
        unwindMapName,
        parametersFunction,
        connectionSupplier);
  }

  private Neo4jBlockingUnwindFn(
      ReportedSourceType reportedSourceType,
      TargetType targetType,
      String staticCypher,
      ImportSpecification importSpecification,
      Target target,
      boolean logCypher,
      String unwindMapName,
      SerializableFunction<Row, Map<String, Object>> parametersFunction,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {

    this.reportedSourceType = reportedSourceType;
    this.targetType = targetType;
    this.staticCypher = staticCypher;
    this.importSpecification = importSpecification;
    this.target = target;
    this.parametersFunction = parametersFunction;
    this.logCypher = logCypher;
    this.unwindMapName = unwindMapName;
    this.connectionSupplier = connectionSupplier;

    parameters = new ArrayList<>();
    loggingDone = false;
  }

  @Setup
  public void setup() {
    this.neo4jConnection = connectionSupplier.get();
    this.cypher = resolveCypher();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    KV<Integer, Iterable<Row>> rowBatch = context.element();
    LOG.debug("Processing row batch from key: {}", rowBatch.getKey());

    Iterable<Row> rows = rowBatch.getValue();
    rows.forEach(row -> parameters.add(parametersFunction.apply(row)));
    executeCypherUnwindStatement();
  }

  @Teardown
  public void tearDown() {
    if (this.neo4jConnection != null) {
      this.neo4jConnection.close();
    }
  }

  private void executeCypherUnwindStatement() {
    if (this.parameters.isEmpty()) {
      return;
    }

    final Map<String, Object> parametersMap = new HashMap<>();
    parametersMap.put(unwindMapName, parameters);

    if (logCypher && !loggingDone) {
      String parametersString = getParametersString(parametersMap);
      LOG.debug(
          "Starting a write transaction for unwind statement cypher: "
              + cypher
              + ", parameters: "
              + parametersString);
      loggingDone = true;
    }

    try {
      ResultSummary summary =
          neo4jConnection.writeTransaction(
              transaction -> transaction.run(cypher, parametersMap).consume(),
              TransactionConfig.builder()
                  .withMetadata(
                      Neo4jTelemetry.transactionMetadata(
                          Map.of(
                              "sink",
                              "neo4j",
                              "source",
                              reportedSourceType.format(),
                              "target-type",
                              targetType.name().toLowerCase(Locale.ROOT),
                              "step",
                              "import")))
                  .build());
      LOG.debug("Batch transaction of {} rows completed: {}", parameters.size(), summary);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error writing " + parameters.size() + " rows to Neo4j with Cypher: " + cypher, e);
    }
    parameters.clear();
  }

  private String resolveCypher() {
    if (target == null) {
      return staticCypher;
    }

    if (targetType == TargetType.QUERY) {
      var query = ((CustomQueryTarget) target).getQuery();
      LOG.info("Custom cypher query: {}", query);
      return query;
    }

    var capabilities = neo4jConnection.capabilities();
    var query =
        CypherGenerator.getImportStatement(
            importSpecification, (EntityTarget) target, capabilities);
    LOG.info("Unwind cypher query: {}", query);
    return query;
  }

  private static String getParametersString(Map<String, Object> parametersMap) {
    StringBuilder parametersString = new StringBuilder();
    parametersMap
        .keySet()
        .forEach(
            key -> {
              if (parametersString.length() > 0) {
                parametersString.append(',');
              }
              parametersString.append(key).append('=');
              Object value = parametersMap.get(key);
              if (value == null) {
                parametersString.append("<null>");
              } else {
                parametersString.append(value);
              }
            });
    return parametersString.toString();
  }
}
