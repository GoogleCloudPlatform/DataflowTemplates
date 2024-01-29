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

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.summary.ResultSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Write to Neo4j synchronously, called from inside @Neo4jRowWriterTransform. */
public class Neo4jBlockingUnwindFn extends DoFn<KV<Integer, Row>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jBlockingUnwindFn.class);
  private final Counter numRecords = Metrics.counter(Neo4jBlockingUnwindFn.class, "norecords");
  private final String templateVersion;
  private final String cypher;
  private SerializableFunction<Row, Map<String, Object>> parametersFunction = null;
  private final boolean logCypher;
  private final long batchSize;
  private final String unwindMapName;
  private long elementsInput;
  private boolean loggingDone;
  private final List<Map<String, Object>> unwindList;
  private final ConnectionParams connectionParams;
  private Neo4jConnection neo4jConnection;

  public Neo4jBlockingUnwindFn(
      ConnectionParams connectionParams,
      String templateVersion,
      String cypher,
      long batchSize,
      boolean logCypher,
      String unwindMapName,
      SerializableFunction<Row, Map<String, Object>> parametersFunction) {

    this.connectionParams = connectionParams;
    this.templateVersion = templateVersion;
    this.cypher = cypher;
    this.parametersFunction = parametersFunction;
    this.logCypher = logCypher;
    this.batchSize = batchSize;
    this.unwindMapName = unwindMapName;

    unwindList = new ArrayList<>();
    elementsInput = 0;
    loggingDone = false;
  }

  @Setup
  public void setup() {
    this.neo4jConnection = new Neo4jConnection(this.connectionParams, this.templateVersion);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {

    KV<Integer, Row> parameters = context.element();
    LOG.debug("Processing row from group/key: {}", parameters.getKey());

    if (parametersFunction != null) {
      // Every input element creates a new Map<String,Object> entry in unwindList
      //
      unwindList.add(parametersFunction.apply(parameters.getValue()));
    } else {
      // Someone is writing a bunch of static or procedurally generated values to Neo4j
      unwindList.add(Collections.emptyMap());
    }
    elementsInput++;

    if (elementsInput >= batchSize) {
      // Execute the cypher query with the collected parameters map
      //
      executeCypherUnwindStatement();
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    executeCypherUnwindStatement();
  }

  @Teardown
  public void tearDown() {
    this.neo4jConnection.close();
  }

  private void executeCypherUnwindStatement() {
    // In case of errors and no actual input read (error in mapper) we don't have input
    // So we don't want to execute any cypher in this case.  There's no need to generate even more
    // errors
    //
    if (this.elementsInput == 0) {
      return;
    }

    // Add the accumulated list to the overall parameters map
    // It contains a single parameter to unwind
    //
    final Map<String, Object> parametersMap = new HashMap<>();
    parametersMap.put(unwindMapName, unwindList);

    // Every "write" transaction writes a batch of elements to Neo4j.
    // The changes to the database are automatically committed.
    //
    TransactionWork<ResultSummary> transactionWork =
        transaction -> {
          return transaction.run(cypher, parametersMap).consume();
        };

    if (logCypher && !loggingDone) {
      String parametersString = getParametersString(parametersMap);
      LOG.info(
          "Starting a write transaction for unwind statement cypher: "
              + cypher
              + ", parameters: "
              + parametersString);
      loggingDone = true;
    }

    try {
      ResultSummary summary = neo4jConnection.writeTransaction(transactionWork);
      LOG.debug("Batch transaction of {} rows completed: {}", unwindList.size(), summary);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error writing " + unwindList.size() + " rows to Neo4j with Cypher: " + cypher, e);
    }

    // Now we need to reset the number of elements read and the parameters map
    unwindList.clear();
    elementsInput = 0;
  }

  protected String getParametersString(Map<String, Object> parametersMap) {
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
