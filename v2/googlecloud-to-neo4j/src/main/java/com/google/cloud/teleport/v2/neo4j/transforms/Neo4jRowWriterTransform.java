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
import com.google.cloud.teleport.v2.neo4j.database.Neo4jCapabilities;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.neo4j.driver.TransactionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Neo4j write transformation. */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);
  private final JobSpec jobSpec;
  private final Target target;
  private final SerializableSupplier<Neo4jConnection> connectionSupplier;

  public Neo4jRowWriterTransform(
      JobSpec jobSpec, ConnectionParams neoConnection, String templateVersion, Target target) {
    this(jobSpec, target, () -> new Neo4jConnection(neoConnection, templateVersion));
  }

  @VisibleForTesting
  Neo4jRowWriterTransform(
      JobSpec jobSpec, Target target, SerializableSupplier<Neo4jConnection> connectionSupplier) {
    this.jobSpec = jobSpec;
    this.target = target;
    this.connectionSupplier = connectionSupplier;
  }

  @NonNull
  @Override
  public PCollection<Row> expand(@NonNull PCollection<Row> input) {
    TargetType targetType = target.getType();
    ReportedSourceType reportedSourceType = determineReportedSourceType();
    if (targetType != TargetType.custom_query) {
      createIndicesAndConstraints(reportedSourceType);
    }

    Config config = jobSpec.getConfig();
    int batchSize;
    int parallelism;
    switch (targetType) {
      case node:
        batchSize = config.getNodeBatchSize();
        parallelism = config.getNodeParallelism();
        break;
      case edge:
        batchSize = config.getEdgeBatchSize();
        parallelism = config.getEdgeParallelism();
        break;
      case custom_query:
        batchSize = config.getCustomQueryBatchSize();
        parallelism = config.getCustomQueryParallelism();
        break;
      default:
        throw new IllegalStateException(String.format("Unsupported target type: %s", targetType));
    }

    Neo4jBlockingUnwindFn neo4jUnwindFn =
        new Neo4jBlockingUnwindFn(
            reportedSourceType,
            targetType,
            getCypherQuery(),
            false,
            "rows",
            getRowCastingFunction(),
            connectionSupplier);

    return input
        .apply("Create KV pairs", CreateKvTransform.of(parallelism))
        .apply("Group by keys", GroupByKey.create())
        .apply("Split into batches", ParDo.of(SplitIntoBatches.of(batchSize)))
        .apply(target.getSequence() + ": Neo4j write " + target.getName(), ParDo.of(neo4jUnwindFn))
        .setRowSchema(input.getSchema());
  }

  private ReportedSourceType determineReportedSourceType() {
    String sourceName = target.getSource();
    Source source = jobSpec.getSources().get(sourceName);
    return ReportedSourceType.reportedSourceTypeOf(source);
  }

  private void createIndicesAndConstraints(ReportedSourceType reportedSourceType) {
    try (Neo4jConnection neo4jDirectConnect = connectionSupplier.get()) {
      var capabilities = neo4jDirectConnect.capabilities();

      Set<String> cyphers = generateIndexAndConstraints(capabilities);
      if (cyphers.isEmpty()) {
        return;
      }

      LOG.info("Adding {} indices and constraints", cyphers.size());
      for (String cypher : cyphers) {
        LOG.info("Executing cypher: {}", cypher);
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
                              target.getType().name(),
                              "step",
                              "init-schema")))
                  .build();
          neo4jDirectConnect.executeCypher(cypher, txConfig);
        } catch (Exception e) {
          LOG.error("Error executing cypher: {}, {}", cypher, e.getMessage());
        }
      }
    }
  }

  private String getCypherQuery() {
    if (target.getType() == TargetType.custom_query) {
      String cypher = target.getCustomQuery();
      LOG.info("Custom cypher query: {}", cypher);
      return cypher;
    }
    String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
    LOG.info("Unwind cypher: {}", unwindCypher);
    return unwindCypher;
  }

  private Set<String> generateIndexAndConstraints(Neo4jCapabilities capabilities) {
    return CypherGenerator.getIndexAndConstraintsCypherStatements(target, capabilities);
  }

  private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
    return (row) -> DataCastingUtils.rowToNeo4jDataMap(row, target);
  }
}
