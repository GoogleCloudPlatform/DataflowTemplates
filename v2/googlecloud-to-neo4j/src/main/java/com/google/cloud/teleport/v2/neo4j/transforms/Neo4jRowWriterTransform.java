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
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Neo4j write transformation. */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);
  private final JobSpec jobSpec;
  private final ConnectionParams neoConnection;
  private final OptionsParams optionsParams;
  private final TargetType targetType;
  private final Target target;

  public Neo4jRowWriterTransform(
      JobSpec jobSpec,
      ConnectionParams neoConnection,
      OptionsParams optionsParams,
      TargetType targetType,
      Target target) {
    this.jobSpec = jobSpec;
    this.neoConnection = neoConnection;
    this.optionsParams = optionsParams;
    this.targetType = targetType;
    this.target = target;
  }

  @NonNull
  @Override
  public PCollection<Row> expand(@NonNull PCollection<Row> input) {
    createIndicesAndConstraints();

    int batchSize = jobSpec.getConfig().getNodeBatchSize();
    int parallelism = jobSpec.getConfig().getNodeParallelism();
    if (target.getType() == TargetType.edge) {
      batchSize = jobSpec.getConfig().getEdgeBatchSize();
      parallelism = jobSpec.getConfig().getEdgeParallelism();
    }

    // data loading
    String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
    LOG.info("Unwind cypher: {}", unwindCypher);

    Neo4jBlockingUnwindFn neo4jUnwindFn =
        new Neo4jBlockingUnwindFn(
            neoConnection,
            optionsParams,
            unwindCypher,
            batchSize,
            false,
            "rows",
            getRowCastingFunction());

    return input
        .apply("Create KV pairs", CreateKvTransform.of(parallelism))
        .apply(target.getSequence() + ": Neo4j write " + target.getName(), ParDo.of(neo4jUnwindFn))
        .setRowSchema(input.getSchema());
  }

  private void createIndicesAndConstraints() {
    List<String> cyphers = generateIndexAndConstraints();
    if (cyphers.isEmpty()) {
      return;
    }
    try (Neo4jConnection neo4jDirectConnect = new Neo4jConnection(neoConnection)) {
      LOG.info("Adding {} indices and constraints", cyphers.size());
      for (String cypher : cyphers) {
        LOG.info("Executing cypher: {}", cypher);
        try {
          neo4jDirectConnect.executeCypher(cypher);
        } catch (Exception e) {
          LOG.error("Error executing cypher: {}, {}", cypher, e.getMessage());
        }
      }
    }
  }

  private List<String> generateIndexAndConstraints() {
    if (target.getType() == TargetType.edge
        && target.getEdgeNodesMatchMode() == EdgeNodesMatchMode.merge) {
      return CypherGenerator.getEdgeNodeConstraintsCypherStatements(target);
    }
    return CypherGenerator.getIndexAndConstraintsCypherStatements(
        targetType, jobSpec.getConfig(), target);
  }

  private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
    return (row) -> DataCastingUtils.rowToNeo4jDataMap(row, target);
  }
}
