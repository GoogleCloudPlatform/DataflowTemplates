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
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Neo4j write transformation. */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);
  private final JobSpec jobSpec;
  private final ConnectionParams neoConnection;
  private final Target target;

  public Neo4jRowWriterTransform(JobSpec jobSpec, ConnectionParams neoConnection, Target target) {
    this.jobSpec = jobSpec;
    this.neoConnection = neoConnection;
    this.target = target;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    Config config = jobSpec.getConfig();
    // indices and constraints
    List<String> cyphers =
        CypherGenerator.getNodeIndexAndConstraintsCypherStatements(config, target);
    if (!cyphers.isEmpty()) {
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

    // set batch sizes
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
            neoConnection, unwindCypher, batchSize, false, "rows", getRowCastingFunction());

    return input
        .apply("Create KV pairs", CreateKvTransform.of(parallelism))
        .apply(target.getSequence() + ": Neo4j write " + target.getName(), ParDo.of(neo4jUnwindFn))
        .setRowSchema(input.getSchema());
  }

  private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
    return (row) -> DataCastingUtils.rowToNeo4jDataMap(row, target);
  }
}
