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
package com.google.cloud.teleport.v2.source.neo4j.iowrapper;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.neo4j.rowmapper.Neo4jRowMapper;
import com.google.cloud.teleport.v2.source.neo4j.schema.Neo4jSchemaReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Custom Beam PTransform to partition and query Neo4j node/edge tables. */
public class Neo4jTableReader extends PTransform<PBegin, PCollection<SourceRow>> {
  private static final Logger LOG = LoggerFactory.getLogger(Neo4jTableReader.class);

  private final String uri;
  private final String user;
  private final String password;
  private final int chunkSize;
  private final SourceTableSchema sourceTableSchema;

  public Neo4jTableReader(
      String uri,
      String user,
      String password,
      int chunkSize,
      SourceTableSchema sourceTableSchema) {
    this.uri = uri;
    this.user = user;
    this.password = password;
    this.chunkSize = chunkSize;
    this.sourceTableSchema = sourceTableSchema;
  }

  private long fetchMaxId(String query) {
    try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        Session session = driver.session()) {
      Result result = session.run(query);
      if (result.hasNext()) {
        Record record = result.next();
        Value val = record.get(0);
        if (!val.isNull()) {
          return val.asLong();
        }
      }
    } catch (Exception e) {
      LOG.warn("Error querying max ID from Neo4j (defaulting to 0):", e);
    }
    return 0L;
  }

  @Override
  public PCollection<SourceRow> expand(PBegin input) {
    String tableName = sourceTableSchema.tableName();
    String elementType = "GraphNode".equals(tableName) ? "node" : "edge";
    String maxQuery = "node".equals(elementType)
        ? "MATCH (n) RETURN max(id(n))"
        : "MATCH ()-[r]->() RETURN max(id(r))";

    long maxId = fetchMaxId(maxQuery);
    LOG.info("Neo4j database returned max {} ID: {}", elementType, maxId);

    List<Neo4jPartitionTask> partitionTasks = new ArrayList<>();
    for (long i = 0; i <= maxId; i += chunkSize) {
      partitionTasks.add(new Neo4jPartitionTask(elementType, i, i + chunkSize));
    }
    if (partitionTasks.isEmpty()) {
      partitionTasks.add(new Neo4jPartitionTask(elementType, 0, 0));
    }

    return input
        .apply("CreatePartitions_" + tableName, Create.of(partitionTasks))
        .apply(
            "ReadFromNeo4j_" + tableName,
            ParDo.of(new ReadFromNeo4jFn(uri, user, password, sourceTableSchema)));
  }

  /** DoFn that runs on workers to query ranges of Neo4j IDs. */
  private static class ReadFromNeo4jFn extends DoFn<Neo4jPartitionTask, SourceRow> {
    private final String uri;
    private final String user;
    private final String password;
    private final SourceTableSchema sourceTableSchema;
    private transient Driver driver;
    private transient Neo4jRowMapper rowMapper;

    public ReadFromNeo4jFn(
        String uri, String user, String password, SourceTableSchema sourceTableSchema) {
      this.uri = uri;
      this.user = user;
      this.password = password;
      this.sourceTableSchema = sourceTableSchema;
    }

    @Setup
    public void setup() {
      this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
      // Construct schema reference referencing the virtual database
      SourceSchemaReference schemaReference =
          SourceSchemaReference.ofNeo4j(
              Neo4jSchemaReference.builder().setDatabaseName("neo4j").build());
      this.rowMapper = new Neo4jRowMapper(schemaReference, sourceTableSchema);
    }

    @Teardown
    public void teardown() {
      if (this.driver != null) {
        this.driver.close();
      }
    }

    @ProcessElement
    public void processElement(@Element Neo4jPartitionTask task, OutputReceiver<SourceRow> out)
        throws Exception {
      try (Session session = driver.session()) {
        if ("node".equals(task.getElementType())) {
          extractNodes(session, task, out);
        } else {
          extractEdges(session, task, out);
        }
      }
    }

    private void extractNodes(
        Session session, Neo4jPartitionTask task, OutputReceiver<SourceRow> out) throws Exception {
      String cypher =
          "MATCH (n) WHERE id(n) >= $startId AND id(n) < $endId "
              + "RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties";

      Map<String, Object> parameters = new HashMap<>();
      parameters.put("startId", task.getStartId());
      parameters.put("endId", task.getEndId());

      Result result = session.run(cypher, parameters);
      while (result.hasNext()) {
        Record record = result.next();
        out.output(rowMapper.map(record));
      }
    }

    private void extractEdges(
        Session session, Neo4jPartitionTask task, OutputReceiver<SourceRow> out) throws Exception {
      String cypher =
          "MATCH (src)-[r]->(dest) WHERE id(r) >= $startId AND id(r) < $endId "
              + "RETURN id(src) AS src_id, id(dest) AS dest_id, id(r) AS edge_id, type(r) AS label, properties(r) AS properties";

      Map<String, Object> parameters = new HashMap<>();
      parameters.put("startId", task.getStartId());
      parameters.put("endId", task.getEndId());

      Result result = session.run(cypher, parameters);
      while (result.hasNext()) {
        Record record = result.next();
        out.output(rowMapper.map(record));
      }
    }
  }
}
