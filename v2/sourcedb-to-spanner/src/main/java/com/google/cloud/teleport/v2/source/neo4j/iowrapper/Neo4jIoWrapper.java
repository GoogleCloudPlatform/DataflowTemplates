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

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.source.neo4j.schema.Neo4jSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** IO Wrapper for Neo4j source mapping database tables. */
public final class Neo4jIoWrapper implements IoWrapper, Serializable {

  private final SourceSchema sourceSchema;
  private final ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;

  public Neo4jIoWrapper(
      String neo4jUri,
      String neo4jUser,
      String neo4jPassword,
      int readChunkSize,
      List<String> sourceTables) {

    SourceSchemaReference schemaReference =
        SourceSchemaReference.ofNeo4j(
            Neo4jSchemaReference.builder().setDatabaseName("neo4j").build());

    SourceSchema.Builder schemaBuilder = SourceSchema.builder().setSchemaReference(schemaReference);

    UnifiedTypeMapper typeMapper = new UnifiedTypeMapper(Neo4jMappingsProvider.getMapping());

    // 1. Construct Virtual Node Table Schema
    SourceTableSchema nodeTableSchema =
        SourceTableSchema.builder(typeMapper)
            .setTableName("GraphNode")
            .setPrimaryKeyColumns(ImmutableList.of("id"))
            .addSourceColumnNameToSourceColumnType(
                "id", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "label", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "properties", new SourceColumnType("JSON", new Long[0], null))
            .build();
    schemaBuilder.addTableSchema(nodeTableSchema);

    // 2. Construct Virtual Edge Table Schema
    SourceTableSchema edgeTableSchema =
        SourceTableSchema.builder(typeMapper)
            .setTableName("GraphEdge")
            .setPrimaryKeyColumns(ImmutableList.of("id", "dest_id", "edge_id"))
            .addSourceColumnNameToSourceColumnType(
                "id", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "dest_id", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "edge_id", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "label", new SourceColumnType("STRING", new Long[0], null))
            .addSourceColumnNameToSourceColumnType(
                "properties", new SourceColumnType("JSON", new Long[0], null))
            .build();
    schemaBuilder.addTableSchema(edgeTableSchema);

    this.sourceSchema = schemaBuilder.build();

    // Construct table reader transforms
    ImmutableMap.Builder<
            ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        readersBuilder = ImmutableMap.builder();

    SourceTableReference nodeRef =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaReference)
            .setSourceTableSchemaUUID(nodeTableSchema.tableSchemaUUID())
            .setSourceTableName("GraphNode")
            .build();
    readersBuilder.put(
        ImmutableList.of(nodeRef),
        new Neo4jTableReader(neo4jUri, neo4jUser, neo4jPassword, readChunkSize, nodeTableSchema));

    SourceTableReference edgeRef =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaReference)
            .setSourceTableSchemaUUID(edgeTableSchema.tableSchemaUUID())
            .setSourceTableName("GraphEdge")
            .build();
    readersBuilder.put(
        ImmutableList.of(edgeRef),
        new Neo4jTableReader(neo4jUri, neo4jUser, neo4jPassword, readChunkSize, edgeTableSchema));

    this.tableReaders = readersBuilder.build();
  }

  @Override
  public ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return this.tableReaders;
  }

  @Override
  public ImmutableList<SourceSchema> discoverTableSchema() {
    return ImmutableList.of(this.sourceSchema);
  }
}
