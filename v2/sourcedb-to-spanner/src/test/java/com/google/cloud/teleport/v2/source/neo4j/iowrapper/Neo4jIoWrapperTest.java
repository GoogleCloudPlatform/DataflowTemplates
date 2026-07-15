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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

/** Unit tests for {@link Neo4jIoWrapper}. */
public class Neo4jIoWrapperTest {

  @Test
  public void testDiscoverTableSchema() {
    Neo4jIoWrapper ioWrapper =
        new Neo4jIoWrapper(
            "bolt://localhost:7687", "neo4j", "password", 1000, ImmutableList.of());

    List<SourceSchema> schemas = ioWrapper.discoverTableSchema();
    assertEquals(1, schemas.size());

    SourceSchema sourceSchema = schemas.get(0);
    assertNotNull(sourceSchema);
    assertEquals("Database.neo4j", sourceSchema.schemaReference().getName());

    List<SourceTableSchema> tableSchemas = sourceSchema.tableSchemas();
    assertEquals(2, tableSchemas.size());

    Set<String> tableNames =
        tableSchemas.stream().map(SourceTableSchema::tableName).collect(Collectors.toSet());
    assertTrue(tableNames.contains("GraphNode"));
    assertTrue(tableNames.contains("GraphEdge"));

    // Verify GraphNode columns
    SourceTableSchema nodeSchema =
        tableSchemas.stream()
            .filter(s -> "GraphNode".equals(s.tableName()))
            .findFirst()
            .orElse(null);
    assertNotNull(nodeSchema);
    assertEquals(ImmutableList.of("id"), nodeSchema.primaryKeyColumns());
    assertTrue(nodeSchema.sourceColumnNameToSourceColumnType().containsKey("id"));
    assertTrue(nodeSchema.sourceColumnNameToSourceColumnType().containsKey("label"));
    assertTrue(nodeSchema.sourceColumnNameToSourceColumnType().containsKey("properties"));
    assertEquals("STRING", nodeSchema.sourceColumnNameToSourceColumnType().get("id").getName());
    assertEquals("STRING", nodeSchema.sourceColumnNameToSourceColumnType().get("label").getName());
    assertEquals("JSON", nodeSchema.sourceColumnNameToSourceColumnType().get("properties").getName());

    // Verify GraphEdge columns
    SourceTableSchema edgeSchema =
        tableSchemas.stream()
            .filter(s -> "GraphEdge".equals(s.tableName()))
            .findFirst()
            .orElse(null);
    assertNotNull(edgeSchema);
    assertEquals(ImmutableList.of("id", "dest_id", "edge_id"), edgeSchema.primaryKeyColumns());
    assertTrue(edgeSchema.sourceColumnNameToSourceColumnType().containsKey("id"));
    assertTrue(edgeSchema.sourceColumnNameToSourceColumnType().containsKey("dest_id"));
    assertTrue(edgeSchema.sourceColumnNameToSourceColumnType().containsKey("edge_id"));
    assertTrue(edgeSchema.sourceColumnNameToSourceColumnType().containsKey("label"));
    assertTrue(edgeSchema.sourceColumnNameToSourceColumnType().containsKey("properties"));
    assertEquals("STRING", edgeSchema.sourceColumnNameToSourceColumnType().get("id").getName());
    assertEquals("STRING", edgeSchema.sourceColumnNameToSourceColumnType().get("dest_id").getName());
    assertEquals("STRING", edgeSchema.sourceColumnNameToSourceColumnType().get("edge_id").getName());
    assertEquals("STRING", edgeSchema.sourceColumnNameToSourceColumnType().get("label").getName());
    assertEquals("JSON", edgeSchema.sourceColumnNameToSourceColumnType().get("properties").getName());
  }

  @Test
  public void testGetTableReaders() {
    Neo4jIoWrapper ioWrapper =
        new Neo4jIoWrapper(
            "bolt://localhost:7687", "neo4j", "password", 1000, ImmutableList.of());

    var readers = ioWrapper.getTableReaders();
    assertEquals(2, readers.size());

    Set<String> readerTables =
        readers.keySet().stream()
            .flatMap(List::stream)
            .map(SourceTableReference::sourceTableName)
            .collect(Collectors.toSet());
    assertTrue(readerTables.contains("GraphNode"));
    assertTrue(readerTables.contains("GraphEdge"));
  }
}
