/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class PropertyGraphTest {

  @Test
  public void testPropertyDeclaration() {
    PropertyGraph.PropertyDeclaration propertyDeclaration =
        new PropertyGraph.PropertyDeclaration("propertyA", "INT64");
    assertEquals("propertyA", propertyDeclaration.name);
    assertEquals("INT64", propertyDeclaration.type);
  }

  @Test
  public void testGraphElementLabel() {
    PropertyGraph.GraphElementLabel graphElementLabel =
        new PropertyGraph.GraphElementLabel("label1", ImmutableList.of("propertyA", "propertyB"));
    assertEquals("label1", graphElementLabel.name);
    assertEquals(ImmutableList.of("propertyA", "propertyB"), graphElementLabel.properties);
  }

  @Test
  public void testPropertyGraph() {
    GraphElementTable nodeTable =
        GraphElementTable.builder()
            .name("nodeTable")
            .baseTableName("baseNodeTable")
            .kind(GraphElementTable.Kind.NODE)
            .keyColumns(ImmutableList.of("nodeKey"))
            .labelToPropertyDefinitions(
                ImmutableList.of(
                    new GraphElementTable.LabelToPropertyDefinitions(
                        "nodeLabel",
                        ImmutableList.of(
                            new GraphElementTable.PropertyDefinition("propertyA", "valueA")))))
            .autoBuild();

    GraphElementTable edgeTable =
        GraphElementTable.builder()
            .name("edgeTable")
            .baseTableName("baseEdgeTable")
            .kind(GraphElementTable.Kind.EDGE)
            .keyColumns(ImmutableList.of("edgeKey1", "edgeKey2"))
            .labelToPropertyDefinitions(
                ImmutableList.of(
                    new GraphElementTable.LabelToPropertyDefinitions(
                        "edgeLabel",
                        ImmutableList.of(
                            new GraphElementTable.PropertyDefinition("propertyB", "valueB")))))
            .sourceNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "nodeTable", ImmutableList.of("nodeKey"), ImmutableList.of("edgeKey1")))
            .targetNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "nodeTable", ImmutableList.of("nodeKey"), ImmutableList.of("edgeKey2")))
            .autoBuild();

    PropertyGraph propertyGraph =
        PropertyGraph.builder()
            .name("myGraph")
            .addNodeTable(nodeTable)
            .addEdgeTable(edgeTable)
            .addPropertyDeclaration(new PropertyGraph.PropertyDeclaration("propertyA", "INT64"))
            .addLabel(
                new PropertyGraph.GraphElementLabel("nodeLabel", ImmutableList.of("propertyA")))
            .build();

    assertEquals("myGraph", propertyGraph.name());
    assertEquals(ImmutableList.of(nodeTable), propertyGraph.nodeTables());
    assertEquals(ImmutableList.of(edgeTable), propertyGraph.edgeTables());

    // Test getPropertyDeclaration
    PropertyGraph.PropertyDeclaration propertyDeclaration =
        propertyGraph.getPropertyDeclaration("propertyA");
    assertEquals("propertyA", propertyDeclaration.name);

    // Test getLabel
    PropertyGraph.GraphElementLabel label = propertyGraph.getLabel("nodeLabel");
    assertEquals("nodeLabel", label.name);

    // Test getNodeTable
    GraphElementTable retrievedNodeTable = propertyGraph.getNodeTable("nodeTable");
    assertEquals("nodeTable", retrievedNodeTable.name());

    // Test getEdgeTable
    GraphElementTable retrievedEdgeTable = propertyGraph.getEdgeTable("edgeTable");
    assertEquals("edgeTable", retrievedEdgeTable.name());

    String expectedPrettyPrint =
        "CREATE PROPERTY GRAPH myGraph\n"
            + "NODE TABLES(\n"
            + "baseNodeTable AS nodeTable\n"
            + " KEY (nodeKey)\n"
            + "LABEL nodeLabel PROPERTIES(valueA AS propertyA)"
            + ")\n"
            + "EDGE TABLES(\n"
            + "baseEdgeTable AS edgeTable\n"
            + " KEY (edgeKey1, edgeKey2)\n"
            + "SOURCE KEY(edgeKey1) REFERENCES nodeTable DESTINATION KEY(edgeKey2) REFERENCES nodeTable\n"
            + "LABEL edgeLabel PROPERTIES(valueB AS propertyB)"
            + ")";
    assertEquals(expectedPrettyPrint, propertyGraph.prettyPrint());
  }

  @Test
  public void testPropertyGraphBuilder() {
    PropertyGraph.Builder builder = PropertyGraph.builder();
    builder.name("myGraph");
    builder.dialect(Dialect.GOOGLE_STANDARD_SQL);

    GraphElementTable nodeTable =
        GraphElementTable.builder(builder.dialect())
            .name("nodeTable")
            .baseTableName("baseNodeTable")
            .kind(GraphElementTable.Kind.NODE)
            .keyColumns(ImmutableList.of("nodeKey"))
            .autoBuild();

    GraphElementTable edgeTable =
        GraphElementTable.builder(builder.dialect())
            .name("edgeTable")
            .baseTableName("baseEdgeTable")
            .kind(GraphElementTable.Kind.EDGE)
            .keyColumns(ImmutableList.of("edgeKey1", "edgeKey2"))
            .sourceNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "nodeTable", ImmutableList.of("nodeKey"), ImmutableList.of("edgeKey1")))
            .targetNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "nodeTable", ImmutableList.of("nodeKey"), ImmutableList.of("edgeKey2")))
            .autoBuild();

    builder.addNodeTable(nodeTable);
    builder.addEdgeTable(edgeTable);
    builder.addPropertyDeclaration(new PropertyGraph.PropertyDeclaration("propertyA", "INT64"));
    builder.addLabel(
        new PropertyGraph.GraphElementLabel("nodeLabel", ImmutableList.of("propertyA")));

    PropertyGraph propertyGraph = builder.build();

    assertEquals("myGraph", propertyGraph.name());
    assertEquals(1, propertyGraph.nodeTables().size());
    assertEquals(nodeTable, propertyGraph.nodeTables().get(0));
    assertEquals(1, propertyGraph.edgeTables().size());
    assertEquals(edgeTable, propertyGraph.edgeTables().get(0));
  }

  @Test
  public void testToBuilder() {
    PropertyGraph propertyGraph =
        PropertyGraph.builder()
            .name("myGraph")
            .addNodeTable(
                GraphElementTable.builder()
                    .name("nodeTable")
                    .baseTableName("baseNodeTable")
                    .kind(GraphElementTable.Kind.NODE)
                    .keyColumns(ImmutableList.of("nodeKey"))
                    .autoBuild())
            .addPropertyDeclaration(new PropertyGraph.PropertyDeclaration("propertyA", "INT64"))
            .addLabel(
                new PropertyGraph.GraphElementLabel("nodeLabel", ImmutableList.of("propertyA")))
            .build();

    PropertyGraph.Builder builder = propertyGraph.toBuilder();
    PropertyGraph newPropertyGraph = builder.build();

    assertEquals(propertyGraph.name(), newPropertyGraph.name());
    assertEquals(propertyGraph.nodeTables(), newPropertyGraph.nodeTables());
    assertEquals(propertyGraph.edgeTables(), newPropertyGraph.edgeTables());
    assertEquals(propertyGraph.propertyDeclarations(), newPropertyGraph.propertyDeclarations());
    assertEquals(propertyGraph.labels(), newPropertyGraph.labels());
  }

  @Test
  public void testGettersWithEmptyLists() {
    PropertyGraph propertyGraph = PropertyGraph.builder().name("myGraph").build();

    assertNull(propertyGraph.getPropertyDeclaration("propertyA"));
    assertNull(propertyGraph.getLabel("nodeLabel"));
    assertNull(propertyGraph.getNodeTable("nodeTable"));
    assertNull(propertyGraph.getEdgeTable("edgeTable"));
  }
}
