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

public class GraphElementTableTest {
  @Test
  public void testPropertyDefinition() {
    GraphElementTable.PropertyDefinition propertyDefinition =
        new GraphElementTable.PropertyDefinition("propertyA", "valueA");
    assertEquals("propertyA", propertyDefinition.name);
    assertEquals("valueA", propertyDefinition.valueExpressionString);
  }

  @Test
  public void testLabelToPropertyDefinitions() {
    ImmutableList<GraphElementTable.PropertyDefinition> propertyDefinitions =
        ImmutableList.of(
            new GraphElementTable.PropertyDefinition("propertyA", "valueA"),
            new GraphElementTable.PropertyDefinition("propertyB", "valueB"));
    GraphElementTable.LabelToPropertyDefinitions labelToPropertyDefinitions =
        new GraphElementTable.LabelToPropertyDefinitions("label1", propertyDefinitions);

    assertEquals("label1", labelToPropertyDefinitions.labelName);
    assertEquals(propertyDefinitions, labelToPropertyDefinitions.propertyDefinitions());

    assertEquals(
        propertyDefinitions.get(0), labelToPropertyDefinitions.getPropertyDefinition("propertyA"));
    assertNull(labelToPropertyDefinitions.getPropertyDefinition("propertyC"));

    String expectedPrettyPrint =
        "LABEL label1 PROPERTIES(valueA AS propertyA, valueB AS propertyB)";
    assertEquals(expectedPrettyPrint, labelToPropertyDefinitions.prettyPrint());

    // Test prettyPrint with aliased properties
    propertyDefinitions =
        ImmutableList.of(
            new GraphElementTable.PropertyDefinition("propertyA", "propertyA"),
            new GraphElementTable.PropertyDefinition("propertyB", "valueB"));
    labelToPropertyDefinitions =
        new GraphElementTable.LabelToPropertyDefinitions("label1", propertyDefinitions);
    expectedPrettyPrint = "LABEL label1 PROPERTIES(propertyA, valueB AS propertyB)";
    assertEquals(expectedPrettyPrint, labelToPropertyDefinitions.prettyPrint());

    // Test prettyPrint with no properties
    labelToPropertyDefinitions =
        new GraphElementTable.LabelToPropertyDefinitions("label1", ImmutableList.of());
    expectedPrettyPrint = "LABEL label1 NO PROPERTIES";
    assertEquals(expectedPrettyPrint, labelToPropertyDefinitions.prettyPrint());
  }

  @Test
  public void testGraphNodeTableReference() {
    GraphElementTable.GraphNodeTableReference graphNodeTableReference =
        new GraphElementTable.GraphNodeTableReference(
            "nodeTable", ImmutableList.of("nodeKey1", "nodeKey2"), ImmutableList.of("edgeKey"));

    assertEquals("nodeTable", graphNodeTableReference.nodeTableName);
    assertEquals(ImmutableList.of("nodeKey1", "nodeKey2"), graphNodeTableReference.nodeKeyColumns);
    assertEquals(ImmutableList.of("edgeKey"), graphNodeTableReference.edgeKeyColumns);

    String expectedPrettyPrint = "KEY(edgeKey) REFERENCES nodeTable";
    assertEquals(expectedPrettyPrint, graphNodeTableReference.prettyPrint());
  }

  @Test
  public void testGraphElementTable() {
    GraphElementTable graphElementTable =
        GraphElementTable.builder()
            .name("edgeTable")
            .baseTableName("baseEdgeTable")
            .kind(GraphElementTable.Kind.EDGE)
            .keyColumns(ImmutableList.of("edgeKey1", "edgeKey2"))
            .labelToPropertyDefinitions(
                ImmutableList.of(
                    new GraphElementTable.LabelToPropertyDefinitions(
                        "label1",
                        ImmutableList.of(
                            new GraphElementTable.PropertyDefinition("propertyA", "valueA")))))
            .sourceNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "sourceNodeTable",
                    ImmutableList.of("sourceNodeKey"),
                    ImmutableList.of("edgeKey1")))
            .targetNodeTable(
                new GraphElementTable.GraphNodeTableReference(
                    "targetNodeTable",
                    ImmutableList.of("targetNodeKey"),
                    ImmutableList.of("edgeKey2")))
            .autoBuild();

    assertEquals("edgeTable", graphElementTable.name());
    assertEquals("baseEdgeTable", graphElementTable.baseTableName());
    assertEquals(GraphElementTable.Kind.EDGE, graphElementTable.kind());
    assertEquals(Dialect.GOOGLE_STANDARD_SQL, graphElementTable.dialect());
    assertEquals(ImmutableList.of("edgeKey1", "edgeKey2"), graphElementTable.keyColumns());

    String expectedPrettyPrint =
        "baseEdgeTable AS edgeTable\n"
            + " KEY (edgeKey1, edgeKey2)\n"
            + "SOURCE KEY(edgeKey1) REFERENCES sourceNodeTable DESTINATION KEY(edgeKey2) REFERENCES targetNodeTable\n"
            + "LABEL label1 PROPERTIES(valueA AS propertyA)";
    assertEquals(expectedPrettyPrint, graphElementTable.prettyPrint());

    // Test getLabelToPropertyDefinitions
    GraphElementTable.LabelToPropertyDefinitions labelToPropertyDefinitions =
        graphElementTable.getLabelToPropertyDefinitions("label1");
    assertEquals("label1", labelToPropertyDefinitions.labelName);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrettyPrintWithInvalidDialect() {
    GraphElementTable graphElementTable =
        GraphElementTable.builder(Dialect.POSTGRESQL).baseTableName("baseEdgeTable").autoBuild();
    graphElementTable.prettyPrint();
  }
}
