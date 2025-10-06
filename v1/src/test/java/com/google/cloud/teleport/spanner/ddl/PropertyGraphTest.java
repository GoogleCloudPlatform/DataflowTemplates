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
            + "SOURCE KEY(edgeKey1) REFERENCES nodeTable(nodeKey) DESTINATION KEY(edgeKey2) REFERENCES nodeTable(nodeKey)\n"
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

  @Test
  public void testPropertyGraphOnView() {
    Ddl ddl =
        Ddl.builder()
            .createTable("GraphTableAccount")
            .column("loc_id")
            .int64()
            .notNull()
            .endColumn()
            .column("aid")
            .int64()
            .notNull()
            .endColumn()
            .column("owner_id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("loc_id")
            .asc("aid")
            .end()
            .endTable()
            .createTable("GraphTablePerson")
            .column("loc_id")
            .int64()
            .notNull()
            .endColumn()
            .column("pid")
            .int64()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("loc_id")
            .asc("pid")
            .end()
            .endTable()
            .createView("V_FilteredPerson")
            .query("SELECT t.loc_id, t.pid FROM GraphTablePerson AS t WHERE t.loc_id = 1")
            .security(View.SqlSecurity.INVOKER)
            .endView()
            .createView("V_GraphTableAccount")
            .query("SELECT t.loc_id, t.aid, t.owner_id FROM GraphTableAccount AS t")
            .security(View.SqlSecurity.INVOKER)
            .endView()
            .createView("V_GroupByPerson")
            .query(
                "SELECT t.loc_id, t.pid, COUNT(*) AS cnt FROM GraphTablePerson AS t GROUP BY"
                    + " t.loc_id, t.pid ORDER BY cnt DESC")
            .security(View.SqlSecurity.INVOKER)
            .endView()
            .createPropertyGraph("aml_view_complex")
            .addNodeTable(
                GraphElementTable.builder()
                    .name("V_FilteredPerson")
                    .baseTableName("V_FilteredPerson")
                    .kind(GraphElementTable.Kind.NODE)
                    .keyColumns(ImmutableList.of("loc_id", "pid"))
                    .autoBuild())
            .addNodeTable(
                GraphElementTable.builder()
                    .name("V_GraphTableAccount")
                    .baseTableName("V_GraphTableAccount")
                    .kind(GraphElementTable.Kind.NODE)
                    .keyColumns(ImmutableList.of("loc_id", "aid"))
                    .autoBuild())
            .addNodeTable(
                GraphElementTable.builder()
                    .name("V_GroupByPerson")
                    .baseTableName("V_GroupByPerson")
                    .kind(GraphElementTable.Kind.NODE)
                    .keyColumns(ImmutableList.of("loc_id", "pid"))
                    .autoBuild())
            .addEdgeTable(
                GraphElementTable.builder()
                    .name("Owns")
                    .baseTableName("V_GraphTableAccount")
                    .kind(GraphElementTable.Kind.EDGE)
                    .keyColumns(ImmutableList.of("loc_id", "aid"))
                    .sourceNodeTable(
                        new GraphElementTable.GraphNodeTableReference(
                            "V_FilteredPerson",
                            ImmutableList.of("loc_id", "pid"),
                            ImmutableList.of("loc_id", "owner_id")))
                    .targetNodeTable(
                        new GraphElementTable.GraphNodeTableReference(
                            "V_GraphTableAccount",
                            ImmutableList.of("loc_id", "aid"),
                            ImmutableList.of("loc_id", "aid")))
                    .autoBuild())
            .endPropertyGraph()
            .build();
    String expected =
        "CREATE TABLE `GraphTableAccount` (\n"
            + "\t`loc_id`                                INT64 NOT NULL,\n"
            + "\t`aid`                                   INT64 NOT NULL,\n"
            + "\t`owner_id`                              INT64,\n"
            + ") PRIMARY KEY (`loc_id` ASC, `aid` ASC)\n"
            + "\n\n"
            + "CREATE TABLE `GraphTablePerson` (\n"
            + "\t`loc_id`                                INT64 NOT NULL,\n"
            + "\t`pid`                                   INT64 NOT NULL,\n"
            + ") PRIMARY KEY (`loc_id` ASC, `pid` ASC)\n"
            + "\n\n"
            + "CREATE VIEW `V_FilteredPerson` SQL SECURITY INVOKER AS SELECT t.loc_id, t.pid FROM"
            + " GraphTablePerson AS t WHERE t.loc_id = 1\n"
            + "CREATE VIEW `V_GraphTableAccount` SQL SECURITY INVOKER AS SELECT t.loc_id, t.aid, t.owner_id FROM GraphTableAccount AS t\n"
            + "CREATE VIEW `V_GroupByPerson` SQL SECURITY INVOKER AS SELECT t.loc_id, t.pid,"
            + " COUNT(*) AS cnt FROM GraphTablePerson AS t GROUP BY t.loc_id, t.pid ORDER BY"
            + " cnt DESC\n"
            + "CREATE PROPERTY GRAPH aml_view_complex\n"
            + "NODE TABLES(\n"
            + "V_FilteredPerson AS V_FilteredPerson\n"
            + " KEY (loc_id, pid)\n"
            + ", V_GraphTableAccount AS V_GraphTableAccount\n"
            + " KEY (loc_id, aid)\n"
            + ", V_GroupByPerson AS V_GroupByPerson\n"
            + " KEY (loc_id, pid)\n"
            + ")\n"
            + "EDGE TABLES(\n"
            + "V_GraphTableAccount AS Owns\n"
            + " KEY (loc_id, aid)\n"
            + "SOURCE KEY(loc_id, owner_id) REFERENCES V_FilteredPerson(loc_id,pid) DESTINATION"
            + " KEY(loc_id, aid) REFERENCES V_GraphTableAccount(loc_id,aid)\n"
            + ")";
    assertEquals(expected, ddl.prettyPrint());
  }
}
