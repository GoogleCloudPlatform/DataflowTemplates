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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaUtilsTest {

  @Test
  public void testDAGConstructionSimple() {
    // Parent -> Child
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(false) // Default false, should be set to true
            .childTables(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_parent")
                        .keyColumns(ImmutableList.of("parentId"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(true) // Should be set to false
            .childTables(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    DataGeneratorTable newParent = dagSchema.tables().get("Parent");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newParent.isRoot());
    assertFalse(newChild.isRoot());
    assertEquals(1, newParent.childTables().size());
    assertEquals("Child", newParent.childTables().get(0));
    assertEquals(0, newChild.childTables().size());
  }

  @Test
  public void testDAGConstructionMultiParentChain() {
    // P1 (10 QPS) -> P2 (100 QPS) -> Child
    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(100)
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(200)
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    DataGeneratorTable newP1 = dagSchema.tables().get("P1");
    DataGeneratorTable newP2 = dagSchema.tables().get("P2");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newP1.isRoot());
    assertFalse(newP2.isRoot());
    assertFalse(newChild.isRoot());

    // P1 should have P2
    assertEquals(1, newP1.childTables().size());
    assertEquals("P2", newP1.childTables().get(0));

    // P2 should have Child
    assertEquals(1, newP2.childTables().size());
    assertEquals("Child", newP2.childTables().get(0));

    assertEquals(0, newChild.childTables().size());
  }

  @Test
  public void testDAGConstructionInterleaving() {
    // InterleavedParent -> Child (interleaved)
    // OtherParent (1 QPS) -> Child (FK)

    DataGeneratorTable interleavedParent =
        DataGeneratorTable.builder()
            .name("InterleavedParent")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable otherParent =
        DataGeneratorTable.builder()
            .name("OtherParent")
            .insertQps(1)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .interleavedInTable("InterleavedParent")
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_other")
                        .keyColumns(ImmutableList.of("otherId"))
                        .referencedTable("OtherParent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "InterleavedParent",
                    interleavedParent,
                    "OtherParent",
                    otherParent,
                    "Child",
                    child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    assertFalse(dagSchema.tables().get("InterleavedParent").isRoot());
    assertTrue(dagSchema.tables().get("OtherParent").isRoot());
    assertFalse(dagSchema.tables().get("Child").isRoot());

    assertEquals(1, dagSchema.tables().get("OtherParent").childTables().size());
    assertEquals("InterleavedParent", dagSchema.tables().get("OtherParent").childTables().get(0));

    assertEquals(1, dagSchema.tables().get("InterleavedParent").childTables().size());
    assertEquals("Child", dagSchema.tables().get("InterleavedParent").childTables().get(0));
  }

  @Test
  public void testDAGConstructionGrandChild() {
    // P1 -> P2 -> C1 -> GC1
    // P2 -> C2
    // P1 (10), P2 (100), C1 (20), C2 (30), GC1 (40)
    // C1 now has parents P1 and P2

    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable c1 =
        DataGeneratorTable.builder()
            .name("C1")
            .insertQps(20)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2_c1")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable c2 =
        DataGeneratorTable.builder()
            .name("C2")
            .insertQps(30)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable gc1 =
        DataGeneratorTable.builder()
            .name("GC1")
            .insertQps(40)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_c1")
                        .keyColumns(ImmutableList.of("c1Id"))
                        .referencedTable("C1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "C1", c1, "C2", c2, "GC1", gc1))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    assertTrue(dagSchema.tables().get("P1").isRoot());
    assertFalse(dagSchema.tables().get("P2").isRoot()); // P2 is now a sequence child of P1 for C1
    assertFalse(dagSchema.tables().get("C1").isRoot());
    assertFalse(dagSchema.tables().get("C2").isRoot());
    assertFalse(dagSchema.tables().get("GC1").isRoot());

    assertEquals(1, dagSchema.tables().get("P1").childTables().size());
    assertEquals("P2", dagSchema.tables().get("P1").childTables().get(0)); // P1 -> P2 sequence

    assertEquals(2, dagSchema.tables().get("P2").childTables().size());
    assertTrue(dagSchema.tables().get("P2").childTables().contains("C1")); // P2 -> C1 sequence
    assertTrue(dagSchema.tables().get("P2").childTables().contains("C2")); // P2 -> C2 direct

    assertEquals(1, dagSchema.tables().get("C1").childTables().size());
    assertEquals("GC1", dagSchema.tables().get("C1").childTables().get(0));
    assertEquals(0, dagSchema.tables().get("C2").childTables().size());
    assertEquals(0, dagSchema.tables().get("GC1").childTables().size());
  }
}
