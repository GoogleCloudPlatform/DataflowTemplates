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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.dofn.GeneratePrimaryKeyFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GeneratePrimaryKey} and its {@link GeneratePrimaryKeyFn}. */
@RunWith(JUnit4.class)
public class GeneratePrimaryKeyTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPrimaryKeyColumns_emptyPkListReturnsEmpty() {
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("T")
            .columns(ImmutableList.of(col("id", LogicalType.STRING)))
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    assertTrue(GeneratePrimaryKeyFn.primaryKeyColumns(table).isEmpty());
  }

  @Test
  public void testPrimaryKeyColumns_preservesDeclaredPkOrder() {
    // Declared column order (a, b) differs from PK order (b, a); helper must honour the PK
    // order so composite keys are emitted with the correct column ordering.
    DataGeneratorColumn a = col("a", LogicalType.STRING);
    DataGeneratorColumn b = col("b", LogicalType.INT64);
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("T")
            .columns(ImmutableList.of(a, b))
            .primaryKeys(ImmutableList.of("b", "a"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    List<DataGeneratorColumn> pkCols = GeneratePrimaryKeyFn.primaryKeyColumns(table);
    assertEquals(2, pkCols.size());
    assertEquals("b", pkCols.get(0).name());
    assertEquals("a", pkCols.get(1).name());
  }

  @Test
  public void testGeneratePrimaryKey_emitsCompositeKeyAndShardId() {
    DataGeneratorColumn id = col("id", LogicalType.STRING, 10L, null, null);
    DataGeneratorColumn seq = col("seq", LogicalType.INT64);
    DataGeneratorColumn payload = col("val", LogicalType.STRING); // non-PK
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("T")
            .columns(ImmutableList.of(id, seq, payload))
            .primaryKeys(ImmutableList.of("id", "seq"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    PCollection<KV<String, Row>> out =
        pipeline
            .apply("In", Create.of(table))
            .apply("GeneratePK", new GeneratePrimaryKey(null, null));

    PAssert.that(out)
        .satisfies(
            iter -> {
              KV<String, Row> kv = iter.iterator().next();
              assertEquals("T", kv.getKey());
              Row row = kv.getValue();
              // Expect PK columns in declared PK order + shard_id. val must not appear.
              assertEquals(3, row.getSchema().getFieldCount());
              assertEquals("id", row.getSchema().getField(0).getName());
              assertEquals("seq", row.getSchema().getField(1).getName());
              assertEquals(Constants.SHARD_ID_COLUMN_NAME, row.getSchema().getField(2).getName());

              assertNotNull(row.getString("id"));
              assertTrue(row.getString("id").length() > 0);
              assertNotNull(row.getInt64("seq"));
              assertNotNull(row.getString(Constants.SHARD_ID_COLUMN_NAME));
              // Default maxShards=1 → synthesised id "shard0".
              assertEquals("shard0", row.getString(Constants.SHARD_ID_COLUMN_NAME));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testGeneratePrimaryKey_shardIdWithinMaxShardsRange() {
    DataGeneratorColumn id = col("id", LogicalType.STRING);
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("T")
            .columns(ImmutableList.of(id))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    PCollection<KV<String, Row>> out =
        pipeline
            .apply("In", Create.of(table, table, table, table, table))
            .apply("GeneratePK", new GeneratePrimaryKey(null, null));

    PAssert.that(out)
        .satisfies(
            iter -> {
              for (KV<String, Row> kv : iter) {
                String shard = kv.getValue().getString(Constants.SHARD_ID_COLUMN_NAME);
                assertEquals("shard0", shard);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testGeneratePrimaryKey_tableWithoutPkEmitsNothing() {
    DataGeneratorColumn id = col("id", LogicalType.STRING);
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("T")
            .columns(ImmutableList.of(id))
            .primaryKeys(ImmutableList.of()) // no PK
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    PCollection<KV<String, Row>> out =
        pipeline
            .apply("In", Create.of(table))
            .apply("GeneratePK", new GeneratePrimaryKey(null, null));

    PAssert.that(out).empty();
    pipeline.run();
  }

  @Test
  public void testGeneratePrimaryKey_perTableSchemaIsolation() {
    // The fn caches PK Row schemas per table. Two tables with different PK column lists must
    // not bleed into each other's cached schema.
    DataGeneratorColumn id1 = col("id", LogicalType.STRING);
    DataGeneratorColumn id2a = col("a", LogicalType.INT64);
    DataGeneratorColumn id2b = col("b", LogicalType.STRING);

    DataGeneratorTable t1 =
        DataGeneratorTable.builder()
            .name("T1")
            .columns(ImmutableList.of(id1))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable t2 =
        DataGeneratorTable.builder()
            .name("T2")
            .columns(ImmutableList.of(id2a, id2b))
            .primaryKeys(ImmutableList.of("a", "b"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    PCollection<KV<String, Row>> out =
        pipeline
            .apply("In", Create.of(t1, t2, t1))
            .apply("GeneratePK", new GeneratePrimaryKey(null, null));

    PAssert.that(out)
        .satisfies(
            iter -> {
              int t1Rows = 0;
              int t2Rows = 0;
              for (KV<String, Row> kv : iter) {
                switch (kv.getKey()) {
                  case "T1":
                    t1Rows++;
                    // T1 schema: id + shard_id
                    assertEquals(2, kv.getValue().getSchema().getFieldCount());
                    assertEquals("id", kv.getValue().getSchema().getField(0).getName());
                    break;
                  case "T2":
                    t2Rows++;
                    // T2 schema: a, b + shard_id
                    assertEquals(3, kv.getValue().getSchema().getFieldCount());
                    assertEquals("a", kv.getValue().getSchema().getField(0).getName());
                    assertEquals("b", kv.getValue().getSchema().getField(1).getName());
                    break;
                  default:
                    throw new AssertionError("Unexpected table name: " + kv.getKey());
                }
              }
              assertEquals(2, t1Rows);
              assertEquals(1, t2Rows);
              return null;
            });

    pipeline.run();
  }

  private static DataGeneratorColumn col(String name, LogicalType type) {
    return col(name, type, null, null, null);
  }

  private static DataGeneratorColumn col(
      String name, LogicalType type, Long size, Integer precision, Integer scale) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isNullable(false)
        .isSkipped(false)
        .isGenerated(false)
        .size(size)
        .precision(precision)
        .scale(scale)
        .build();
  }
}
