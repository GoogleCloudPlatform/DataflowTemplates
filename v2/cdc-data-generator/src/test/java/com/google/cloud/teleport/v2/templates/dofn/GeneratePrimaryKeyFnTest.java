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
package com.google.cloud.teleport.v2.templates.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SqlServerSinkConfig;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Unit tests for {@link GeneratePrimaryKeyFn}. */
@RunWith(JUnit4.class)
public class GeneratePrimaryKeyFnTest {

  @Test
  public void testPrimaryKeyColumns() {
    DataGeneratorColumn col1 =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .build();

    DataGeneratorColumn col2 =
        DataGeneratorColumn.builder()
            .name("name")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of(col1, col2))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .build();

    assertEquals(1, GeneratePrimaryKeyFn.primaryKeyColumns(table).size());
    assertEquals("id", GeneratePrimaryKeyFn.primaryKeyColumns(table).get(0).name());
  }

  @Test
  public void testCustomGeneratorExceptionCatching() {
    DataGeneratorColumn col1 =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .build();

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of(col1))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .build();

    GeneratePrimaryKeyFn fn =
        new GeneratePrimaryKeyFn(null, "SPANNER", "dummy.jar", BadCustomGenerator.class.getName());
    fn.setup();

    @SuppressWarnings("unchecked")
    DoFn.OutputReceiver<KV<String, Row>> receiver =
        (DoFn.OutputReceiver<KV<String, Row>>) Mockito.mock(DoFn.OutputReceiver.class);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> fn.processElement(table, receiver));

    assertTrue(exception.getMessage().contains("Failed to assemble root primary key for table"));
  }

  @Test
  public void testShardIdPickedFromSqlServerSinkConfig() {
    DataGeneratorColumn col1 =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of(col1))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .build();

    SqlServerSinkConfig config =
        new SqlServerSinkConfig(
            ImmutableList.of(
                new Shard("mssqlA", "h", "1433", "u", "p", "dbA", null, null, null),
                new Shard("mssqlB", "h", "1433", "u", "p", "dbB", null, null, null)));
    GeneratePrimaryKeyFn fn = new GeneratePrimaryKeyFn(config, "SQLSERVER", null, null);
    fn.setup();

    @SuppressWarnings("unchecked")
    DoFn.OutputReceiver<KV<String, Row>> receiver =
        (DoFn.OutputReceiver<KV<String, Row>>) Mockito.mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<KV<String, Row>> captor = ArgumentCaptor.forClass(KV.class);

    for (int i = 0; i < 20; i++) {
      fn.processElement(table, receiver);
    }
    Mockito.verify(receiver, Mockito.times(20)).output(captor.capture());
    Set<String> seenShards = new HashSet<>();
    for (KV<String, Row> kv : captor.getAllValues()) {
      String shardId = kv.getValue().getString(Constants.SHARD_ID_COLUMN_NAME);
      assertTrue(ImmutableSet.of("mssqlA", "mssqlB").contains(shardId));
      seenShards.add(shardId);
    }
    // With 20 draws it is overwhelmingly likely that both shards are selected at least once.
    assertEquals(2, seenShards.size());
  }

  public static class BadCustomGenerator implements CustomDataGenerator {
    @Override
    public Object generate(String tableName, String columnName) {
      return "not a long";
    }
  }
}
