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

import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
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

  public static class BadCustomGenerator implements CustomDataGenerator {
    @Override
    public Object generate(String tableName, String columnName) {
      return "not a long";
    }
  }
}
