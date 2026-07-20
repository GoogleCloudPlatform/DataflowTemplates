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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
}
