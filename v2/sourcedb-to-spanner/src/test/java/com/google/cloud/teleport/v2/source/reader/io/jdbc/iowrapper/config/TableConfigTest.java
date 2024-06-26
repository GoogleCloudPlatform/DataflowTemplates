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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link TableConfig}. */
@RunWith(MockitoJUnitRunner.class)
public class TableConfigTest {
  @Test
  public void testTableConfigBuildsWithDefaults() {
    final String testTable = "testTable";
    final String partitionColumn = "col_1";

    TableConfig tableConfig =
        TableConfig.builder(testTable).withPartitionColum(partitionColumn).build();
    assertThat(tableConfig.tableName()).isEqualTo(testTable);
    assertThat(tableConfig.maxPartitions()).isNull();
    assertThat(tableConfig.partitionColumns()).isEqualTo(ImmutableList.of(partitionColumn));
  }

  @Test
  public void testTableConfigBuilds() {
    final String testTable = "testTable";
    final String partitionColumn = "col_1";
    final int maxPartitions = 100;

    TableConfig tableConfig =
        TableConfig.builder(testTable)
            .withPartitionColum(partitionColumn)
            .setMaxPartitions(maxPartitions)
            .build();
    assertThat(tableConfig.tableName()).isEqualTo(testTable);
    assertThat(tableConfig.maxPartitions()).isEqualTo(maxPartitions);
    assertThat(tableConfig.partitionColumns()).isEqualTo(ImmutableList.of(partitionColumn));
  }

  @Test
  public void testTableConfigPreconditions() {
    final String testTable = "testTable";

    assertThrows(IllegalStateException.class, () -> TableConfig.builder(testTable).build());
    assertThrows(
        IllegalStateException.class,
        () ->
            TableConfig.builder(testTable)
                .withPartitionColum("col_1")
                .withPartitionColum("col_2")
                .build());
  }
}
