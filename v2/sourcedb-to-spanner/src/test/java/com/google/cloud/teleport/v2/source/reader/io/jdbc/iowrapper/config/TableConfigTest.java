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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
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
    final PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col_1").setColumnClass(Integer.class).build();

    TableConfig tableConfig =
        TableConfig.builder(testTable)
            .withPartitionColum(partitionColumn)
            .setApproxRowCount(42L)
            .build();
    assertThat(tableConfig.tableName()).isEqualTo(testTable);
    assertThat(tableConfig.maxPartitions()).isNull();
    assertThat(tableConfig.partitionColumns()).isEqualTo(ImmutableList.of(partitionColumn));
    assertThat(tableConfig.approxRowCount()).isEqualTo(42L);
  }

  @Test
  public void testTableConfigBuilds() {
    final String testTable = "testTable";
    final PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col_1").setColumnClass(Integer.class).build();
    final int maxPartitions = 100;

    TableConfig tableConfig =
        TableConfig.builder(testTable)
            .withPartitionColum(partitionColumn)
            .setMaxPartitions(maxPartitions)
            .setApproxRowCount(42L)
            .build();
    assertThat(tableConfig.tableName()).isEqualTo(testTable);
    assertThat(tableConfig.maxPartitions()).isEqualTo(maxPartitions);
    assertThat(tableConfig.partitionColumns()).isEqualTo(ImmutableList.of(partitionColumn));
    assertThat(tableConfig.approxRowCount()).isEqualTo(42L);
  }
}
