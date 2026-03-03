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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link TableSplitSpecification}. */
@RunWith(JUnit4.class)
public class TableSplitSpecificationTest {

  @Test
  public void testTableSplitSpecificationAutoDerivation() {
    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName("test_table").build();
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("id").setColumnClass(Long.class).build();
    ImmutableList<PartitionColumn> partitionColumns = ImmutableList.of(partitionColumn);
    long approxRowCount = 1000000L;

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableIdentifier)
            .setPartitionColumns(partitionColumns)
            .setApproxRowCount(approxRowCount)
            .build();

    // Derived maxPartitionsHint: Math.max(1, round(floor(sqrt(1000000) / 20))) = round(floor(1000 /
    // 20)) = 50
    assertThat(spec.maxPartitionsHint()).isEqualTo(50L);

    // Derived initialSplitHeight: logToBaseTwo(50 * 1) = logToBaseTwo(50) = 6 (since 2^5 < 50 <=
    // 2^6)
    assertThat(spec.initialSplitHeight()).isEqualTo(6L);

    // Derived splitStagesCount: logToBaseTwo(50) + partitionColumns.size() + 1 = 6 + 1 + 1 = 8
    assertThat(spec.splitStagesCount()).isEqualTo(8L);
  }

  @Test
  public void testTableSplitSpecificationExplicitValues() {
    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName("test_table").build();
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("id").setColumnClass(Long.class).build();
    ImmutableList<PartitionColumn> partitionColumns = ImmutableList.of(partitionColumn);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableIdentifier)
            .setPartitionColumns(partitionColumns)
            .setApproxRowCount(1000000L)
            .setMaxPartitionsHint(100L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(10L)
            .build();

    assertThat(spec.maxPartitionsHint()).isEqualTo(100L);
    assertThat(spec.initialSplitHeight()).isEqualTo(5L);
    assertThat(spec.splitStagesCount()).isEqualTo(10L);
  }

  @Test
  public void testInferMaxPartitions() {
    assertThat(TableSplitSpecification.inferMaxPartitions(0L)).isEqualTo(1L);
    assertThat(TableSplitSpecification.inferMaxPartitions(100L))
        .isEqualTo(1L); // sqrt(100)/20 = 0.5 -> 0 -> max(1,0) = 1
    assertThat(TableSplitSpecification.inferMaxPartitions(400L))
        .isEqualTo(1L); // sqrt(400)/20 = 1 -> 1
    assertThat(TableSplitSpecification.inferMaxPartitions(1000000L))
        .isEqualTo(50L); // sqrt(1M)/20 = 1000/20 = 50
  }

  @Test
  public void testLogToBaseTwo() {
    assertThat(TableSplitSpecification.logToBaseTwo(0L)).isEqualTo(0L);
    assertThat(TableSplitSpecification.logToBaseTwo(1L)).isEqualTo(0L);
    assertThat(TableSplitSpecification.logToBaseTwo(2L)).isEqualTo(1L);
    assertThat(TableSplitSpecification.logToBaseTwo(3L)).isEqualTo(2L);
    assertThat(TableSplitSpecification.logToBaseTwo(4L)).isEqualTo(2L);
    assertThat(TableSplitSpecification.logToBaseTwo(5L)).isEqualTo(3L);
    assertThat(TableSplitSpecification.logToBaseTwo(63L)).isEqualTo(6L);
    assertThat(TableSplitSpecification.logToBaseTwo(64L)).isEqualTo(6L);
    assertThat(TableSplitSpecification.logToBaseTwo(65L)).isEqualTo(7L);
  }

  @Test
  public void testTableSplitSpecificationAutoDerivationWithZeroApproxRowCount() {
    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName("test_table").build();
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("id").setColumnClass(Long.class).build();
    ImmutableList<PartitionColumn> partitionColumns = ImmutableList.of(partitionColumn);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableIdentifier)
            .setPartitionColumns(partitionColumns)
            .setApproxRowCount(0L)
            .build();

    // Derived maxPartitionsHint: Math.max(1, round(floor(sqrt(0) / 20))) = 1
    assertThat(spec.maxPartitionsHint()).isEqualTo(1L);

    // Derived initialSplitHeight: logToBaseTwo(1 * 1) = 0
    assertThat(spec.initialSplitHeight()).isEqualTo(0L);

    // Derived splitStagesCount: logToBaseTwo(1) + partitionColumns.size() + 1 = 0 + 1 + 1 = 2
    assertThat(spec.splitStagesCount()).isEqualTo(2L);
  }
}
