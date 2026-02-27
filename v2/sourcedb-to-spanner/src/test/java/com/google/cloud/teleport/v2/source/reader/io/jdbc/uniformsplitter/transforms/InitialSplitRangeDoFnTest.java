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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link InitialSplitRangeDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class InitialSplitRangeDoFnTest {
  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<ImmutableList<Range>> rangeCaptor;
  @Mock ProcessContext mockProcessContext;

  private TableSplitSpecification getTableSplitSpecification(String tableName, long splitHeight) {
    return TableSplitSpecification.builder()
        .setTableIdentifier(TableIdentifier.builder().setTableName(tableName).build())
        .setPartitionColumns(
            ImmutableList.of(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build()))
        .setApproxRowCount(100L)
        .setMaxPartitionsHint(10L)
        .setInitialSplitHeight(splitHeight)
        .setSplitStagesCount(1L)
        .build();
  }

  private Range getRange(String tableName, long start, long end) {
    return Range.builder()
        .setTableIdentifier(TableIdentifier.builder().setTableName(tableName).build())
        .setColName("col1")
        .setStart(start)
        .setEnd(end)
        .setColClass(Long.class)
        .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
        .setIsFirst(true)
        .setIsLast(true)
        .build();
  }

  @Test
  public void testInitialSplitRangeDoFn_withSingleTable_splitsCorrectly() {

    InitialSplitRangeDoFn initialSplitRangeDoFn =
        InitialSplitRangeDoFn.builder()
            .setTableSplitSpecification(getTableSplitSpecification("testTable", 5))
            .build();
    Range range = getRange("testTable", 0, 5);
    initialSplitRangeDoFn.processElement(range, mockOut, mockProcessContext);
    verify(mockOut, times(1)).output(rangeCaptor.capture());
    // We expect these to be 0-1, 1-2, 2-3, 3-4, 4-5
    ImmutableList<Range> actualRanges = rangeCaptor.getValue();
    assertThat(actualRanges.size()).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      assertThat(actualRanges.get(i).start()).isEqualTo(i);
      assertThat(actualRanges.get(i).end()).isEqualTo(i + 1);
      if (i == 0) {
        assertThat(actualRanges.get(i).isFirst()).isTrue();
      }
      if (i == 4) {
        assertThat(actualRanges.get(i).isLast()).isTrue();
      }
    }
  }

  @Test
  public void testInitialSplitRangeDoFn_withMultipleTables_splitsCorrectly() {
    InitialSplitRangeDoFn initialSplitRangeDoFn =
        InitialSplitRangeDoFn.builder()
            .setTableSplitSpecifications(
                ImmutableList.of(
                    getTableSplitSpecification("table1", 2L),
                    getTableSplitSpecification("table2", 3L)))
            .build();

    // Test table1
    initialSplitRangeDoFn.processElement(getRange("table1", 0L, 4L), mockOut, mockProcessContext);
    verify(mockOut, times(1)).output(rangeCaptor.capture());
    ImmutableList<Range> table1Ranges = rangeCaptor.getValue();
    assertThat(table1Ranges.size()).isEqualTo(4);
    assertThat(table1Ranges.get(0).tableIdentifier().tableName()).isEqualTo("table1");

    // Test table2
    initialSplitRangeDoFn.processElement(getRange("table2", 0L, 8L), mockOut, mockProcessContext);
    verify(mockOut, times(2)).output(rangeCaptor.capture());
    ImmutableList<Range> table2Ranges = rangeCaptor.getValue();
    assertThat(table2Ranges.size()).isEqualTo(8);
    assertThat(table2Ranges.get(0).tableIdentifier().tableName()).isEqualTo("table2");
  }

  @Test
  public void testInitialSplitRangeDoFn_withUnknownTable_throwsException() {
    InitialSplitRangeDoFn initialSplitRangeDoFn =
        InitialSplitRangeDoFn.builder()
            .setTableSplitSpecification(getTableSplitSpecification("testTable", 2L))
            .build();
    Range rangeForUnknownTable = getRange("unknownTable", 0L, 4L);

    assertThrows(
        RuntimeException.class,
        () ->
            initialSplitRangeDoFn.processElement(
                rangeForUnknownTable, mockOut, mockProcessContext));
  }
}
