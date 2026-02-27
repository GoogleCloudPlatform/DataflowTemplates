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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import java.sql.PreparedStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ColumnForBoundaryQueryPreparedStatementSetter}. */
@RunWith(MockitoJUnitRunner.class)
public class ColumnForBoundaryQueryPreparedStatementSetterMockTest {

  @Test
  public void testSetParameters_withNullParentRange() throws Exception {

    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("testTable").build();
    TableSplitSpecification tableSplitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(testTableIdentifier)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(1L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();

    ColumnForBoundaryQueryPreparedStatementSetter setter =
        new ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList.of(tableSplitSpec));
    PreparedStatement mockStatement = mock(PreparedStatement.class);

    ColumnForBoundaryQuery initialColumn =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .setParentRange(null) // explicitly null parent range
            .build();

    setter.setParameters(initialColumn, mockStatement);

    // Verify that the statement is correctly padded with nulls/false flags.
    verify(mockStatement, times(1)).setObject(1, false);
    verify(mockStatement, times(1)).setObject(2, null);
    verify(mockStatement, times(1)).setObject(3, null);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, null);
  }

  @Test
  public void testSetParameters_withSingleLevelParentRange() throws Exception {
    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("testTable").build();
    TableSplitSpecification tableSplitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(testTableIdentifier)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(1L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();
    ColumnForBoundaryQueryPreparedStatementSetter setter =
        new ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList.of(tableSplitSpec));
    PreparedStatement mockStatement = mock(PreparedStatement.class);

    // Directly mock Range and stub only relevant public methods
    Range mockParentRange = mock(Range.class);
    when(mockParentRange.start()).thenReturn(10);
    when(mockParentRange.end()).thenReturn(20);
    when(mockParentRange.isLast()).thenReturn(false);
    when(mockParentRange.childRange()).thenReturn(null);
    when(mockParentRange.colName()).thenReturn("col1");

    ColumnForBoundaryQuery columnWithinRange =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(mockParentRange)
            .build();

    setter.setParameters(columnWithinRange, mockStatement);

    verify(mockStatement, times(1)).setObject(1, true);
    verify(mockStatement, times(1)).setObject(2, 10);
    verify(mockStatement, times(1)).setObject(3, 20);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, 20);

    // Verify padding for the second column
    verify(mockStatement, times(1)).setObject(6, false);
    verify(mockStatement, times(1)).setObject(7, null);
    verify(mockStatement, times(1)).setObject(8, null);
    verify(mockStatement, times(1)).setObject(9, false);
    verify(mockStatement, times(1)).setObject(10, null);
  }

  @Test
  public void testSetParameters_withMultiLevelParentRange() throws Exception {
    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("testTable").build();
    TableSplitSpecification tableSplitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(testTableIdentifier)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col3")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(1L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();
    ColumnForBoundaryQueryPreparedStatementSetter setter =
        new ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList.of(tableSplitSpec));
    PreparedStatement mockStatement = mock(PreparedStatement.class);

    // Directly mock child Range
    Range mockChildRange = mock(Range.class);
    when(mockChildRange.start()).thenReturn(100);
    when(mockChildRange.end()).thenReturn(200);
    when(mockChildRange.isLast()).thenReturn(true);
    when(mockChildRange.childRange()).thenReturn(null);
    when(mockChildRange.colName()).thenReturn("col2");

    // Directly mock parent Range, making its child the mocked child Range
    Range mockParentRange = mock(Range.class);
    when(mockParentRange.start()).thenReturn(10);
    when(mockParentRange.end()).thenReturn(50);
    when(mockParentRange.isLast()).thenReturn(false);
    when(mockParentRange.childRange()).thenReturn(mockChildRange);
    when(mockParentRange.colName()).thenReturn("col1");

    ColumnForBoundaryQuery columnWithinRange =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col3")
            .setColumnClass(Integer.class)
            .setParentRange(mockParentRange)
            .build();

    setter.setParameters(columnWithinRange, mockStatement);

    verify(mockStatement, times(1)).setObject(1, true);
    verify(mockStatement, times(1)).setObject(2, 10);
    verify(mockStatement, times(1)).setObject(3, 50);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, 50);

    verify(mockStatement, times(1)).setObject(6, true);
    verify(mockStatement, times(1)).setObject(7, 100);
    verify(mockStatement, times(1)).setObject(8, 200);
    verify(mockStatement, times(1)).setObject(9, true);
    verify(mockStatement, times(1)).setObject(10, 200);

    // Verify padding for the third column
    verify(mockStatement, times(1)).setObject(11, false);
    verify(mockStatement, times(1)).setObject(12, null);
    verify(mockStatement, times(1)).setObject(13, null);
    verify(mockStatement, times(1)).setObject(14, false);
    verify(mockStatement, times(1)).setObject(15, null);
  }
}
