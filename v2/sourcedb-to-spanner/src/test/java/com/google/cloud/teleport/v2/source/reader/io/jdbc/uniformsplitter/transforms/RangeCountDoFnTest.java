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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeCountDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeCountDoFnTest {
  SerializableFunction<Void, DataSource> mockDataSourceProviderFn =
      Mockito.mock(SerializableFunction.class, withSettings().serializable());
  DataSource mockDataSource = Mockito.mock(DataSource.class, withSettings().serializable());

  Connection mockConnection = Mockito.mock(Connection.class, withSettings().serializable());

  @Mock PreparedStatement mockPreparedStatemet;

  @Mock ResultSet mockResultSet;

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<Range> rangeCaptor;
  @Mock DoFn.ProcessContext mockProcessContext;

  @Test
  public void testRangeCountDoFnBasic() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(42L);
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()));
    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    rangeCountDoFn.setup();
    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue()).isEqualTo(input.withCount(42L, mockProcessContext));
  }

  @Test
  public void testRangeCountDoFnTimeoutException() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery())
        .thenThrow(new SQLTimeoutException("test"))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1))
        .thenThrow(new SQLTimeoutException())
        .thenThrow(
            new SQLException(
                "Query execution was interrupted, maximum statement execution time exceeded"));
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()));
    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    rangeCountDoFn.setup();
    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);
    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut, times(2)).output(rangeCaptor.capture());
    ImmutableList<Boolean> outputRangesAreUncounted =
        rangeCaptor.getAllValues().stream()
            .map(range -> range.isUncounted())
            .collect(ImmutableList.toImmutableList());
    assertThat(outputRangesAreUncounted).isEqualTo(ImmutableList.of(true, true));
  }

  @Test
  public void testRangeCountDoFnOtherException() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery()).thenThrow(new SQLException("test"));
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()));
    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    rangeCountDoFn.setup();
    assertThrows(
        SQLException.class,
        () -> rangeCountDoFn.processElement(input, mockOut, mockProcessContext));
  }

  @Test
  public void testRangeCountDoFnUnexprectedResultSet() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false /* Empty ResultSet */).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(0L);
    when(mockResultSet.wasNull()).thenReturn(true) /* Null ResultSet */;
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()));
    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    rangeCountDoFn.setup();
    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);
    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);
    verify(mockOut, times(2)).output(rangeCaptor.capture());
    // The Range remains uncounted with logs.
    assertThat(rangeCaptor.getAllValues()).isEqualTo(ImmutableList.of(input, input));
  }

  @Test
  public void testRangeCountDoFnMissingTable() throws Exception {
    TableSplitSpecification tableSpec1 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("existingTable").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(tableSpec1));

    Range inputMissingTable =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("missingTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    rangeCountDoFn.setup();
    assertThrows(
        RuntimeException.class,
        () -> rangeCountDoFn.processElement(inputMissingTable, mockOut, mockProcessContext));
  }

  @Test
  public void testIsValidRangeHelperFunction() {
    TableIdentifier existingTable = TableIdentifier.builder().setTableName("existingTable").build();
    TableIdentifier missingTable = TableIdentifier.builder().setTableName("missingTable").build();

    ImmutableMap<TableIdentifier, String> countQueries =
        ImmutableMap.of(existingTable, "SELECT COUNT(*) FROM existingTable");

    Range inputExistingTable =
        Range.<Integer>builder()
            .setTableIdentifier(existingTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    Range inputMissingFromCountQueries =
        Range.<Integer>builder()
            .setTableIdentifier(missingTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    Range inputMissingFromNumColumnsMap =
        Range.<Integer>builder()
            .setTableIdentifier(missingTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    // Case 1: TableIdentifier present in both maps
    assertThat(RangeCountDoFn.isInvalidValidRange(inputExistingTable, countQueries)).isFalse();

    // Case 2: TableIdentifier missing from countQueries
    assertThat(RangeCountDoFn.isInvalidValidRange(inputMissingFromCountQueries, countQueries))
        .isTrue();
  }

  @Test
  public void testRangeCountDoFnMultiTable() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    PreparedStatement mockPreparedStatement1 = Mockito.mock(PreparedStatement.class);
    PreparedStatement mockPreparedStatement2 = Mockito.mock(PreparedStatement.class);
    ResultSet mockResultSet1 = Mockito.mock(ResultSet.class);
    ResultSet mockResultSet2 = Mockito.mock(ResultSet.class);

    doNothing().when(mockPreparedStatement1).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatement1).setObject(anyInt(), any());
    when(mockPreparedStatement1.executeQuery()).thenReturn(mockResultSet1);
    when(mockResultSet1.next()).thenReturn(true);
    when(mockResultSet1.getLong(1)).thenReturn(42L);

    doNothing().when(mockPreparedStatement2).setQueryTimeout(anyInt());
    doNothing().when(mockPreparedStatement2).setObject(anyInt(), any());
    when(mockPreparedStatement2.executeQuery()).thenReturn(mockResultSet2);
    when(mockResultSet2.next()).thenReturn(true);
    when(mockResultSet2.getLong(1)).thenReturn(84L);

    TableSplitSpecification tableSpec1 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable1").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();
    TableSplitSpecification tableSpec2 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable2").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Long.class)
                        .build()))
            .setApproxRowCount(200L)
            .setMaxPartitionsHint(20L)
            .setInitialSplitHeight(10L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            mockDataSourceProviderFn,
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(tableSpec1, tableSpec2));

    // Use reflection to get the dbAdapter from rangeCountDoFn
    java.lang.reflect.Field dbAdapterField = RangeCountDoFn.class.getDeclaredField("dbAdapter");
    dbAdapterField.setAccessible(true);
    UniformSplitterDBAdapter dbAdapter =
        (UniformSplitterDBAdapter) dbAdapterField.get(rangeCountDoFn);

    String countQuery1 =
        dbAdapter.getCountQuery(
            tableSpec1.tableIdentifier().tableName(),
            tableSpec1.partitionColumns().stream()
                .map(pc -> pc.columnName())
                .collect(ImmutableList.toImmutableList()),
            2000L);
    String countQuery2 =
        dbAdapter.getCountQuery(
            tableSpec2.tableIdentifier().tableName(),
            tableSpec2.partitionColumns().stream()
                .map(pc -> pc.columnName())
                .collect(ImmutableList.toImmutableList()),
            2000L);

    when(mockConnection.prepareStatement(
            countQuery1, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(mockPreparedStatement1);
    when(mockConnection.prepareStatement(
            countQuery2, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(mockPreparedStatement2);

    Range input1 =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable1").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    Range input2 =
        Range.<Integer>builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable2").build())
            .setColName("col2")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(200)
            .build();

    rangeCountDoFn.setup();
    rangeCountDoFn.processElement(input1, mockOut, mockProcessContext);
    rangeCountDoFn.processElement(input2, mockOut, mockProcessContext);

    verify(mockOut, times(2)).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getAllValues().get(0))
        .isEqualTo(input1.withCount(42L, mockProcessContext));
    assertThat(rangeCaptor.getAllValues().get(1))
        .isEqualTo(input2.withCount(84L, mockProcessContext));
  }
}
