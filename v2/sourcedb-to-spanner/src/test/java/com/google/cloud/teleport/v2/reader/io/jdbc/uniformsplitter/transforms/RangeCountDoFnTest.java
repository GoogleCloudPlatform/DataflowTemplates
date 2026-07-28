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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.DataSourceProviderImpl;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
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
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RangeCountDoFnTest {
  @Mock SerializableFunction<Void, DataSource> mockDataSourceProviderFn;
  @Mock DataSource mockDataSource;

  @Mock Connection mockConnection;

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
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(42L);
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
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
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
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
    when(mockPreparedStatemet.executeQuery()).thenThrow(new SQLException("test"));
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
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
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false /* Empty ResultSet */).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(0L);
    when(mockResultSet.wasNull()).thenReturn(true) /* Null ResultSet */;
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
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
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("existingTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnTypeName("dummy")
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
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(tableSpec1));

    Range inputMissingTable =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("missingTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    assertThrows(
        RuntimeException.class,
        () -> rangeCountDoFn.processElement(inputMissingTable, mockOut, mockProcessContext));
  }

  @Test
  public void testIsValidRangeHelperFunction() {
    TableIdentifier existingTable =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("existingTable")
            .build();
    TableIdentifier missingTable =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("missingTable")
            .build();

    ImmutableMap<TableIdentifier, String> countQueries =
        ImmutableMap.of(existingTable, "SELECT COUNT(*) FROM existingTable");

    Range inputExistingTable =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(existingTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    Range inputMissingFromCountQueries =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(missingTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    Range inputMissingFromNumColumnsMap =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
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

    when(mockPreparedStatement1.executeQuery()).thenReturn(mockResultSet1);
    when(mockResultSet1.next()).thenReturn(true);
    when(mockResultSet1.getLong(1)).thenReturn(42L);

    when(mockPreparedStatement2.executeQuery()).thenReturn(mockResultSet2);
    when(mockResultSet2.next()).thenReturn(true);
    when(mockResultSet2.getLong(1)).thenReturn(84L);

    TableSplitSpecification tableSpec1 =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable1")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnTypeName("dummy")
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
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable2")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnTypeName("dummy")
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
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable1")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();
    Range input2 =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable2")
                    .build())
            .setColName("col2")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(200)
            .build();

    rangeCountDoFn.processElement(input1, mockOut, mockProcessContext);
    rangeCountDoFn.processElement(input2, mockOut, mockProcessContext);

    verify(mockOut, times(2)).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getAllValues().get(0))
        .isEqualTo(input1.withCount(42L, mockProcessContext));
    assertThat(rangeCaptor.getAllValues().get(1))
        .isEqualTo(input2.withCount(84L, mockProcessContext));
  }

  @Test
  public void testCountModeApproxSuccess() throws Exception {
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");
    when(mockAdapter.getApproximateCountQuery(anyString(), any())).thenReturn("EXPLAIN SELECT");

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatemet);
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockAdapter.parseApproximateCount(mockResultSet)).thenReturn(1500L);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .setColumnTypeName("dummy")
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(spec),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(spec.tableIdentifier())
            .setColName("col1")
            .setColumnTypeName("dummy")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().count()).isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(1500L);
  }

  @Test
  public void testCountModeApproxUnsupported() throws Exception {
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(false);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .setColumnTypeName("dummy")
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(spec),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(spec.tableIdentifier())
            .setColName("col1")
            .setColumnTypeName("dummy")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().count()).isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testCountModeTryExactSuccess() throws Exception {
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(false);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(500L);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .setColumnTypeName("dummy")
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(spec),
            RangeCountTransform.CountMode.TRY_EXACT);

    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(spec.tableIdentifier())
            .setColName("col1")
            .setColumnTypeName("dummy")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().count()).isEqualTo(500L);
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(500L);
  }

  @Test
  public void testCountModeTryExactTimeoutFallback() throws Exception {
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");
    when(mockAdapter.getApproximateCountQuery(anyString(), any())).thenReturn("EXPLAIN SELECT");

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);

    PreparedStatement mockExactStmt = Mockito.mock(PreparedStatement.class);
    PreparedStatement mockApproxStmt = Mockito.mock(PreparedStatement.class);

    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockExactStmt);
    when(mockConnection.prepareStatement("EXPLAIN SELECT")).thenReturn(mockApproxStmt);

    when(mockExactStmt.executeQuery()).thenThrow(new SQLTimeoutException("timeout"));
    when(mockApproxStmt.executeQuery()).thenReturn(mockResultSet);
    when(mockAdapter.parseApproximateCount(mockResultSet)).thenReturn(2500L);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .setColumnTypeName("dummy")
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(spec),
            RangeCountTransform.CountMode.TRY_EXACT);

    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(spec.tableIdentifier())
            .setColName("col1")
            .setColumnTypeName("dummy")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().count()).isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(2500L);
  }

  @Test
  public void testApproxCountQueryExceptionLogsOncePerWorker() throws Exception {
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");
    when(mockAdapter.getApproximateCountQuery(anyString(), any())).thenReturn("EXPLAIN SELECT");

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatemet);
    when(mockPreparedStatemet.executeQuery()).thenThrow(new SQLException("ORA-01031"));

    RangeCountDoFn.resetLoggedApproxCountError();

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .setColumnTypeName("dummy")
                        .build()))
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setInitialSplitHeight(5L)
            .setSplitStagesCount(1L)
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(spec),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setTableIdentifier(spec.tableIdentifier())
            .setColName("col1")
            .setColumnTypeName("dummy")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);
    doFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut, times(2)).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getAllValues().get(0).approxCount())
        .isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(rangeCaptor.getAllValues().get(1).approxCount())
        .isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testLifecycleMethods() throws Exception {
    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of());

    rangeCountDoFn.startBundle();
    java.lang.reflect.Field managerField =
        RangeCountDoFn.class.getDeclaredField("dataSourceManager");
    managerField.setAccessible(true);
    assertThat(managerField.get(rangeCountDoFn)).isNotNull();

    rangeCountDoFn.finishBundle();
    assertThat(managerField.get(rangeCountDoFn)).isNull();

    rangeCountDoFn.startBundle();
    assertThat(managerField.get(rangeCountDoFn)).isNotNull();
    rangeCountDoFn.tearDown();
    assertThat(managerField.get(rangeCountDoFn)).isNull();
  }

  @Test
  public void testExecuteExactCountGeneralException() throws Exception {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    // Throw a non-SQLException to trigger the general catch block
    when(mockPreparedStatemet.executeQuery()).thenThrow(new RuntimeException("General Exception"));

    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    assertThrows(
        RuntimeException.class,
        () -> rangeCountDoFn.processElement(input, mockOut, mockProcessContext));
  }

  @Test
  public void testExecuteApproxCountGeneralException() throws Exception {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatemet);

    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");
    when(mockAdapter.getApproximateCountQuery(anyString(), any())).thenReturn("EXPLAIN SELECT");

    // Trigger "Unexpected error in approximate count"
    when(mockPreparedStatemet.executeQuery()).thenThrow(new RuntimeException("Unexpected"));

    RangeCountDoFn.resetLoggedApproxCountError();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);
    doFn.processElement(input, mockOut, mockProcessContext);
    verify(mockOut, times(2)).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testApproxCountQueryIsNull() throws Exception {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    // Return false during construction, so approxCountQueries map is empty.
    // Return true during processElement, so executeApproxCount is called.
    when(mockAdapter.supportsApproximateCounts()).thenReturn(false).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);
    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testExecuteExactCountEmptyResultSet() throws Exception {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    // ResultSet.next() returns false
    when(mockResultSet.next()).thenReturn(false);

    RangeCountDoFn rangeCountDoFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
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
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    rangeCountDoFn.processElement(input, mockOut, mockProcessContext);
    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().count()).isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testApproximateCountDirectUnsupported() throws Exception {
    Mockito.lenient().when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(false);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            2000L,
            mockAdapter,
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable")
                            .build())
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);
    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(Range.INDETERMINATE_COUNT);
  }

  @Test
  public void testExecuteApproxCountReturnsIndeterminateOnParseFailure() throws Exception {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatemet);

    UniformSplitterDBAdapter mockAdapter = Mockito.mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.supportsApproximateCounts()).thenReturn(true);
    when(mockAdapter.getCountQuery(anyString(), any(), anyLong())).thenReturn("SELECT COUNT(*)");
    when(mockAdapter.getApproximateCountQuery(anyString(), any())).thenReturn("EXPLAIN SELECT");
    // Adapter returns -1L on parse failure
    when(mockAdapter.parseApproximateCount(any())).thenReturn(-1L);

    TableIdentifier tableIdentifier =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("testTable")
            .build();

    RangeCountDoFn doFn =
        new RangeCountDoFn(
            DataSourceProviderImpl.builder()
                .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", mockDataSourceProviderFn)
                .build(),
            42L,
            mockAdapter,
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(tableIdentifier)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnTypeName("dummy")
                                .setColumnName("col1")
                                .setColumnClass(Long.class)
                                .build()))
                    .setApproxRowCount(100L)
                    .setMaxPartitionsHint(10L)
                    .setInitialSplitHeight(5L)
                    .setSplitStagesCount(1L)
                    .build()),
            RangeCountTransform.CountMode.APPROX);

    Range input =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(tableIdentifier)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .build();

    doFn.processElement(input, mockOut, mockProcessContext);

    // Verify that -1L was translated to INDETERMINATE_COUNT
    verify(mockOut).output(rangeCaptor.capture());
    assertThat(rangeCaptor.getValue().approxCount()).isEqualTo(Range.INDETERMINATE_COUNT);
  }
}
