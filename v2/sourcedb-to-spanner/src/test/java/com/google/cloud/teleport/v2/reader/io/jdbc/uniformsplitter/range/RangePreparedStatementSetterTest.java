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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.MockitoJUnitRunner;
import org.postgresql.util.PGobject;

/** Test class for {@link RangePreparedStatementSetter}. */
@RunWith(MockitoJUnitRunner.class)
public class RangePreparedStatementSetterTest {
  Connection connection;

  @BeforeClass
  public static void beforeClass() {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
  }

  @Before
  public void initDerby() throws SQLException, ClassNotFoundException {
    // Creating testDB database
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    connection = DriverManager.getConnection("jdbc:derby:memory:TestingDB;create=true");
    createDerbyTable();
  }

  private void createDerbyTable() throws SQLException {
    Statement stmtCreateTable = connection.createStatement();
    String createTableSQL =
        "CREATE TABLE test_table_range_setter ("
            + "col1 INT,"
            + "col2 INT,"
            + "data CHAR(20),"
            + "PRIMARY KEY (col1, col2)"
            + ")";
    stmtCreateTable.executeUpdate(createTableSQL);

    // 2.2 Insert Data (Using PreparedStatement for Efficiency & Security)
    String insertSQL = "INSERT INTO test_table_range_setter (col1, col2, data) VALUES (?, ?, ?)";
    try (PreparedStatement stmtInsert = connection.prepareStatement(insertSQL)) {
      stmtInsert.setInt(1, 10);
      stmtInsert.setInt(2, 30);
      stmtInsert.setString(3, "Data A");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 15);
      stmtInsert.setInt(2, 35);
      stmtInsert.setString(3, "Data B");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 25);
      stmtInsert.setInt(2, 50);
      stmtInsert.setString(3, "Data C");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 30);
      stmtInsert.setInt(2, 60);
      stmtInsert.setString(3, "Data D");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 40);
      stmtInsert.setInt(2, 70);
      stmtInsert.setString(3, "Data E");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 40);
      stmtInsert.setInt(2, 80);
      stmtInsert.setString(3, "Data F");
      stmtInsert.addBatch();

      stmtInsert.executeBatch();
    }
  }

  @Test
  public void testSetParameters_throwsExceptionOnNullElement() throws Exception {
    TableSplitSpecification tableSplitSpecification =
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
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(1L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(tableSplitSpecification));
    PreparedStatement mockStatement = mock(PreparedStatement.class);
    assertThrows(NullPointerException.class, () -> setter.setParameters(null, mockStatement));
  }

  @Test
  public void testSetRangeParameters_throwsOnUnknownTable() throws Exception {
    TableSplitSpecification tableSplitSpecification =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("knownTable")
                    .build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnTypeName("dummy")
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(1L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(tableSplitSpecification));

    Range unknownTableRange =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("unknownTable")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();
    PreparedStatement mockStatement = mock(PreparedStatement.class);
    assertThrows(
        RuntimeException.class, () -> setter.setParameters(unknownTableRange, mockStatement));
  }

  @Test
  public void testSetParameters() throws Exception {

    ImmutableList<String> partitionCols = ImmutableList.of("col1", "col2");
    TableSplitSpecification tableSplitSpecification =
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
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnTypeName("dummy")
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(2L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(2L)
            .build();
    RangePreparedStatementSetter rangePreparedStatementSetter =
        new RangePreparedStatementSetter(ImmutableList.of(tableSplitSpecification));

    Range singleColNonLastRange =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(tableSplitSpecification.tableIdentifier())
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();
    String readQuery =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .getReadQuery("test_table_range_setter", partitionCols);
    PreparedStatement readStmtSingleColNonLast = connection.prepareStatement(readQuery);
    rangePreparedStatementSetter.setParameters(singleColNonLastRange, readStmtSingleColNonLast);
    ImmutableList.Builder<String> readSingleColNonLastRangedataPointsBuilder =
        ImmutableList.builder();
    ResultSet readStmtSingleColNonLastResultSet = readStmtSingleColNonLast.executeQuery();
    while (readStmtSingleColNonLastResultSet.next()) {
      readSingleColNonLastRangedataPointsBuilder.add(
          readStmtSingleColNonLastResultSet.getString("data").trim());
    }
    readStmtSingleColNonLastResultSet.close();

    String countQuery =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .getCountQuery("test_table_range_setter", partitionCols, 0);
    PreparedStatement countStmtSingleColNonLast = connection.prepareStatement(countQuery);
    rangePreparedStatementSetter.setParameters(singleColNonLastRange, countStmtSingleColNonLast);
    ResultSet countStmtSingleColNonLastResultSet = countStmtSingleColNonLast.executeQuery();
    countStmtSingleColNonLastResultSet.next();
    Integer countSingleColNonLast = countStmtSingleColNonLastResultSet.getInt(1);
    countStmtSingleColNonLastResultSet.close();

    Range singleColLastRange = singleColNonLastRange.toBuilder().setIsLast(true).build();
    rangePreparedStatementSetter.setParameters(singleColLastRange, readStmtSingleColNonLast);
    ResultSet readStmtSingleColLastResultSet = readStmtSingleColNonLast.executeQuery();
    ImmutableList.Builder<String> readSingleColLastRangedataPointsBuilder = ImmutableList.builder();
    while (readStmtSingleColLastResultSet.next()) {
      readSingleColLastRangedataPointsBuilder.add(
          readStmtSingleColLastResultSet.getString("data").trim());
    }
    readStmtSingleColLastResultSet.close();

    Range bothColRange =
        Range.<Integer>builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(tableSplitSpecification.tableIdentifier())
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(11)
            .setIsLast(false)
            .build()
            .withChildRange(
                Range.<Integer>builder()
                    .setColumnTypeName("dummy")
                    .setTableIdentifier(tableSplitSpecification.tableIdentifier())
                    .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                    .setColName("col2")
                    .setColClass(Integer.class)
                    .setStart(30)
                    .setEnd(40)
                    .build(),
                null);
    PreparedStatement countStmtBothCol = connection.prepareStatement(countQuery);
    rangePreparedStatementSetter.setParameters(bothColRange, countStmtBothCol);
    ResultSet countStmtBothColResultSet = countStmtBothCol.executeQuery();
    countStmtBothColResultSet.next();
    Integer countBothCol = countStmtBothColResultSet.getInt(1);
    countStmtBothColResultSet.close();

    assertThat(readSingleColNonLastRangedataPointsBuilder.build())
        .isEqualTo(ImmutableList.of("Data A", "Data B"));
    assertThat(countSingleColNonLast).isEqualTo(2);
    assertThat(readSingleColLastRangedataPointsBuilder.build())
        .isEqualTo(ImmutableList.of("Data A", "Data B", "Data C"));
    assertThat(countBothCol).isEqualTo(1);
  }

  private void dropDerbyTable() throws SQLException {
    Statement statement = connection.createStatement();
    statement.executeUpdate("drop table test_table_range_setter");
  }

  @After
  public void exitDerby() throws SQLException {
    dropDerbyTable();
    connection.close();
  }

  @Test
  public void testSetParameters_withUuidColumn() throws Exception {
    byte[] startBytes = new byte[16];
    byte[] endBytes = new byte[16];
    Arrays.fill(endBytes, (byte) 0xFF);

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_uuid_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("uuid_col")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid") // UUID column type
            .build();

    Boundary<byte[]> boundary =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startBytes)
            .setEnd(endBytes)
            .setBoundarySplitter(
                BoundarySplitterFactory.create(
                    byte[].class)) // UUID is split using byte array splitter
            .build();

    Range range =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("uuid_col")
            .setColClass(byte[].class)
            .setStart(startBytes)
            .setEnd(endBytes)
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    // Verify that raw 16-byte arrays (startBytes and endBytes) were successfully converted into
    // native java.util.UUID instances before being bound to parameter indices 2 and 3.
    // 0L, 0L represents 16 bytes of 0x00 (min UUID), and -1L, -1L represents 16 bytes of 0xFF (max
    // UUID).
    verify(mockStatement).setObject(2, new java.util.UUID(0L, 0L));
    verify(mockStatement).setObject(3, new java.util.UUID(-1L, -1L));
  }

  @Test
  public void testSetParameters_withBit() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.builder().setDataSourceId("test_ds").setTableName("test_bit_table").build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("bit_col")
            .setColumnClass(String.class)
            .setColumnTypeName("bit")
            .build();

    Range range =
        Range.builder()
            .setColumnTypeName("bit")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.createBitSplitter())
            .setColName("bit_col")
            .setColClass(String.class)
            .setStart("0000")
            .setEnd("1010")
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    ArgumentCaptor<Object> captor2 = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Object> captor3 = ArgumentCaptor.forClass(Object.class);
    verify(mockStatement).setObject(ArgumentMatchers.eq(2), captor2.capture());
    verify(mockStatement).setObject(ArgumentMatchers.eq(3), captor3.capture());

    Object capturedStart = captor2.getValue();
    assertThat(capturedStart).isInstanceOf(PGobject.class);
    PGobject startPgObj = (PGobject) capturedStart;
    assertThat(startPgObj.getType()).isEqualTo("bit");
    assertThat(startPgObj.getValue()).isEqualTo("0000");

    Object capturedEnd = captor3.getValue();
    assertThat(capturedEnd).isInstanceOf(PGobject.class);
    PGobject endPgObj = (PGobject) capturedEnd;
    assertThat(endPgObj.getType()).isEqualTo("bit");
    assertThat(endPgObj.getValue()).isEqualTo("1010");
  }

  @Test
  public void testSetParameters_withLocalTimeMax() throws Exception {
    java.time.LocalTime start = java.time.LocalTime.parse("08:00:00");
    java.time.LocalTime end = java.time.LocalTime.MAX;

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_time_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("time_col")
            .setColumnClass(java.time.LocalTime.class)
            .setColumnTypeName("time") // Time column type
            .build();

    Range range =
        Range.builder()
            .setColumnTypeName("time")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(java.time.LocalTime.class))
            .setColName("time_col")
            .setColClass(java.time.LocalTime.class)
            .setStart(start)
            .setEnd(end)
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockStatement).setObject(ArgumentMatchers.eq(3), captor.capture());
    Object capturedEnd = captor.getValue();
    assertThat(capturedEnd).isInstanceOf(PGobject.class);
    PGobject pgObj = (PGobject) capturedEnd;
    assertThat(pgObj.getType()).isEqualTo("time");
    assertThat(pgObj.getValue()).isEqualTo("24:00:00");
  }

  @Test
  public void testSetParameters_withOffsetTimeMax() throws Exception {
    java.time.OffsetTime start = java.time.OffsetTime.parse("08:00:00+05:00");
    java.time.OffsetTime end =
        java.time.OffsetTime.of(java.time.LocalTime.MAX, java.time.ZoneOffset.ofHours(5));

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_timetz_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("timetz_col")
            .setColumnClass(java.time.OffsetTime.class)
            .setColumnTypeName("timetz")
            .build();

    Range range =
        Range.builder()
            .setColumnTypeName("timetz")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(java.time.OffsetTime.class))
            .setColName("timetz_col")
            .setColClass(java.time.OffsetTime.class)
            .setStart(start)
            .setEnd(end)
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockStatement).setObject(ArgumentMatchers.eq(3), captor.capture());
    Object capturedEnd = captor.getValue();
    assertThat(capturedEnd).isInstanceOf(PGobject.class);
    PGobject pgObj = (PGobject) capturedEnd;
    assertThat(pgObj.getType()).isEqualTo("timetz");
    assertThat(pgObj.getValue()).isEqualTo("24:00:00+05:00");
  }

  @Test
  public void testSetParameters_withOffsetTimeMax_zuluOffset() throws Exception {
    java.time.OffsetTime start = java.time.OffsetTime.parse("08:00:00Z");
    java.time.OffsetTime end =
        java.time.OffsetTime.of(java.time.LocalTime.MAX, java.time.ZoneOffset.UTC);

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_timetz_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("timetz_col")
            .setColumnClass(java.time.OffsetTime.class)
            .setColumnTypeName("timetz")
            .build();

    Range range =
        Range.builder()
            .setColumnTypeName("timetz")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(java.time.OffsetTime.class))
            .setColName("timetz_col")
            .setColClass(java.time.OffsetTime.class)
            .setStart(start)
            .setEnd(end)
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockStatement).setObject(ArgumentMatchers.eq(3), captor.capture());
    Object capturedEnd = captor.getValue();
    assertThat(capturedEnd).isInstanceOf(PGobject.class);
    PGobject pgObj = (PGobject) capturedEnd;
    assertThat(pgObj.getType()).isEqualTo("timetz");
    assertThat(pgObj.getValue()).isEqualTo("24:00:00+00");
  }
}
