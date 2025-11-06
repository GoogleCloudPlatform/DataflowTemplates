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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ColumnForBoundaryQueryPreparedStatementSetter}. */
@RunWith(MockitoJUnitRunner.class)
public class ColumnForBoundaryQueryPreparedStatementSetterTest {

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
        "CREATE TABLE test_table_column_boundary ("
            + "col1 INT,"
            + "col2 INT,"
            + "PRIMARY KEY (col1, col2)"
            + ")";
    stmtCreateTable.executeUpdate(createTableSQL);

    // Insert Data
    String insertSQL = "INSERT INTO test_table_column_boundary (col1, col2) VALUES (?, ?)";
    try (PreparedStatement stmtInsert = connection.prepareStatement(insertSQL)) {
      stmtInsert.setInt(1, 10);
      stmtInsert.setInt(2, 30);
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 14);
      stmtInsert.setInt(2, 140);
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 15);
      stmtInsert.setInt(2, 135);
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 125);
      stmtInsert.setInt(2, 50);
      stmtInsert.addBatch();

      stmtInsert.executeBatch();
    }
  }

  @Test
  public void testSetParameters_withNullParentRange() throws Exception {
    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("test_table_column_boundary").build();
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
    verify(mockStatement, times(1)).setObject(1, false);
    verify(mockStatement, times(1)).setObject(2, null);
    verify(mockStatement, times(1)).setObject(3, null);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, null);
  }

  @Test
  public void testSetParameters_withSingleLevelParentRange() throws Exception {
    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("test_table_column_boundary").build();
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

    Range parentRange =
        Range.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(20)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    ColumnForBoundaryQuery query =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(parentRange)
            .build();

    setter.setParameters(query, mockStatement);

    verify(mockStatement, times(1)).setObject(1, true);
    verify(mockStatement, times(1)).setObject(2, 10);
    verify(mockStatement, times(1)).setObject(3, 20);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, 20);
    verify(mockStatement, times(1)).setObject(6, false);
    verify(mockStatement, times(1)).setObject(7, null);
    verify(mockStatement, times(1)).setObject(8, null);
    verify(mockStatement, times(1)).setObject(9, false);
    verify(mockStatement, times(1)).setObject(10, null);
  }

  @Test
  public void testSetParameters_withMultiLevelParentRange() throws Exception {
    TableIdentifier testTableIdentifier =
        TableIdentifier.builder().setTableName("test_table_column_boundary").build();
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

    Range childRange =
        Range.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColName("col2")
            .setColClass(Integer.class)
            .setStart(100)
            .setEnd(200)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    Range parentRange =
        Range.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(10)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build()
            .withChildRange(childRange, null);

    ColumnForBoundaryQuery query =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col3")
            .setColumnClass(Integer.class)
            .setParentRange(parentRange)
            .build();

    setter.setParameters(query, mockStatement);

    verify(mockStatement, times(1)).setObject(1, true);
    verify(mockStatement, times(1)).setObject(2, 10);
    verify(mockStatement, times(1)).setObject(3, 10);
    verify(mockStatement, times(1)).setObject(4, false);
    verify(mockStatement, times(1)).setObject(5, 10);
    verify(mockStatement, times(1)).setObject(6, true);
    verify(mockStatement, times(1)).setObject(7, 100);
    verify(mockStatement, times(1)).setObject(8, 200);
    verify(mockStatement, times(1)).setObject(9, false);
    verify(mockStatement, times(1)).setObject(10, 200);
    verify(mockStatement, times(1)).setObject(11, false);
    verify(mockStatement, times(1)).setObject(12, null);
    verify(mockStatement, times(1)).setObject(13, null);
    verify(mockStatement, times(1)).setObject(14, false);
    verify(mockStatement, times(1)).setObject(15, null);
  }

  @Test
  public void testSetParameters_withDbIntegration() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.builder().setTableName("test_table_column_boundary").build();
    ColumnForBoundaryQueryPreparedStatementSetter setter =
        new ColumnForBoundaryQueryPreparedStatementSetter(
            ImmutableList.of(
                TableSplitSpecification.builder()
                    .setTableIdentifier(tableId)
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
                    .build()));
    ColumnForBoundaryQuery initialColumn =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(tableId)
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();

    String boundaryQueryCol1 =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .getBoundaryQuery(
                "test_table_column_boundary", ImmutableList.of("col1", "col2"), "col1");
    try (PreparedStatement boundaryStmtCol1 = connection.prepareStatement(boundaryQueryCol1)) {
      setter.setParameters(initialColumn, boundaryStmtCol1);
      try (ResultSet rs = boundaryStmtCol1.executeQuery()) {
        rs.next();
        assertThat(rs.getInt(1)).isEqualTo(10);
        assertThat(rs.getInt(2)).isEqualTo(125);
      }
    }

    Range parentRange =
        Range.builder()
            .setTableIdentifier(tableId)
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(14)
            .setEnd(15)
            .setIsLast(true)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    ColumnForBoundaryQuery columnWithinRange =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(tableId)
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(parentRange)
            .build();

    String boundaryQueryCol2 =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .getBoundaryQuery(
                "test_table_column_boundary", ImmutableList.of("col1", "col2"), "col2");
    try (PreparedStatement boundaryStmtCol2 = connection.prepareStatement(boundaryQueryCol2)) {
      setter.setParameters(columnWithinRange, boundaryStmtCol2);
      try (ResultSet rs = boundaryStmtCol2.executeQuery()) {
        rs.next();
        assertThat(rs.getInt(1)).isEqualTo(135);
        assertThat(rs.getInt(2)).isEqualTo(140);
      }
    }
  }

  @After
  public void exitDerby() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("drop table test_table_column_boundary");
    }
    connection.close();
  }
}
