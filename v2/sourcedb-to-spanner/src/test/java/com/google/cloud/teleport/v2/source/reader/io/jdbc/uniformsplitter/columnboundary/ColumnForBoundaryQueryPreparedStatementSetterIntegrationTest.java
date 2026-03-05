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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Integration Test class for {@link ColumnForBoundaryQueryPreparedStatementSetter}. */
@RunWith(MockitoJUnitRunner.class)
public class ColumnForBoundaryQueryPreparedStatementSetterIntegrationTest {

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

    // 2.2 Insert Data (Using PreparedStatement for Efficiency & Security)
    String insertSQL = "INSERT INTO test_table_column_boundary (col1, col2) VALUES (?, ?)";
    PreparedStatement stmtInsert = connection.prepareStatement(insertSQL);

    // Batch the insert operations
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

  private void dropDerbyTable() throws SQLException {
    Statement statement = connection.createStatement();
    statement.executeUpdate("drop table test_table_column_boundary");
  }

  @Test
  public void testSetParameters_withDbIntegration() throws Exception {
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
    ColumnForBoundaryQueryPreparedStatementSetter columnForBoundaryQueryPreparedStatementSetter =
        new ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList.of(tableSplitSpec));

    // Test for initial column boundary discovery (no parent range)
    ColumnForBoundaryQuery initialColumn =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .setParentRange(null) // Explicitly set null parent range
            .build();

    // Boundary query for col1 with parameters that are filled with disabled flags for the second
    // column.
    String boundaryQueryCol1 =
        "SELECT MIN(col1), MAX(col1) FROM test_table_column_boundary WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))";
    PreparedStatement boundaryStmtCol1 = connection.prepareStatement(boundaryQueryCol1);

    columnForBoundaryQueryPreparedStatementSetter.setParameters(initialColumn, boundaryStmtCol1);
    ResultSet fullBoundaryResultSet = boundaryStmtCol1.executeQuery();
    fullBoundaryResultSet.next();
    Pair<Integer, Integer> initialBoundary =
        Pair.of(fullBoundaryResultSet.getInt(1), fullBoundaryResultSet.getInt(2));

    // Test for column boundary discovery within a parent range
    Range parentRange =
        Range.<Integer>builder()
            .setTableIdentifier(testTableIdentifier)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(14)
            .setEnd(15)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    ColumnForBoundaryQuery columnWithinRange =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(testTableIdentifier)
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(parentRange)
            .build();

    // Boundary query for col2 with parent range conditions.
    String boundaryQueryCol2 =
        "SELECT MIN(col2), MAX(col2) FROM test_table_column_boundary WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))";
    PreparedStatement boundaryStmtCol2 = connection.prepareStatement(boundaryQueryCol2);

    columnForBoundaryQueryPreparedStatementSetter.setParameters(
        columnWithinRange, boundaryStmtCol2);
    ResultSet columnWithinRangeResultSet = boundaryStmtCol2.executeQuery();
    columnWithinRangeResultSet.next();
    Pair<Integer, Integer> columnWithinRangeBoundary =
        Pair.of(columnWithinRangeResultSet.getInt(1), columnWithinRangeResultSet.getInt(2));

    assertThat(initialBoundary).isEqualTo(Pair.of(10, 125));
    assertThat(columnWithinRangeBoundary).isEqualTo(Pair.of(135, 140));
  }

  @After
  public void exitDerby() throws SQLException {
    dropDerbyTable();
    connection.close();
  }
}
