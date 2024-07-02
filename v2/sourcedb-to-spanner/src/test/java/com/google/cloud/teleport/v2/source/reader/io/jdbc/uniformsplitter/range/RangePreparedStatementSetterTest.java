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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
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
    PreparedStatement stmtInsert = connection.prepareStatement(insertSQL);

    // Batch the insert operations
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

  private void dropDerbyTable() throws SQLException {
    Statement statement = connection.createStatement();
    statement.executeUpdate("drop table test_table_range_setter");
  }

  @Test
  public void testSetParameters() throws Exception {

    ImmutableList<String> partitionCols = ImmutableList.of("col1", "col2");
    RangePreparedStatementSetter rangePreparedStatementSetter =
        new RangePreparedStatementSetter(partitionCols.size());

    Range singleColNonLastRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
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
    ResultSet readStmtSingleColNonLastResultSet = readStmtSingleColNonLast.executeQuery();
    ImmutableList.Builder<String> readSingleColNonLastRangedataPointsBuilder =
        ImmutableList.builder();
    while (readStmtSingleColNonLastResultSet.next()) {
      readSingleColNonLastRangedataPointsBuilder.add(
          readStmtSingleColNonLastResultSet.getString("data").trim());
    }

    String countQuery =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .getCountQuery("test_table_range_setter", partitionCols, 0);
    PreparedStatement countStmtSingleColNonLast = connection.prepareStatement(countQuery);
    rangePreparedStatementSetter.setParameters(singleColNonLastRange, countStmtSingleColNonLast);
    ResultSet countStmtSingleColNonLastResultSet = countStmtSingleColNonLast.executeQuery();
    countStmtSingleColNonLastResultSet.next();
    Integer countSingleColNonLast = countStmtSingleColNonLastResultSet.getInt(1);

    Range singleColLastRange = singleColNonLastRange.toBuilder().setIsLast(true).build();
    rangePreparedStatementSetter.setParameters(singleColLastRange, readStmtSingleColNonLast);
    ResultSet readStmtSingleColLastResultSet = readStmtSingleColNonLast.executeQuery();
    ImmutableList.Builder<String> readSingleColLastRangedataPointsBuilder = ImmutableList.builder();
    while (readStmtSingleColLastResultSet.next()) {
      readSingleColLastRangedataPointsBuilder.add(
          readStmtSingleColLastResultSet.getString("data").trim());
    }

    Range bothColRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(11)
            .setIsLast(false)
            .build()
            .withChildRange(
                Range.<Integer>builder()
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

    assertThat(readSingleColNonLastRangedataPointsBuilder.build())
        .isEqualTo(ImmutableList.of("Data A", "Data B"));
    assertThat(countSingleColNonLast).isEqualTo(2);
    assertThat(readSingleColLastRangedataPointsBuilder.build())
        .isEqualTo(ImmutableList.of("Data A", "Data B", "Data C"));
    assertThat(countBothCol).isEqualTo(1);
  }

  @After
  public void exitDerby() throws SQLException {
    dropDerbyTable();
    connection.close();
  }
}
