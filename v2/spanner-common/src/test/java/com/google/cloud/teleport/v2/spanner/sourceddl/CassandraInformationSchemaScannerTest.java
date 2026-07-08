/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;

public class CassandraInformationSchemaScannerTest {

  @Test
  public void testScanSingleTable() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    Iterator<Row> tableIterator = Collections.singletonList(tableRow).iterator();

    PreparedStatement columnStmt = mock(PreparedStatement.class);
    BoundStatement columnBoundStmt = mock(BoundStatement.class);
    ResultSet columnResult = mock(ResultSet.class);
    Row columnRow = mock(Row.class);
    Iterator<Row> columnIterator = Collections.singletonList(columnRow).iterator();

    // Mock table query
    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn("users");

    // Mock column query
    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenReturn(columnStmt);
    when(columnStmt.bind("ks1", "users")).thenReturn(columnBoundStmt);
    when(session.execute(columnBoundStmt)).thenReturn(columnResult);
    when(columnResult.iterator()).thenReturn(columnIterator);
    when(columnRow.getString("column_name")).thenReturn("id");
    when(columnRow.getString("type")).thenReturn("int");
    when(columnRow.getString("kind")).thenReturn("partition_key");

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();

    assertEquals("ks1", schema.databaseName());
    assertEquals(SourceDatabaseType.CASSANDRA, schema.sourceType());
    assertEquals(1, schema.tables().size());
    SourceTable table = schema.tables().get("users");
    assertEquals("users", table.name());
    assertEquals("ks1", table.schema());
    assertEquals(1, table.columns().size());
    SourceColumn column = table.columns().get(0);
    assertEquals("id", column.name());
    assertEquals("INT", column.type());
    assertEquals(true, column.isNullable());
    assertEquals(true, column.isPrimaryKey());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("id", table.primaryKeyColumns().get(0));
  }

  @Test
  public void testScanTableWithNoColumns() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    java.util.Iterator<Row> tableIterator =
        java.util.Collections.singletonList(tableRow).iterator();

    PreparedStatement columnStmt = mock(PreparedStatement.class);
    BoundStatement columnBoundStmt = mock(BoundStatement.class);
    ResultSet columnResult = mock(ResultSet.class);
    java.util.Iterator<Row> columnIterator = java.util.Collections.emptyIterator();

    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn("empty_table");

    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenReturn(columnStmt);
    when(columnStmt.bind("ks1", "empty_table")).thenReturn(columnBoundStmt);
    when(session.execute(columnBoundStmt)).thenReturn(columnResult);
    when(columnResult.iterator()).thenReturn(columnIterator);

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();
    SourceTable table = schema.tables().get("empty_table");
    assertEquals(0, table.columns().size());
    assertEquals(0, table.primaryKeyColumns().size());
  }

  @Test
  public void testScanTableWithSpecialCharacterNames() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    java.util.Iterator<Row> tableIterator =
        java.util.Collections.singletonList(tableRow).iterator();

    PreparedStatement columnStmt = mock(PreparedStatement.class);
    BoundStatement columnBoundStmt = mock(BoundStatement.class);
    ResultSet columnResult = mock(ResultSet.class);
    Row columnRow = mock(Row.class);
    java.util.Iterator<Row> columnIterator =
        java.util.Collections.singletonList(columnRow).iterator();

    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn("user$#@!");

    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenReturn(columnStmt);
    when(columnStmt.bind("ks1", "user$#@!")).thenReturn(columnBoundStmt);
    when(session.execute(columnBoundStmt)).thenReturn(columnResult);
    when(columnResult.iterator()).thenReturn(columnIterator);
    when(columnRow.getString("column_name")).thenReturn("col@!$");
    when(columnRow.getString("type")).thenReturn("text");
    when(columnRow.getString("kind")).thenReturn("");

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();
    SourceTable table = schema.tables().get("user$#@!");
    assertEquals("user$#@!", table.name());
    assertEquals(1, table.columns().size());
    assertEquals("col@!$", table.columns().get(0).name());
  }

  @Test
  public void testScanWithNullTableName() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    java.util.Iterator<Row> tableIterator =
        java.util.Collections.singletonList(tableRow).iterator();

    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn(null);

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();
    assertEquals(0, schema.tables().size());
  }

  @Test(expected = RuntimeException.class)
  public void testScanThrowsExceptionOnColumns() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    java.util.Iterator<Row> tableIterator =
        java.util.Collections.singletonList(tableRow).iterator();

    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn("users");

    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenThrow(new RuntimeException("Column scan error"));

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    scanner.scan();
  }

  @Test
  public void testScanMultipleTables() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow1 = mock(Row.class);
    Row tableRow2 = mock(Row.class);
    java.util.Iterator<Row> tableIterator =
        java.util.Arrays.asList(tableRow1, tableRow2).iterator();

    PreparedStatement columnStmt = mock(PreparedStatement.class);
    BoundStatement columnBoundStmt1 = mock(BoundStatement.class);
    BoundStatement columnBoundStmt2 = mock(BoundStatement.class);
    ResultSet columnResult1 = mock(ResultSet.class);
    ResultSet columnResult2 = mock(ResultSet.class);
    Row columnRow1 = mock(Row.class);
    Row columnRow2 = mock(Row.class);
    java.util.Iterator<Row> columnIterator1 =
        java.util.Collections.singletonList(columnRow1).iterator();
    java.util.Iterator<Row> columnIterator2 =
        java.util.Collections.singletonList(columnRow2).iterator();

    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow1.getString("table_name")).thenReturn("users");
    when(tableRow2.getString("table_name")).thenReturn("orders");

    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenReturn(columnStmt);
    when(columnStmt.bind("ks1", "users")).thenReturn(columnBoundStmt1);
    when(columnStmt.bind("ks1", "orders")).thenReturn(columnBoundStmt2);
    when(session.execute(columnBoundStmt1)).thenReturn(columnResult1);
    when(session.execute(columnBoundStmt2)).thenReturn(columnResult2);
    when(columnResult1.iterator()).thenReturn(columnIterator1);
    when(columnResult2.iterator()).thenReturn(columnIterator2);
    when(columnRow1.getString("column_name")).thenReturn("id");
    when(columnRow1.getString("type")).thenReturn("int");
    when(columnRow1.getString("kind")).thenReturn("partition_key");
    when(columnRow2.getString("column_name")).thenReturn("order_id");
    when(columnRow2.getString("type")).thenReturn("bigint");
    when(columnRow2.getString("kind")).thenReturn("partition_key");

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();
    assertEquals(2, schema.tables().size());
    assertEquals("users", schema.tables().get("users").name());
    assertEquals("orders", schema.tables().get("orders").name());
  }
}
