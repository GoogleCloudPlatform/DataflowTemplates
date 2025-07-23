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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class MySqlInformationSchemaScannerTest {

  @Test
  public void testScanSingleTable() throws SQLException {
    // Mock JDBC objects
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);
    ResultSet columnRs = mock(ResultSet.class);
    ResultSet pkRs = mock(ResultSet.class);

    // Mock table query
    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn("users");
    when(tableRs.getString(2)).thenReturn("testdb");

    // Mock column query
    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'users' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs);
    when(columnRs.next()).thenReturn(true, false);
    when(columnRs.getString("column_name")).thenReturn("id");
    when(columnRs.getString("data_type")).thenReturn("INT");
    when(columnRs.getString("is_nullable")).thenReturn("NO");
    when(columnRs.getString("column_key")).thenReturn("PRI");
    when(columnRs.getString("character_maximum_length")).thenReturn("10");
    when(columnRs.getString("numeric_precision")).thenReturn(null);
    when(columnRs.getString("numeric_scale")).thenReturn(null);

    // Mock primary key query
    when(stmt.executeQuery(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = 'testdb' AND table_name = 'users' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position"))
        .thenReturn(pkRs);
    when(pkRs.next()).thenReturn(true, false);
    when(pkRs.getString("column_name")).thenReturn("id");

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    SourceSchema schema = scanner.scan();

    assertEquals("testdb", schema.databaseName());
    assertEquals(SourceDatabaseType.MYSQL, schema.sourceType());
    assertEquals(1, schema.tables().size());
    SourceTable table = schema.tables().get("users");
    assertEquals("users", table.name());
    assertEquals("testdb", table.schema());
    assertEquals(1, table.columns().size());
    SourceColumn column = table.columns().get(0);
    assertEquals("id", column.name());
    assertEquals("INT", column.type());
    assertEquals(false, column.isNullable());
    assertEquals(true, column.isPrimaryKey());
    assertEquals(Long.valueOf(10L), column.size());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("id", table.primaryKeyColumns().get(0));
  }

  @Test
  public void testScanTableWithNoColumns() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);
    ResultSet columnRs = mock(ResultSet.class);
    ResultSet pkRs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn("empty_table");
    when(tableRs.getString(2)).thenReturn("testdb");

    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'empty_table' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs);
    when(columnRs.next()).thenReturn(false);

    when(stmt.executeQuery(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = 'testdb' AND table_name = 'empty_table' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position"))
        .thenReturn(pkRs);
    when(pkRs.next()).thenReturn(false);

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    SourceSchema schema = scanner.scan();
    SourceTable table = schema.tables().get("empty_table");
    assertEquals(0, table.columns().size());
    assertEquals(0, table.primaryKeyColumns().size());
  }

  @Test
  public void testScanTableWithSpecialCharacterNames() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);
    ResultSet columnRs = mock(ResultSet.class);
    ResultSet pkRs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn("user$#@!");
    when(tableRs.getString(2)).thenReturn("testdb");

    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'user$#@!' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs);
    when(columnRs.next()).thenReturn(true, false);
    when(columnRs.getString("column_name")).thenReturn("col@!$");
    when(columnRs.getString("data_type")).thenReturn("VARCHAR");
    when(columnRs.getString("is_nullable")).thenReturn("YES");
    when(columnRs.getString("column_key")).thenReturn("");
    when(columnRs.getString("character_maximum_length")).thenReturn("255");
    when(columnRs.getString("numeric_precision")).thenReturn(null);
    when(columnRs.getString("numeric_scale")).thenReturn(null);

    when(stmt.executeQuery(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = 'testdb' AND table_name = 'user$#@!' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position"))
        .thenReturn(pkRs);
    when(pkRs.next()).thenReturn(false);

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    SourceSchema schema = scanner.scan();
    SourceTable table = schema.tables().get("user$#@!");
    assertEquals("user$#@!", table.name());
    assertEquals(1, table.columns().size());
    assertEquals("col@!$", table.columns().get(0).name());
  }

  @Test
  public void testScanWithNullTableName() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn(null);
    when(tableRs.getString(2)).thenReturn("testdb");

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    // Should not throw, but table with null name should not be added
    SourceSchema schema = scanner.scan();
    assertEquals(0, schema.tables().size());
  }

  @Test(expected = RuntimeException.class)
  public void testScanThrowsSQLExceptionOnColumns() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn("users");
    when(tableRs.getString(2)).thenReturn("testdb");

    // Simulate SQLException when scanning columns
    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'users' "
                + "ORDER BY ordinal_position"))
        .thenThrow(new SQLException("Column scan error"));

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    scanner.scan();
  }

  @Test
  public void testScanMultipleTables() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);
    ResultSet columnRs1 = mock(ResultSet.class);
    ResultSet columnRs2 = mock(ResultSet.class);
    ResultSet pkRs1 = mock(ResultSet.class);
    ResultSet pkRs2 = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'testdb' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, true, false);
    when(tableRs.getString(1)).thenReturn("users", "orders");
    when(tableRs.getString(2)).thenReturn("testdb", "testdb");

    // users table
    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'users' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs1);
    when(columnRs1.next()).thenReturn(true, false);
    when(columnRs1.getString("column_name")).thenReturn("id");
    when(columnRs1.getString("data_type")).thenReturn("INT");
    when(columnRs1.getString("is_nullable")).thenReturn("NO");
    when(columnRs1.getString("column_key")).thenReturn("PRI");
    when(columnRs1.getString("character_maximum_length")).thenReturn("10");
    when(columnRs1.getString("numeric_precision")).thenReturn(null);
    when(columnRs1.getString("numeric_scale")).thenReturn(null);
    when(stmt.executeQuery(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = 'testdb' AND table_name = 'users' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position"))
        .thenReturn(pkRs1);
    when(pkRs1.next()).thenReturn(true, false);
    when(pkRs1.getString("column_name")).thenReturn("id");

    // orders table
    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'testdb' AND table_name = 'orders' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs2);
    when(columnRs2.next()).thenReturn(true, false);
    when(columnRs2.getString("column_name")).thenReturn("order_id");
    when(columnRs2.getString("data_type")).thenReturn("BIGINT");
    when(columnRs2.getString("is_nullable")).thenReturn("NO");
    when(columnRs2.getString("column_key")).thenReturn("PRI");
    when(columnRs2.getString("character_maximum_length")).thenReturn(null);
    when(columnRs2.getString("numeric_precision")).thenReturn("20");
    when(columnRs2.getString("numeric_scale")).thenReturn(null);
    when(stmt.executeQuery(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = 'testdb' AND table_name = 'orders' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position"))
        .thenReturn(pkRs2);
    when(pkRs2.next()).thenReturn(true, false);
    when(pkRs2.getString("column_name")).thenReturn("order_id");

    MySqlInformationSchemaScanner scanner = new MySqlInformationSchemaScanner(connection, "testdb");
    SourceSchema schema = scanner.scan();
    assertEquals(2, schema.tables().size());
    assertEquals("users", schema.tables().get("users").name());
    assertEquals("orders", schema.tables().get("orders").name());
  }
}
