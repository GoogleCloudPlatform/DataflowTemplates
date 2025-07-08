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
}
