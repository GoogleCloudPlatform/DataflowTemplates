/*
 * Copyright (C) 2026 Google LLC
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

public class PostgreSQLInformationSchemaScannerTest {

  @Test
  public void testScanSingleTable() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet tableRs = mock(ResultSet.class);
    ResultSet columnRs = mock(ResultSet.class);
    ResultSet pkRs = mock(ResultSet.class);
    ResultSet idxRs = mock(ResultSet.class);
    ResultSet fkRs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(
            "SELECT table_name, table_schema "
                + "FROM information_schema.tables "
                + "WHERE table_schema = 'public' "
                + "AND table_type = 'BASE TABLE'"))
        .thenReturn(tableRs);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString(1)).thenReturn("users");
    when(tableRs.getString(2)).thenReturn("public");

    // Mock column query
    when(stmt.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, is_generated "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'public' AND table_name = 'users' "
                + "ORDER BY ordinal_position"))
        .thenReturn(columnRs);
    when(columnRs.next()).thenReturn(true, false);
    when(columnRs.getString("column_name")).thenReturn("id");
    when(columnRs.getString("data_type")).thenReturn("integer");
    when(columnRs.getString("is_nullable")).thenReturn("NO");
    when(columnRs.getString("is_generated")).thenReturn("NEVER");
    when(columnRs.getString("character_maximum_length")).thenReturn(null);
    when(columnRs.getString("numeric_precision")).thenReturn("32");
    when(columnRs.getString("numeric_scale")).thenReturn("0");

    // Mock primary key query
    when(stmt.executeQuery(
            "SELECT kcu.column_name "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "  ON tc.constraint_name = kcu.constraint_name "
                + "  AND tc.table_schema = kcu.table_schema "
                + "WHERE tc.table_schema = 'public' AND tc.table_name = 'users' "
                + "AND tc.constraint_type = 'PRIMARY KEY' "
                + "ORDER BY kcu.ordinal_position"))
        .thenReturn(pkRs);
    when(pkRs.next()).thenReturn(true, false);
    when(pkRs.getString("column_name")).thenReturn("id");

    // Mock index query
    when(stmt.executeQuery(
            "SELECT i.relname AS index_name, "
                + "a.attname AS column_name, "
                + "ix.indisunique AS is_unique, "
                + "ix.indisprimary AS is_primary "
                + "FROM pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n "
                + "WHERE t.oid = ix.indrelid "
                + "  AND i.oid = ix.indexrelid "
                + "  AND a.attrelid = t.oid "
                + "  AND a.attnum = ANY(ix.indkey) "
                + "  AND t.relnamespace = n.oid "
                + "  AND n.nspname = 'public' "
                + "  AND t.relkind = 'r' "
                + "  AND t.relname = 'users' "
                + "  AND ix.indisprimary = false "
                + "ORDER BY t.relname, i.relname, a.attnum"))
        .thenReturn(idxRs);
    when(idxRs.next()).thenReturn(false);

    // Mock fk query
    when(stmt.executeQuery(
            "SELECT tc.constraint_name, tc.table_name, kcu.column_name, "
                + "ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name "
                + "FROM information_schema.table_constraints AS tc "
                + "JOIN information_schema.key_column_usage AS kcu "
                + "  ON tc.constraint_name = kcu.constraint_name "
                + "  AND tc.table_schema = kcu.table_schema "
                + "JOIN information_schema.constraint_column_usage AS ccu "
                + "  ON ccu.constraint_name = tc.constraint_name "
                + "  AND ccu.table_schema = tc.table_schema "
                + "WHERE tc.constraint_type = 'FOREIGN KEY' "
                + "  AND tc.table_schema = 'public' AND tc.table_name = 'users' "
                + "ORDER BY tc.constraint_name, kcu.ordinal_position"))
        .thenReturn(fkRs);
    when(fkRs.next()).thenReturn(false);

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(connection, "testdb");
    SourceSchema schema = scanner.scan();

    assertEquals("testdb", schema.databaseName());
    assertEquals(SourceDatabaseType.POSTGRESQL, schema.sourceType());
    assertEquals(1, schema.tables().size());
    SourceTable table = schema.tables().get("users");
    assertEquals("users", table.name());
    assertEquals("public", table.schema());
    assertEquals(1, table.columns().size());
    SourceColumn column = table.columns().get(0);
    assertEquals("id", column.name());
    assertEquals("integer", column.type());
    assertEquals(false, column.isNullable());
    // isPrimaryKey is not set in scanColumns natively in this implementation, it's
    // tracked in the table.
    // wait table.primaryKeyColumns will have it.
    assertEquals(false, column.isGenerated());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("id", table.primaryKeyColumns().get(0));
  }
}
