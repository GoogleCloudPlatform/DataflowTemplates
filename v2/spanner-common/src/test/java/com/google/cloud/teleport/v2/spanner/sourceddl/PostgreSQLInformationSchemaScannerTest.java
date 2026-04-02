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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PostgreSQLInformationSchemaScannerTest {

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockTablesResultSet;
  @Mock private ResultSet mockColumnsResultSet;
  @Mock private ResultSet mockPrimaryKeysResultSet;
  @Mock private ResultSet mockIndexesResultSet;
  @Mock private ResultSet mockForeignKeysResultSet;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
  }

  @Test
  public void testScanSuccessful() throws SQLException {
    // 1. Mock Tables
    when(mockStatement.executeQuery(
            "SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"))
        .thenReturn(mockTablesResultSet);
    when(mockTablesResultSet.next()).thenReturn(true, true, false); // 2 tables, then end
    when(mockTablesResultSet.getString(1)).thenReturn("table1", (String) null);
    when(mockTablesResultSet.getString(2)).thenReturn("public", "public");

    // 2. Mock Columns
    when(mockStatement.executeQuery(
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, is_generated FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'table1' ORDER BY ordinal_position"))
        .thenReturn(mockColumnsResultSet);
    when(mockColumnsResultSet.next()).thenReturn(true, true, false); // 2 columns
    // Column 1
    when(mockColumnsResultSet.getString("column_name")).thenReturn("col1", "col2");
    when(mockColumnsResultSet.getString("data_type")).thenReturn("varchar", "numeric");
    when(mockColumnsResultSet.getString("is_nullable")).thenReturn("NO", "YES");
    when(mockColumnsResultSet.getString("is_generated")).thenReturn("NEVER", "ALWAYS");
    when(mockColumnsResultSet.getString("character_maximum_length")).thenReturn("255", null);
    when(mockColumnsResultSet.getString("numeric_precision")).thenReturn(null, "10");
    when(mockColumnsResultSet.getString("numeric_scale")).thenReturn(null, "2");

    // 3. Mock Primary Keys
    when(mockStatement.executeQuery(
            "SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu   ON tc.constraint_name = kcu.constraint_name   AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = 'public' AND tc.table_name = 'table1' AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position"))
        .thenReturn(mockPrimaryKeysResultSet);
    when(mockPrimaryKeysResultSet.next()).thenReturn(true, false);
    when(mockPrimaryKeysResultSet.getString("column_name")).thenReturn("col1");

    // 4. Mock Indexes
    when(mockStatement.executeQuery(
            "SELECT i.relname AS index_name, a.attname AS column_name, ix.indisunique AS is_unique, ix.indisprimary AS is_primary FROM pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n WHERE t.oid = ix.indrelid   AND i.oid = ix.indexrelid   AND a.attrelid = t.oid   AND a.attnum = ANY(ix.indkey)   AND t.relnamespace = n.oid   AND n.nspname = 'public'   AND t.relkind = 'r'   AND t.relname = 'table1'   AND ix.indisprimary = false ORDER BY t.relname, i.relname, a.attnum"))
        .thenReturn(mockIndexesResultSet);
    when(mockIndexesResultSet.next()).thenReturn(true, true, false);
    when(mockIndexesResultSet.getString("index_name")).thenReturn("idx_col2", "idx_col2");
    when(mockIndexesResultSet.getString("column_name")).thenReturn("col2", "col3");
    when(mockIndexesResultSet.getBoolean("is_unique")).thenReturn(true, true);

    // 5. Mock Foreign Keys
    when(mockStatement.executeQuery(
            "SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu   ON tc.constraint_name = kcu.constraint_name   AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu   ON ccu.constraint_name = tc.constraint_name   AND ccu.table_schema = tc.table_schema WHERE tc.constraint_type = 'FOREIGN KEY'   AND tc.table_schema = 'public' AND tc.table_name = 'table1' ORDER BY tc.constraint_name, kcu.ordinal_position"))
        .thenReturn(mockForeignKeysResultSet);
    when(mockForeignKeysResultSet.next()).thenReturn(true, false);
    when(mockForeignKeysResultSet.getString("constraint_name")).thenReturn("fk_col2");
    when(mockForeignKeysResultSet.getString("column_name")).thenReturn("col2");
    when(mockForeignKeysResultSet.getString("referenced_table_name")).thenReturn("table2");
    when(mockForeignKeysResultSet.getString("referenced_column_name")).thenReturn("ref_col2");

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(mockConnection, "test_db", "public");

    SourceSchema schema = scanner.scan();

    assertEquals("test_db", schema.databaseName());
    assertEquals(1, schema.tables().size());
    SourceTable table1 = schema.table("table1");
    assertEquals("table1", table1.name());
    assertEquals("public", table1.schema());

    // Check Columns
    assertEquals(2, table1.columns().size());
    SourceColumn col1 = table1.columns().get(0);
    assertEquals("col1", col1.name());
    assertEquals("varchar", col1.type());
    assertEquals(255L, (long) col1.size());
    assertFalse(col1.isNullable());
    assertFalse(col1.isGenerated());

    SourceColumn col2 = table1.columns().get(1);
    assertEquals("col2", col2.name());
    assertEquals("numeric", col2.type());
    assertEquals(10, (int) col2.precision());
    assertEquals(2, (int) col2.scale());
    assertTrue(col2.isNullable());
    assertTrue(col2.isGenerated());

    // Check Primary Keys
    assertEquals(1, table1.primaryKeyColumns().size());
    assertEquals("col1", table1.primaryKeyColumns().get(0));

    // Check Indexes
    assertEquals(1, table1.indexes().size());
    SourceIndex index = table1.indexes().get(0);
    assertEquals("idx_col2", index.name());
    assertTrue(index.isUnique());
    assertEquals(2, index.columns().size());
    assertEquals("col2", index.columns().get(0));
    assertEquals("col3", index.columns().get(1));

    // Check Foreign Keys
    assertEquals(1, table1.foreignKeys().size());
    SourceForeignKey fk = table1.foreignKeys().get(0);
    assertEquals("fk_col2", fk.name());
    assertEquals("table1", fk.tableName());
    assertEquals("table2", fk.referencedTable());
    assertEquals(1, fk.keyColumns().size());
    assertEquals("col2", fk.keyColumns().get(0));
    assertEquals(1, fk.referencedColumns().size());
    assertEquals("ref_col2", fk.referencedColumns().get(0));
  }

  @Test
  public void testScanSQLExceptionWrappedRuntimeException() throws SQLException {
    when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("Query failed"));

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(
            mockConnection, "test_db", null); // Tests fallback to "public"

    RuntimeException ex = assertThrows(RuntimeException.class, scanner::scan);
    assertTrue(ex.getMessage().contains("Error scanning database schema"));
    assertTrue(ex.getCause() instanceof SQLException);
  }
}
