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
import java.sql.PreparedStatement;
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
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockTablesResultSet;
  @Mock private ResultSet mockColumnsResultSet;
  @Mock private ResultSet mockPrimaryKeysResultSet;
  @Mock private ResultSet mockIndexesResultSet;
  @Mock private ResultSet mockForeignKeysResultSet;
  @Mock private PreparedStatement mockTablesPstmt;
  @Mock private PreparedStatement mockColumnsPstmt;
  @Mock private PreparedStatement mockPksPstmt;
  @Mock private PreparedStatement mockIndexesPstmt;
  @Mock private PreparedStatement mockFksPstmt;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
  }

  @Test
  public void testScanSuccessful() throws SQLException {
    // Setup consecutive returns for prepareStatement
    when(mockConnection.prepareStatement(anyString()))
        .thenReturn(
            mockTablesPstmt, mockColumnsPstmt, mockPksPstmt, mockIndexesPstmt, mockFksPstmt);

    // 1. Mock Tables
    when(mockTablesPstmt.executeQuery()).thenReturn(mockTablesResultSet);
    when(mockTablesResultSet.next()).thenReturn(true, true, false); // 2 tables, then end
    when(mockTablesResultSet.getString(1)).thenReturn("table1", (String) null);
    when(mockTablesResultSet.getString(2)).thenReturn("public", "public");

    // 2. Mock Columns
    when(mockColumnsPstmt.executeQuery()).thenReturn(mockColumnsResultSet);
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
    when(mockPksPstmt.executeQuery()).thenReturn(mockPrimaryKeysResultSet);
    when(mockPrimaryKeysResultSet.next()).thenReturn(true, false);
    when(mockPrimaryKeysResultSet.getString("column_name")).thenReturn("col1");

    // 4. Mock Indexes
    when(mockIndexesPstmt.executeQuery()).thenReturn(mockIndexesResultSet);
    when(mockIndexesResultSet.next()).thenReturn(true, true, false);
    when(mockIndexesResultSet.getString("index_name")).thenReturn("idx_col2", "idx_col2");
    when(mockIndexesResultSet.getString("column_name")).thenReturn("col2", "col3");
    when(mockIndexesResultSet.getBoolean("is_unique")).thenReturn(true, true);

    // 5. Mock Foreign Keys
    when(mockFksPstmt.executeQuery()).thenReturn(mockForeignKeysResultSet);
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
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Query failed"));

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(mockConnection, "test_db", null);

    RuntimeException ex = assertThrows(RuntimeException.class, scanner::scan);
    assertTrue(ex.getMessage().contains("Error scanning database schema"));
    assertTrue(ex.getCause() instanceof SQLException);
  }
}
