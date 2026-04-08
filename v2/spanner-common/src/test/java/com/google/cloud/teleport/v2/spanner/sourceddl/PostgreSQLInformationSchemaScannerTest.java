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
import static org.mockito.Mockito.verify;
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
  @Mock private ResultSet mockForeignKeysResultSet;
  @Mock private ResultSet mockIndexesResultSet;
  @Mock private PreparedStatement mockTablesPstmt;
  @Mock private PreparedStatement mockColumnsPstmt;
  @Mock private PreparedStatement mockPksPstmt;
  @Mock private PreparedStatement mockFksPstmt;
  @Mock private PreparedStatement mockIndexesPstmt;

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
    when(mockColumnsResultSet.next()).thenReturn(true, true, true, false); // 3 columns
    when(mockColumnsResultSet.getString("column_name")).thenReturn("col1", "col2", "col3");
    when(mockColumnsResultSet.getString("data_type")).thenReturn("varchar", "numeric", "ARRAY");
    when(mockColumnsResultSet.getString("element_type")).thenReturn(null, null, "varchar");
    when(mockColumnsResultSet.getString("is_nullable")).thenReturn("NO", "YES", "YES");
    when(mockColumnsResultSet.getString("is_generated")).thenReturn("NEVER", "ALWAYS", "NEVER");
    when(mockColumnsResultSet.getString("character_maximum_length")).thenReturn("255", null, null);
    when(mockColumnsResultSet.getString("numeric_precision")).thenReturn(null, "10", null);
    when(mockColumnsResultSet.getString("numeric_scale")).thenReturn(null, "2", null);

    // 3. Mock Primary Keys
    when(mockPksPstmt.executeQuery()).thenReturn(mockPrimaryKeysResultSet);
    when(mockPrimaryKeysResultSet.next()).thenReturn(true, false);
    when(mockPrimaryKeysResultSet.getString("column_name")).thenReturn("col1");

    // 4. Mock Indexes
    when(mockIndexesPstmt.executeQuery()).thenReturn(mockIndexesResultSet);
    when(mockIndexesResultSet.next()).thenReturn(true, false);
    when(mockIndexesResultSet.getString("index_name")).thenReturn("idx1");
    when(mockIndexesResultSet.getString("column_name")).thenReturn("col2");
    when(mockIndexesResultSet.getBoolean("is_unique")).thenReturn(true);

    // 5. Mock Foreign Keys
    when(mockFksPstmt.executeQuery()).thenReturn(mockForeignKeysResultSet);
    when(mockForeignKeysResultSet.next()).thenReturn(true, false);
    when(mockForeignKeysResultSet.getString("CONSTRAINT_NAME")).thenReturn("fk1");
    when(mockForeignKeysResultSet.getString("COLUMN_NAME")).thenReturn("col3");
    when(mockForeignKeysResultSet.getString("REFERENCED_TABLE_NAME")).thenReturn("table2");
    when(mockForeignKeysResultSet.getString("REF_COLUMN_NAME")).thenReturn("col_ref");

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(mockConnection, "test_db", "public");

    SourceSchema schema = scanner.scan();

    assertEquals("test_db", schema.databaseName());
    assertEquals(1, schema.tables().size());
    SourceTable table1 = schema.table("table1");
    assertEquals("table1", table1.name());
    assertEquals("public", table1.schema());

    // Check Columns
    assertEquals(3, table1.columns().size());
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

    SourceColumn col3 = table1.columns().get(2);
    assertEquals("col3", col3.name());
    assertEquals("varchar[]", col3.type());
    assertTrue(col3.isNullable());
    assertFalse(col3.isGenerated());

    // Check Primary Keys
    assertEquals(1, table1.primaryKeyColumns().size());
    assertEquals("col1", table1.primaryKeyColumns().get(0));

    // Check Indexes
    assertEquals(1, table1.indexes().size());
    SourceIndex idx1 = table1.indexes().get(0);
    assertEquals("idx1", idx1.name());
    assertEquals("table1", idx1.tableName());
    assertTrue(idx1.isUnique());
    assertEquals(1, idx1.columns().size());
    assertEquals("col2", idx1.columns().get(0));

    // Check Foreign Keys
    assertEquals(1, table1.foreignKeys().size());
    SourceForeignKey fk1 = table1.foreignKeys().get(0);
    assertEquals("fk1", fk1.name());
    assertEquals("table1", fk1.tableName());
    assertEquals("table2", fk1.referencedTable());
    assertEquals(1, fk1.keyColumns().size());
    assertEquals("col3", fk1.keyColumns().get(0));
    assertEquals(1, fk1.referencedColumns().size());
    assertEquals("col_ref", fk1.referencedColumns().get(0));
  }

  @Test
  public void testDefaultSchemaIsPublic() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockTablesPstmt);
    when(mockTablesPstmt.executeQuery()).thenReturn(mockTablesResultSet);
    when(mockTablesResultSet.next()).thenReturn(false);

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(mockConnection, "test_db", null);

    scanner.scan();

    verify(mockTablesPstmt).setString(1, "public");
  }

  @Test
  public void testCustomSchemaIsUsed() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockTablesPstmt);
    when(mockTablesPstmt.executeQuery()).thenReturn(mockTablesResultSet);
    when(mockTablesResultSet.next()).thenReturn(false);

    PostgreSQLInformationSchemaScanner scanner =
        new PostgreSQLInformationSchemaScanner(mockConnection, "test_db", "custom_schema");

    scanner.scan();

    verify(mockTablesPstmt).setString(1, "custom_schema");
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
