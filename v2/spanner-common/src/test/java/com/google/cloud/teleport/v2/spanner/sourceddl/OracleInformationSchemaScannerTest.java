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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class OracleInformationSchemaScannerTest {

  @Mock private Connection mockConnection;
  @Mock private DatabaseMetaData mockMetaData;
  @Mock private ResultSet mockTablesRs;
  @Mock private ResultSet mockColsRs;
  @Mock private ResultSet mockPkRs;
  @Mock private ResultSet mockIdxRs;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");
  }

  @Test
  public void testScanSuccessful() throws SQLException {
    when(mockMetaData.getTables(any(), eq("C##DATASTREAM"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("BIN$recycle", "Users");

    when(mockMetaData.getColumns(any(), eq("C##DATASTREAM"), eq("Users"), eq("%")))
        .thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("UserId", "Name", "FullName");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("NUMBER", "VARCHAR2", "VARCHAR2");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "YES", "YES");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO", "YES");
    when(mockColsRs.getInt("COLUMN_SIZE")).thenReturn(38, 10, 2000);
    when(mockColsRs.getInt("DECIMAL_DIGITS")).thenReturn(0, 0, 0);
    when(mockColsRs.wasNull()).thenReturn(false);

    when(mockMetaData.getPrimaryKeys(any(), eq("C##DATASTREAM"), eq("Users"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, false);
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("UserId");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(1);

    OracleInformationSchemaScanner scanner = new OracleInformationSchemaScanner(mockConnection);
    SourceSchema schema = scanner.scan();

    assertNotNull(schema);
    assertEquals("ORACLE", schema.databaseName());
    assertEquals(SourceDatabaseType.ORACLE, schema.sourceType());
    assertEquals(1, schema.tables().size());

    SourceTable table = schema.table("Users");
    assertNotNull(table);
    assertEquals("Users", table.name());
    assertEquals("C##DATASTREAM", table.schema());
    assertEquals(3, table.columns().size());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("UserId", table.primaryKeyColumns().get(0));

    SourceColumn col1 = table.columns().get(0);
    assertEquals("UserId", col1.name());
    assertEquals("NUMBER", col1.type());
    assertFalse(col1.isNullable());
    assertFalse(col1.isGenerated());
    assertTrue(col1.isPrimaryKey());
    assertEquals(Integer.valueOf(38), col1.precision());
    assertEquals(Integer.valueOf(0), col1.scale());

    SourceColumn col2 = table.columns().get(1);
    assertEquals("Name", col2.name());
    assertEquals("VARCHAR2", col2.type());
    assertTrue(col2.isNullable());
    assertFalse(col2.isGenerated());
    assertFalse(col2.isPrimaryKey());
    assertEquals(Long.valueOf(10L), col2.size());

    SourceColumn col3 = table.columns().get(2);
    assertEquals("FullName", col3.name());
    assertEquals("VARCHAR2", col3.type());
    assertTrue(col3.isNullable());
    assertTrue(col3.isGenerated());
    assertFalse(col3.isPrimaryKey());
    assertEquals(Long.valueOf(2000L), col3.size());
  }

  @Test
  public void testScanLowercaseSchemaTableWildcardIdentityAndIndexes() throws SQLException {
    when(mockMetaData.getTables(any(), eq("C##DATASTREAM"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("T_1");

    when(mockMetaData.getPrimaryKeys(any(), eq("C##DATASTREAM"), eq("T_1"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, false);
    when(mockPkRs.getString("PK_NAME")).thenReturn("PK_T_1");
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("ID");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(1);

    when(mockMetaData.getColumns(any(), eq("C##DATASTREAM"), eq("T_1"), eq("%")))
        .thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("TABLE_NAME")).thenReturn("T_1", "T11", "T_1");
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("ID", "SEQ_COL");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("NUMBER", "NUMBER");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "NO");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO");
    when(mockColsRs.getString("IS_AUTOINCREMENT")).thenReturn("NO", "YES");

    when(mockMetaData.getIndexInfo(any(), eq("C##DATASTREAM"), eq("T_1"), eq(false), eq(true)))
        .thenReturn(mockIdxRs);
    when(mockIdxRs.next()).thenReturn(true, true, false);
    when(mockIdxRs.getString("INDEX_NAME")).thenReturn("PK_T_1", "IDX_NON_UNIQUE");
    when(mockIdxRs.getString("COLUMN_NAME")).thenReturn("ID", "SEQ_COL");
    when(mockIdxRs.getBoolean("NON_UNIQUE")).thenReturn(false, true);

    OracleInformationSchemaScanner scanner =
        new OracleInformationSchemaScanner(mockConnection, "c##datastream");
    SourceSchema schema = scanner.scan();

    SourceTable table = schema.table("T_1");
    assertNotNull(table);
    assertEquals(2, table.columns().size());
    assertEquals("ID", table.columns().get(0).name());
    assertFalse(table.columns().get(0).isGenerated());
    assertEquals("SEQ_COL", table.columns().get(1).name());
    assertTrue(table.columns().get(1).isGenerated());

    assertEquals(2, table.indexes().size());
    assertTrue(table.indexes().get(0).isPrimary());
    assertTrue(table.indexes().get(0).isUnique());
    assertFalse(table.indexes().get(1).isPrimary());
    assertFalse(table.indexes().get(1).isUnique());
  }

  @Test
  public void testScanPrimaryKeysOutOfOrder() throws SQLException {
    when(mockMetaData.getTables(any(), eq("C##DATASTREAM"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("Orders");

    when(mockMetaData.getColumns(any(), eq("C##DATASTREAM"), eq("Orders"), eq("%")))
        .thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("ItemId", "TenantId", "OrderId");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("NUMBER", "NUMBER", "NUMBER");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "NO", "NO");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO", "NO");

    when(mockMetaData.getPrimaryKeys(any(), eq("C##DATASTREAM"), eq("Orders")))
        .thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, true, true, false);
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("ItemId", "TenantId", "OrderId");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(3, 1, 2);

    OracleInformationSchemaScanner scanner =
        new OracleInformationSchemaScanner(mockConnection, "C##DATASTREAM");
    SourceSchema schema = scanner.scan();

    SourceTable table = schema.table("Orders");
    assertNotNull(table);
    assertEquals(List.of("TenantId", "OrderId", "ItemId"), table.primaryKeyColumns());
    for (SourceColumn col : table.columns()) {
      assertTrue(col.isPrimaryKey());
    }
  }

  @Test
  public void testScanSQLExceptionThrowsRuntimeException() throws SQLException {
    when(mockMetaData.getTables(any(), any(), any(), any()))
        .thenThrow(new SQLException("Database connection error"));

    OracleInformationSchemaScanner scanner = new OracleInformationSchemaScanner(mockConnection);
    assertThrows(RuntimeException.class, scanner::scan);
  }
}
