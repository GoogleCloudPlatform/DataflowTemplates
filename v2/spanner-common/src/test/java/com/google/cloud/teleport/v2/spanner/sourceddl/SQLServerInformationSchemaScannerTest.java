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
import static org.junit.Assert.assertNull;
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
public class SQLServerInformationSchemaScannerTest {

  @Mock private Connection mockConnection;
  @Mock private DatabaseMetaData mockMetaData;
  @Mock private ResultSet mockTablesRs;
  @Mock private ResultSet mockColsRs;
  @Mock private ResultSet mockPkRs;
  @Mock private ResultSet mockFkRs;
  @Mock private ResultSet mockIdxRs;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockConnection.getSchema()).thenReturn("dbo");
  }

  @Test
  public void testScanSuccessful() throws SQLException {
    when(mockMetaData.getTables(any(), eq("dbo"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("Users");

    when(mockMetaData.getColumns(any(), eq("dbo"), eq("Users"), eq("%"))).thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("UserId", "Name", "FullName");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("bigint", "nvarchar", "nvarchar");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "YES", "YES");
    when(mockColsRs.getString("IS_AUTOINCREMENT")).thenReturn("YES", "NO", "NO");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO", "YES");

    when(mockMetaData.getPrimaryKeys(any(), eq("dbo"), eq("Users"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, false);
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("UserId");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(1);

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb");
    SourceSchema schema = scanner.scan();

    assertNotNull(schema);
    assertEquals("testdb", schema.databaseName());
    assertEquals(SourceDatabaseType.SQLSERVER, schema.sourceType());
    assertEquals(1, schema.tables().size());

    SourceTable table = schema.table("Users");
    assertNotNull(table);
    assertEquals("Users", table.name());
    assertEquals("dbo", table.schema());
    assertEquals(3, table.columns().size());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("UserId", table.primaryKeyColumns().get(0));

    SourceColumn col1 = table.columns().get(0);
    assertEquals("UserId", col1.name());
    assertEquals("bigint", col1.type());
    assertFalse(col1.isNullable());
    assertFalse(col1.isGenerated());
    assertTrue(col1.isPrimaryKey());

    SourceColumn col2 = table.columns().get(1);
    assertEquals("Name", col2.name());
    assertEquals("nvarchar", col2.type());
    assertTrue(col2.isNullable());
    assertFalse(col2.isGenerated());
    assertFalse(col2.isPrimaryKey());

    SourceColumn col3 = table.columns().get(2);
    assertEquals("FullName", col3.name());
    assertEquals("nvarchar", col3.type());
    assertTrue(col3.isNullable());
    assertTrue(col3.isGenerated());
    assertFalse(col3.isPrimaryKey());
  }

  @Test
  public void testScanPrimaryKeysOutOfOrder() throws SQLException {
    when(mockMetaData.getTables(any(), eq("dbo"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("Orders");

    when(mockMetaData.getColumns(any(), eq("dbo"), eq("Orders"), eq("%"))).thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("ItemId", "TenantId", "OrderId");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("int", "int", "int");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "NO", "NO");
    when(mockColsRs.getString("IS_AUTOINCREMENT")).thenReturn("NO", "NO", "NO");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO", "NO");

    when(mockMetaData.getPrimaryKeys(any(), eq("dbo"), eq("Orders"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, true, true, false);
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("ItemId", "TenantId", "OrderId");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(3, 1, 2);

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb");
    SourceSchema schema = scanner.scan();

    assertNotNull(schema);
    SourceTable table = schema.table("Orders");
    assertNotNull(table);
    assertEquals(3, table.primaryKeyColumns().size());
    assertEquals("TenantId", table.primaryKeyColumns().get(0));
    assertEquals("OrderId", table.primaryKeyColumns().get(1));
    assertEquals("ItemId", table.primaryKeyColumns().get(2));
    assertEquals(List.of("TenantId", "OrderId", "ItemId"), table.primaryKeyColumns());

    for (SourceColumn col : table.columns()) {
      assertTrue(col.isPrimaryKey());
    }
  }

  @Test
  public void testScanSQLExceptionThrowsRuntimeException() throws SQLException {
    when(mockMetaData.getTables(any(), any(), any(), any()))
        .thenThrow(new SQLException("Database connection error"));

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb");
    assertThrows(RuntimeException.class, scanner::scan);
  }

  @Test
  public void testScanSizesForeignKeysAndUniqueIndexes() throws SQLException {
    when(mockMetaData.getTables(any(), eq("sales"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("Orders");

    when(mockMetaData.getColumns(any(), eq("sales"), eq("Orders"), eq("%"))).thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(true, true, true, false);
    when(mockColsRs.getString("COLUMN_NAME")).thenReturn("Id", "Amount", "Code");
    when(mockColsRs.getString("TYPE_NAME")).thenReturn("int identity", "decimal", "nvarchar");
    when(mockColsRs.getString("IS_NULLABLE")).thenReturn("NO", "YES", "YES");
    when(mockColsRs.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "NO", "NO");
    when(mockColsRs.getInt("COLUMN_SIZE")).thenReturn(10, 12, 40);
    when(mockColsRs.getInt("DECIMAL_DIGITS")).thenReturn(0, 3, 0);
    // COLUMN_SIZE, DECIMAL_DIGITS per column: (10, 0), (12, 3), (40, NULL).
    when(mockColsRs.wasNull()).thenReturn(false, false, false, false, false, true);

    when(mockMetaData.getPrimaryKeys(any(), eq("sales"), eq("Orders"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(true, false);
    when(mockPkRs.getString("COLUMN_NAME")).thenReturn("Id");
    when(mockPkRs.getInt("KEY_SEQ")).thenReturn(1);
    when(mockPkRs.getString("PK_NAME")).thenReturn("PK_Orders");

    when(mockMetaData.getImportedKeys(any(), eq("sales"), eq("Orders"))).thenReturn(mockFkRs);
    when(mockFkRs.next()).thenReturn(true, true, false);
    when(mockFkRs.getString("FK_NAME")).thenReturn("fk_cust", "fk_cust");
    when(mockFkRs.getInt("KEY_SEQ")).thenReturn(2, 1);
    when(mockFkRs.getString("PKTABLE_NAME")).thenReturn("Customers", "Customers");
    when(mockFkRs.getString("FKCOLUMN_NAME")).thenReturn("Code", "Id");
    when(mockFkRs.getString("PKCOLUMN_NAME")).thenReturn("CustCode", "CustId");

    when(mockMetaData.getIndexInfo(any(), eq("sales"), eq("Orders"), eq(true), eq(false)))
        .thenReturn(mockIdxRs);
    when(mockIdxRs.next()).thenReturn(true, true, true, false);
    when(mockIdxRs.getShort("TYPE"))
        .thenReturn(
            DatabaseMetaData.tableIndexStatistic,
            DatabaseMetaData.tableIndexClustered,
            DatabaseMetaData.tableIndexOther);
    when(mockIdxRs.getString("INDEX_NAME")).thenReturn("PK_Orders", "uq_code");
    when(mockIdxRs.getString("COLUMN_NAME")).thenReturn("Id", "Code");
    when(mockIdxRs.getShort("ORDINAL_POSITION")).thenReturn((short) 1, (short) 1);

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb", "sales");
    SourceTable table = scanner.scan().table("Orders");

    assertEquals("sales", table.schema());
    SourceColumn id = table.columns().get(0);
    assertEquals("int identity", id.type());
    assertEquals(Long.valueOf(10), id.size());
    SourceColumn amount = table.columns().get(1);
    assertEquals(Long.valueOf(12), amount.size());
    assertEquals(Integer.valueOf(12), amount.precision());
    assertEquals(Integer.valueOf(3), amount.scale());
    SourceColumn code = table.columns().get(2);
    assertEquals(Long.valueOf(40), code.size());
    assertNull(code.precision());
    assertNull(code.scale());

    assertEquals(1, table.foreignKeys().size());
    SourceForeignKey fk = table.foreignKeys().get(0);
    assertEquals("fk_cust", fk.name());
    assertEquals("Customers", fk.referencedTable());
    assertEquals(List.of("Id", "Code"), fk.keyColumns());
    assertEquals(List.of("CustId", "CustCode"), fk.referencedColumns());

    assertEquals(2, table.indexes().size());
    SourceIndex pk = table.indexes().get(0);
    assertEquals("PK_Orders", pk.name());
    assertTrue(pk.isPrimary());
    SourceIndex unique = table.indexes().get(1);
    assertEquals("uq_code", unique.name());
    assertTrue(unique.isUnique());
    assertFalse(unique.isPrimary());
    assertEquals(List.of("Code"), unique.columns());
  }

  @Test
  public void testScanBlankSchemaUsesConnectionSchemaAndNullKeyResultSets() throws SQLException {
    when(mockMetaData.getTables(any(), eq("dbo"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("T");
    when(mockMetaData.getColumns(any(), eq("dbo"), eq("T"), eq("%"))).thenReturn(mockColsRs);
    when(mockColsRs.next()).thenReturn(false);
    when(mockMetaData.getPrimaryKeys(any(), eq("dbo"), eq("T"))).thenReturn(mockPkRs);
    when(mockPkRs.next()).thenReturn(false);

    SourceTable table =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb", "  ").scan().table("T");
    assertNotNull(table);
    assertEquals("dbo", table.schema());
    assertTrue(table.foreignKeys().isEmpty());
    assertTrue(table.indexes().isEmpty());
  }
}
