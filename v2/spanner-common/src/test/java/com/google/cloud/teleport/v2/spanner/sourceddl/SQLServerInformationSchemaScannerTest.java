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

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
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

    SourceColumn col2 = table.columns().get(1);
    assertEquals("Name", col2.name());
    assertEquals("nvarchar", col2.type());
    assertTrue(col2.isNullable());
    assertFalse(col2.isGenerated());

    SourceColumn col3 = table.columns().get(2);
    assertEquals("FullName", col3.name());
    assertEquals("nvarchar", col3.type());
    assertTrue(col3.isNullable());
    assertTrue(col3.isGenerated());
  }

  @Test
  public void testScanIgnoresInternalTables() throws SQLException {
    when(mockMetaData.getTables(any(), eq("dbo"), eq("%"), eq(new String[] {"TABLE"})))
        .thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(true, true, false);
    when(mockTablesRs.getString("TABLE_NAME")).thenReturn("trace_xe_action_map", "spt_values");

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb");
    SourceSchema schema = scanner.scan();

    assertNotNull(schema);
    assertEquals(0, schema.tables().size());
  }

  @Test
  public void testScanSQLExceptionThrowsRuntimeException() throws SQLException {
    when(mockMetaData.getTables(any(), any(), any(), any()))
        .thenThrow(new SQLException("Database connection error"));

    SQLServerInformationSchemaScanner scanner =
        new SQLServerInformationSchemaScanner(mockConnection, "testdb");
    assertThrows(RuntimeException.class, scanner::scan);
  }
}
