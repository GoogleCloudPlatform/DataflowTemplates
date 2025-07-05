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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.Test;

public class AbstractSourceInformationSchemaScannerTest {

  // Concrete test implementation of the abstract class
  private static class TestScanner extends AbstractSourceInformationSchemaScanner {
    public TestScanner(Connection connection, String databaseName, SourceDatabaseType sourceType) {
      super(connection, databaseName, sourceType);
    }

    @Override
    protected String getTablesQuery() {
      return "SELECT table_name, table_schema FROM test_tables";
    }

    @Override
    protected List<SourceColumn> scanColumns(String tableName, String schema) throws SQLException {
      return List.of(
          SourceColumn.builder(sourceType)
              .name("id")
              .type("INT")
              .isNullable(false)
              .isPrimaryKey(true)
              .columnOptions(ImmutableList.of())
              .build());
    }

    @Override
    protected List<String> scanPrimaryKeys(String tableName, String schema) throws SQLException {
      return List.of("id");
    }
  }

  @Test
  public void testScanSuccess() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery("SELECT table_name, table_schema FROM test_tables")).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn("users");
    when(rs.getString(2)).thenReturn("public");

    TestScanner scanner = new TestScanner(connection, "testdb", SourceDatabaseType.MYSQL);
    SourceSchema schema = scanner.scan();

    assertEquals("testdb", schema.databaseName());
    assertEquals(SourceDatabaseType.MYSQL, schema.sourceType());
    assertEquals(1, schema.tables().size());
    SourceTable table = schema.tables().get("users");
    assertEquals("users", table.name());
    assertEquals("public", table.schema());
    assertEquals(1, table.columns().size());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("id", table.primaryKeyColumns().get(0));
  }

  @Test
  public void testScanWithSQLException() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery("SELECT table_name, table_schema FROM test_tables"))
        .thenThrow(new SQLException("Database error"));

    TestScanner scanner = new TestScanner(connection, "testdb", SourceDatabaseType.MYSQL);

    RuntimeException exception = assertThrows(RuntimeException.class, () -> scanner.scan());
    assertEquals("Error scanning database schema", exception.getMessage());
    assertEquals(SQLException.class, exception.getCause().getClass());
  }

  @Test
  public void testScanMultipleTables() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);

    when(connection.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery("SELECT table_name, table_schema FROM test_tables")).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString(1)).thenReturn("users", "orders");
    when(rs.getString(2)).thenReturn("public", "public");

    TestScanner scanner = new TestScanner(connection, "testdb", SourceDatabaseType.POSTGRESQL);
    SourceSchema schema = scanner.scan();

    assertEquals(2, schema.tables().size());
    assertEquals("users", schema.tables().get("users").name());
    assertEquals("orders", schema.tables().get("orders").name());
  }
}
