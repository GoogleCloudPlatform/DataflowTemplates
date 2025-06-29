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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;

public class CassandraInformationSchemaScannerTest {

  @Test
  public void testScanSingleTable() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement tableStmt = mock(PreparedStatement.class);
    BoundStatement tableBoundStmt = mock(BoundStatement.class);
    ResultSet tableResult = mock(ResultSet.class);
    Row tableRow = mock(Row.class);
    Iterator<Row> tableIterator = Collections.singletonList(tableRow).iterator();

    PreparedStatement columnStmt = mock(PreparedStatement.class);
    BoundStatement columnBoundStmt = mock(BoundStatement.class);
    ResultSet columnResult = mock(ResultSet.class);
    Row columnRow = mock(Row.class);
    Iterator<Row> columnIterator = Collections.singletonList(columnRow).iterator();

    // Mock table query
    when(session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"))
        .thenReturn(tableStmt);
    when(tableStmt.bind("ks1")).thenReturn(tableBoundStmt);
    when(session.execute(tableBoundStmt)).thenReturn(tableResult);
    when(tableResult.iterator()).thenReturn(tableIterator);
    when(tableRow.getString("table_name")).thenReturn("users");

    // Mock column query
    when(session.prepare(
            "SELECT column_name, type, kind FROM system_schema.columns "
                + "WHERE keyspace_name = ? AND table_name = ?"))
        .thenReturn(columnStmt);
    when(columnStmt.bind("ks1", "users")).thenReturn(columnBoundStmt);
    when(session.execute(columnBoundStmt)).thenReturn(columnResult);
    when(columnResult.iterator()).thenReturn(columnIterator);
    when(columnRow.getString("column_name")).thenReturn("id");
    when(columnRow.getString("type")).thenReturn("int");
    when(columnRow.getString("kind")).thenReturn("partition_key");

    CassandraInformationSchemaScanner scanner =
        new CassandraInformationSchemaScanner(session, "ks1");
    SourceSchema schema = scanner.scan();

    assertEquals("ks1", schema.databaseName());
    assertEquals(SourceDatabaseType.CASSANDRA, schema.sourceType());
    assertEquals(1, schema.tables().size());
    SourceTable table = schema.tables().get("users");
    assertEquals("users", table.name());
    assertEquals("ks1", table.schema());
    assertEquals(1, table.columns().size());
    SourceColumn column = table.columns().get(0);
    assertEquals("id", column.name());
    assertEquals("INT", column.type());
    assertEquals(true, column.isNullable());
    assertEquals(true, column.isPrimaryKey());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("id", table.primaryKeyColumns().get(0));
  }
}
