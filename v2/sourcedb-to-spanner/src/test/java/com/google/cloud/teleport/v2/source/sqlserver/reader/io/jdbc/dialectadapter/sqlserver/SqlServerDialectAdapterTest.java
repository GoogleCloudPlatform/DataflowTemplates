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
package com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.dialectadapter.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SqlServerDialectAdapter}. */
@RunWith(MockitoJUnitRunner.class)
public class SqlServerDialectAdapterTest {

  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;

  private JdbcSchemaReference sourceSchemaReference;
  private SqlServerDialectAdapter adapter;

  @Before
  public void setUp() {
    sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").setNamespace("dbo").build();
    adapter = new SqlServerDialectAdapter();
  }

  @Test
  public void testDiscoverTablesSuccess() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1)).thenReturn("table1", "table2");

    ImmutableList<String> tables = adapter.discoverTables(mockDataSource, sourceSchemaReference);
    assertThat(tables).containsExactly("table1", "table2").inOrder();
  }

  @Test
  public void testDiscoverTablesThrowsSchemaDiscoveryException() throws Exception {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection error"));

    assertThrows(
        SchemaDiscoveryException.class,
        () -> adapter.discoverTables(mockDataSource, sourceSchemaReference));
  }

  @Test
  public void testDiscoverTableSchemaEmptyTables() throws Exception {
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> schema =
        adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, ImmutableList.of());
    assertThat(schema).isEmpty();
  }

  @Test
  public void testDiscoverTableSchemaSuccess() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    // 4 rows covering: charMaxLen, numPrecision+numScale, numPrecision only, neither
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getString("TABLE_NAME")).thenReturn("table1", "table1", "table1", "table1");
    when(mockResultSet.getString("COLUMN_NAME"))
        .thenReturn("col_varchar", "col_decimal", "col_int", "col_datetime");
    when(mockResultSet.getString("DATA_TYPE")).thenReturn("varchar", "decimal", "int", "datetime");

    when(mockResultSet.getLong("CHARACTER_MAXIMUM_LENGTH")).thenReturn(100L, 0L, 0L, 0L);
    when(mockResultSet.getLong("NUMERIC_PRECISION")).thenReturn(0L, 10L, 10L, 0L);
    when(mockResultSet.getLong("NUMERIC_SCALE")).thenReturn(0L, 2L, 0L, 0L);

    // Sequence of rs.wasNull() calls across the 4 rows:
    // Row 1 (varchar): charMaxLen->false, numPrecision->true, numScale->true
    // Row 2 (decimal): charMaxLen->true, numPrecision->false, numScale->false
    // Row 3 (int): charMaxLen->true, numPrecision->false, numScale->true
    // Row 4 (datetime): charMaxLen->true, numPrecision->true, numScale->true
    when(mockResultSet.wasNull())
        .thenReturn(false, true, true, true, false, false, true, false, true, true, true, true);

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> schema =
        adapter.discoverTableSchema(
            mockDataSource, sourceSchemaReference, ImmutableList.of("table1"));

    assertThat(schema).containsKey("table1");
    ImmutableMap<String, SourceColumnType> cols = schema.get("table1");
    assertEquals(Long.valueOf(100L), cols.get("col_varchar").getMods()[0]);
    assertEquals(Long.valueOf(10L), cols.get("col_decimal").getMods()[0]);
    assertEquals(Long.valueOf(2L), cols.get("col_decimal").getMods()[1]);
    assertEquals(Long.valueOf(10L), cols.get("col_int").getMods()[0]);
    assertEquals(0, cols.get("col_datetime").getMods().length);
  }

  @Test
  public void testDiscoverTableSchemaThrowsSchemaDiscoveryException() throws Exception {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection error"));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            adapter.discoverTableSchema(
                mockDataSource, sourceSchemaReference, ImmutableList.of("table1")));
  }

  @Test
  public void testDiscoverTableIndexesEmptyTables() throws Exception {
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, ImmutableList.of());
    assertThat(indexes).isEmpty();
  }

  @Test
  public void testDiscoverTableIndexesAllTypesAndBitBoundary() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    String[] types = {
      "varchar", "bit", "decimal", "float", "real", "date", "datetime2", "binary", "geography"
    };
    Boolean[] nextReturns = new Boolean[types.length];
    for (int i = 0; i < types.length - 1; i++) {
      nextReturns[i] = true;
    }
    nextReturns[types.length - 1] = false;

    when(mockResultSet.next()).thenReturn(true, nextReturns);
    when(mockResultSet.getString("table_name")).thenReturn("table1");
    when(mockResultSet.getString("type_name"))
        .thenReturn(
            "varchar",
            "bit",
            "decimal",
            "float",
            "real",
            "date",
            "datetime2",
            "binary",
            "geography");
    when(mockResultSet.getString("column_name"))
        .thenReturn(
            "col_str",
            "col_bit",
            "col_bit", // called twice for BIT column (builder + customBoundaryQueryColumnKeys)
            "col_dec",
            "col_flt",
            "col_real",
            "col_date",
            "col_dt",
            "col_bin",
            "col_geo");
    when(mockResultSet.getString("index_name")).thenReturn("pk_idx");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L);

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        adapter.discoverTableIndexes(
            mockDataSource, sourceSchemaReference, ImmutableList.of("table1"));

    assertThat(indexes).containsKey("table1");
    ImmutableList<SourceColumnIndexInfo> infoList = indexes.get("table1");
    assertEquals(9, infoList.size());
    assertEquals(IndexType.STRING, infoList.get(0).indexType());
    assertEquals(IndexType.NUMERIC, infoList.get(1).indexType());
    assertEquals(IndexType.DECIMAL, infoList.get(2).indexType());
    assertEquals(IndexType.DOUBLE, infoList.get(3).indexType());
    assertEquals(IndexType.FLOAT, infoList.get(4).indexType());
    assertEquals(IndexType.DATE, infoList.get(5).indexType());
    assertEquals(IndexType.TIME_STAMP, infoList.get(6).indexType());
    assertEquals(IndexType.BINARY, infoList.get(7).indexType());
    assertEquals(IndexType.OTHER, infoList.get(8).indexType());

    // Verify BIT column triggers custom CAST(... AS BIGINT) in getBoundaryQuery
    String boundaryQueryBit =
        adapter.getBoundaryQuery("table1", ImmutableList.of("col_str"), "col_bit");
    assertThat(boundaryQueryBit).contains("CAST(col_bit AS BIGINT)");

    String boundaryQueryRegular = adapter.getBoundaryQuery("table1", ImmutableList.of(), "col_str");
    assertEquals("SELECT MIN(col_str), MAX(col_str) FROM table1", boundaryQueryRegular);

    String boundaryQueryOtherTable =
        adapter.getBoundaryQuery("other_table", ImmutableList.of(), "other_col");
    assertEquals("SELECT MIN(other_col), MAX(other_col) FROM other_table", boundaryQueryOtherTable);

    String boundaryQueryNull = adapter.getBoundaryQuery(null, ImmutableList.of(), null);
    assertEquals("SELECT MIN(null), MAX(null) FROM null", boundaryQueryNull);
  }

  @Test
  public void testDiscoverTableIndexesThrowsSchemaDiscoveryException() throws Exception {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection error"));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            adapter.discoverTableIndexes(
                mockDataSource, sourceSchemaReference, ImmutableList.of("table1")));
  }

  @Test
  public void testGetReadAndCountQueries() {
    assertEquals("SELECT * FROM table1", adapter.getReadQuery("table1", ImmutableList.of()));
    assertThat(adapter.getReadQuery("table1", ImmutableList.of("id"))).contains("WHERE");

    assertEquals(
        "SELECT COUNT(*) FROM table1", adapter.getCountQuery("table1", ImmutableList.of(), 1000L));
    assertThat(adapter.getCountQuery("table1", ImmutableList.of("id"), 1000L)).contains("WHERE");
  }

  @Test
  public void testCheckForTimeoutAndCollationsOrderQuery() {
    assertFalse(adapter.checkForTimeout(new SQLException()));
    assertNotNull(adapter.getCollationsOrderQuery("UTF8", "Latin1_General_BIN", false));
  }
}
