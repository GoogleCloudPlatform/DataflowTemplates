/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.*;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link PostgreSQLDialectAdapter}. */
@RunWith(MockitoJUnitRunner.class)
public class PostgreSQLDialectAdapterTest {
  @Mock DataSource mockDataSource;
  @Mock Connection mockConnection;

  @Mock PreparedStatement mockPreparedStatement;

  @Mock Statement mockStatement;

  @Mock ResultSet mockResultSet;

  private SourceSchemaReference sourceSchemaReference;
  private PostgreSQLDialectAdapter adapter;

  @Before
  public void setUp() throws Exception {
    sourceSchemaReference = SourceSchemaReference.builder().setDbName("testDB").build();
    adapter = new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT);
  }

  @Test
  public void testDiscoverTablesReturnsSchemaAndTableNames()
      throws SQLException, RetriableSchemaDiscoveryException {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("table1", "table2");
    when(mockResultSet.getString("table_schema")).thenReturn("my_schema", "public");

    assertThat(adapter.discoverTables(mockDataSource, sourceSchemaReference))
        .containsExactly("my_schema.table1", "public.table2");
  }

  @Test
  public void testDiscoverTableSchema() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("column_name")).thenReturn("id", "col");
    when(mockResultSet.getString("data_type")).thenReturn("bigint", "text");
    when(mockResultSet.getLong("character_maximum_length")).thenReturn(0L, 0L);
    when(mockResultSet.getLong("numeric_precision")).thenReturn(64L, 0L);
    when(mockResultSet.getLong("numeric_scale")).thenReturn(0L, 0L);
    when(mockResultSet.wasNull())
        .thenReturn(
            // id column -> character_maximum_length == null, numeric_precision != null,
            // numeric_scale == null
            true,
            false,
            true,
            // col column -> character_maximum_length == null, numeric_precision == null,
            // numeric_scale == null
            true,
            true,
            true);

    assertThat(adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, tables))
        .containsExactly(
            "my_schema.table1",
            ImmutableMap.of(
                "id", new SourceColumnType("bigint", new Long[] {64L}, null),
                "col", new SourceColumnType("text", new Long[] {}, null)));
  }

  @Test
  public void testDiscoverTableIndexes() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getString("column_name")).thenReturn("id", "col1", "col2", "col3");
    when(mockResultSet.getString("index_name"))
        .thenReturn("table_pkey", "table_uniq", "table_index", "table_index");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true, true, false, false);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true, false, false, false);
    when(mockResultSet.getLong("cardinality")).thenReturn(1L, 1L, 2L, 2L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L, 1L, 1L, 2L);
    when(mockResultSet.getString("type_category")).thenReturn("N", "S", "N", "D");

    assertThat(adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables))
        .containsExactly(
            "my_schema.table1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("id")
                    .setIndexName("table_pkey")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.NUMERIC)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col1")
                    .setIndexName("table_uniq")
                    .setIsUnique(true)
                    .setIsPrimary(false)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.OTHER)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col2")
                    .setIndexName("table_index")
                    .setIsUnique(false)
                    .setIsPrimary(false)
                    .setCardinality(2L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.NUMERIC)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col3")
                    .setIndexName("table_index")
                    .setIsUnique(false)
                    .setIsPrimary(false)
                    .setCardinality(2L)
                    .setOrdinalPosition(2L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.DATE_TIME)
                    .build()));
  }

  @Test
  public void testReadQuery() {
    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of()))
        .isEqualTo("SELECT * FROM my_schema.table1");
    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of("col1", "col2")))
        .isEqualTo(
            "SELECT * FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
  }

  @Test
  public void testCountQuery() {
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of(), 1000L))
        .isEqualTo("SET statement_timeout = 1000; SELECT COUNT(*) FROM my_schema.table1");
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SET statement_timeout = 1000; SELECT COUNT(*) FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
  }

  @Test
  public void testCheckForTimeout() {
    assertThat(adapter.checkForTimeout(new SQLException("Expected test non-timeout error")))
        .isFalse();
    // SQLState: IO error
    assertThat(
            adapter.checkForTimeout(new SQLException("Expected test non-timeout error", "58030")))
        .isFalse();

    // SQLState: Query cancelled
    assertThat(adapter.checkForTimeout(new SQLException("Expected test timeout error", "57014")))
        .isTrue();
    // SQLState: Lock timeout
    assertThat(adapter.checkForTimeout(new SQLException("Expected test timeout error", "55P03")))
        .isTrue();
  }
}
