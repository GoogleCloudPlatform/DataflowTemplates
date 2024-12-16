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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
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

  private JdbcSchemaReference sourceSchemaReference;
  private PostgreSQLDialectAdapter adapter;

  @Before
  public void setUp() throws Exception {
    sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").setNamespace("public").build();
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

    assertThat(adapter.discoverTables(mockDataSource, sourceSchemaReference))
        .containsExactly("table1", "table2");
  }

  @Test
  public void testDiscoverTableExceptions() throws SQLException {
    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"))
        .thenThrow(new SQLException("test"));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));
  }

  @Test
  public void testDiscoverTableSchema() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getString("column_name")).thenReturn("id", "col1", "col2", "col3");
    when(mockResultSet.getString("data_type")).thenReturn("bigint", "text", "varchar", "numeric");
    when(mockResultSet.getLong("character_maximum_length")).thenReturn(0L, 0L, 100L, 0L);
    when(mockResultSet.getLong("numeric_precision")).thenReturn(64L, 0L, 0L, 100L);
    when(mockResultSet.getLong("numeric_scale")).thenReturn(0L, 0L, 0L, 200L);
    when(mockResultSet.wasNull())
        .thenReturn(
            // id column -> character_maximum_length == null, numeric_precision != null,
            // numeric_scale == null
            true,
            false,
            true,
            // col1 column -> character_maximum_length == null, numeric_precision == null,
            // numeric_scale == null
            true,
            true,
            true,
            // col2 column -> character_maximum_length != null, numeric_precision == null,
            // numeric_scale == null
            false,
            true,
            true,
            // col3 column -> character_maximum_length == null, numeric_precision != null,
            // numeric_scale != null
            true,
            false,
            false);

    assertThat(adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, tables))
        .containsExactly(
            "my_schema.table1",
            ImmutableMap.of(
                "id", new SourceColumnType("bigint", new Long[] {64L}, null),
                "col1", new SourceColumnType("text", new Long[] {}, null),
                "col2", new SourceColumnType("varchar", new Long[] {100L}, null),
                "col3", new SourceColumnType("numeric", new Long[] {100L, 200L}, null)));
  }

  @Test
  public void testDiscoverTableSchemaExceptions() throws SQLException {
    final String testTable = "testTable";
    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"))
        .thenThrow(new SQLException("test"))
        .thenThrow(new SchemaDiscoveryException(new RuntimeException("test")));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
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
    when(mockResultSet.getString("type_category")).thenReturn("N", "S", "S", "D");
    when(mockResultSet.getString("collation")).thenReturn(null, "en_US", "en_US", null);
    when(mockResultSet.getInt("type_length")).thenReturn(100, 0);
    when(mockResultSet.wasNull()).thenReturn(false, true);
    when(mockResultSet.getString("type_name")).thenReturn("char", "text");
    when(mockResultSet.getString("charset")).thenReturn("UTF8", "UTF8");

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
                    .setIndexType(IndexType.STRING)
                    .setCollationReference(
                        CollationReference.builder()
                            .setDbCharacterSet("UTF8")
                            .setDbCollation("en_US")
                            .setPadSpace(true)
                            .build())
                    .setStringMaxLength(100)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col2")
                    .setIndexName("table_index")
                    .setIsUnique(false)
                    .setIsPrimary(false)
                    .setCardinality(2L)
                    .setOrdinalPosition(1L)
                    .setIndexType(IndexType.STRING)
                    .setCollationReference(
                        CollationReference.builder()
                            .setDbCharacterSet("UTF8")
                            .setDbCollation("en_US")
                            .setPadSpace(false)
                            .build())
                    .setStringMaxLength(65535)
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
  public void testDiscoverTableIndexesExceptions() throws SQLException {
    final String testTable = "testTable";
    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"))
        .thenThrow(new SQLException("test"))
        .thenThrow(new SchemaDiscoveryException(new RuntimeException("test")));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testBoundaryQuery() {
    assertThat(adapter.getBoundaryQuery("my_schema.table1", ImmutableList.of(), "id"))
        .isEqualTo("SELECT MIN(id), MAX(id) FROM my_schema.table1");
    assertThat(adapter.getBoundaryQuery("my_schema.table1", ImmutableList.of("col1", "col2"), "id"))
        .isEqualTo(
            "SELECT MIN(id), MAX(id) FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
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

  @Test
  public void testCollationsOrderQueryWithPadSpace() {
    String collationsOrderQuery = adapter.getCollationsOrderQuery("myCharset", "myCollation", true);

    assertThat(collationsOrderQuery).contains("myCharset");
    assertThat(collationsOrderQuery).contains("myCollation");
    assertThat(collationsOrderQuery).contains("CHAR(5)");
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.CHARSET_REPLACEMENT_TAG);
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.COLLATION_REPLACEMENT_TAG);
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.RETURN_TYPE_REPLACEMENT_TAG);
  }

  @Test
  public void testCollationsOrderQueryWithoutPadSpace() {
    String collationsOrderQuery =
        adapter.getCollationsOrderQuery("myCharset", "myCollation", false);

    assertThat(collationsOrderQuery).contains("myCharset");
    assertThat(collationsOrderQuery).contains("myCollation");
    assertThat(collationsOrderQuery).contains("TEXT");
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.CHARSET_REPLACEMENT_TAG);
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.COLLATION_REPLACEMENT_TAG);
    assertThat(collationsOrderQuery).doesNotContain(ResourceUtils.RETURN_TYPE_REPLACEMENT_TAG);
  }
}
