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
package com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.dialectadapter.postgresql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.ResourceUtils;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.LocalTime;
import java.time.OffsetTime;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
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
    PreparedStatement mockParentPreparedStatement = mock(PreparedStatement.class);
    ResultSet mockParentResultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);

    // Default mock for any prepareStatement (used by information_schema query)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    // Specific mock for the pg_inherits query
    when(mockConnection.prepareStatement(contains("pg_inherits")))
        .thenReturn(mockParentPreparedStatement);

    when(mockParentPreparedStatement.executeQuery()).thenReturn(mockParentResultSet);
    when(mockParentResultSet.next()).thenReturn(true, false);
    when(mockParentResultSet.getString("table_name")).thenReturn("parent_table1");

    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("table1", "table2", "parent_table1");

    assertThat(adapter.discoverTables(mockDataSource, sourceSchemaReference))
        .containsExactly("table1", "table2", "parent_table1");
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
  public void testDiscoverTablesThrowsExceptionWhenInheritanceQueryFails() throws SQLException {
    PreparedStatement mockParentPreparedStatement = mock(PreparedStatement.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    // Mock pg_inherits query to throw an exception
    when(mockConnection.prepareStatement(contains("pg_inherits")))
        .thenReturn(mockParentPreparedStatement);
    when(mockParentPreparedStatement.executeQuery())
        .thenThrow(new SQLException("Permission denied"));

    SchemaDiscoveryException exception =
        assertThrows(
            SchemaDiscoveryException.class,
            () ->
                new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT)
                    .discoverTables(mockDataSource, sourceSchemaReference));

    assertThat(exception).hasCauseThat().hasMessageThat().contains("Permission denied");
  }

  @Test
  public void testDiscoverTableSchema() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("my_schema.table1");
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
  public void testDiscoverTableSchemaBulk() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("table1", "table2");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("table1", "table2");
    when(mockResultSet.getString("column_name")).thenReturn("id", "id");
    when(mockResultSet.getString("data_type")).thenReturn("bigint", "bigint");
    when(mockResultSet.wasNull()).thenReturn(true, true, true, true, true, true);

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> result =
        adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, tables);

    assertThat(result).hasSize(2);
    assertThat(result).containsKey("table1");
    assertThat(result).containsKey("table2");
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
    when(mockResultSet.next())
        .thenReturn(
            true, true, true, true, true, true, true, true, true, true, true, true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("my_schema.table1");
    when(mockResultSet.getString("column_name"))
        .thenReturn(
            "col_int2",
            "col_int4",
            "col_int8",
            "col_numeric",
            "col_float4",
            "col_float8",
            "col_varchar",
            "col_text",
            "col_timestamp",
            "col_timestamptz",
            "col_date",
            "col_time",
            "col_timetz");
    when(mockResultSet.getString("index_name"))
        .thenReturn(
            "idx_int2",
            "idx_int4",
            "idx_int8",
            "idx_numeric",
            "idx_float4",
            "idx_float8",
            "idx_varchar",
            "idx_text",
            "idx_timestamp",
            "idx_timestamptz",
            "idx_date",
            "idx_time",
            "idx_timetz");
    when(mockResultSet.getBoolean("is_unique"))
        .thenReturn(true, true, true, true, true, true, true, true, true, true, true, true, true);
    when(mockResultSet.getBoolean("is_primary"))
        .thenReturn(true, true, true, true, true, true, true, true, true, true, true, true, true);
    when(mockResultSet.getLong("cardinality"))
        .thenReturn(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L);
    when(mockResultSet.getLong("ordinal_position"))
        .thenReturn(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L);
    when(mockResultSet.getString("collation"))
        .thenReturn(null, null, null, null, null, null, null, null, null, null, null, null, null);
    when(mockResultSet.getInt("type_length")).thenReturn(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    when(mockResultSet.wasNull())
        .thenReturn(true, true, true, true, true, true, true, true, true, true, true, true, true);
    when(mockResultSet.getString("type_name"))
        .thenReturn(
            "int2",
            "int4",
            "int8",
            "numeric",
            "float4",
            "float8",
            "varchar",
            "text",
            "timestamp",
            "timestamptz",
            "date",
            "time",
            "timetz");
    when(mockResultSet.getString("charset"))
        .thenReturn(null, null, null, null, null, null, null, null, null, null, null, null, null);

    assertThat(adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables))
        .containsExactly(
            "my_schema.table1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_int2")
                    .setIndexName("idx_int2")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.NUMERIC)
                    .setColumnTypeName("int2")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_int4")
                    .setIndexName("idx_int4")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.NUMERIC)
                    .setColumnTypeName("int4")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_int8")
                    .setIndexName("idx_int8")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.NUMERIC)
                    .setColumnTypeName("int8")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_numeric")
                    .setIndexName("idx_numeric")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.DECIMAL)
                    .setColumnTypeName("numeric")
                    .setNumericScale(0)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_float4")
                    .setIndexName("idx_float4")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.FLOAT)
                    .setColumnTypeName("float4")
                    .setDecimalStepSize(new java.math.BigDecimal("0.00001"))
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_float8")
                    .setIndexName("idx_float8")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.DOUBLE)
                    .setColumnTypeName("float8")
                    .setDecimalStepSize(new java.math.BigDecimal("0.0000000001"))
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_varchar")
                    .setIndexName("idx_varchar")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.STRING)
                    .setColumnTypeName("varchar")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_text")
                    .setIndexName("idx_text")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.STRING)
                    .setColumnTypeName("text")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_timestamp")
                    .setIndexName("idx_timestamp")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.TIME_STAMP)
                    .setColumnTypeName("timestamp")
                    .setDatetimePrecision(6)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_timestamptz")
                    .setIndexName("idx_timestamptz")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.TIME_STAMP)
                    .setColumnTypeName("timestamptz")
                    .setDatetimePrecision(6)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_date")
                    .setIndexName("idx_date")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.DATE)
                    .setColumnTypeName("date")
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_time")
                    .setIndexName("idx_time")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.LOCAL_TIME)
                    .setColumnTypeName("time")
                    .setDatetimePrecision(6)
                    .build(),
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_timetz")
                    .setIndexName("idx_timetz")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.OFFSET_TIME)
                    .setColumnTypeName("timetz")
                    .setDatetimePrecision(6)
                    .build()));
  }

  @Test
  public void testDiscoverTableIndexesWithUuid()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("table_name")).thenReturn("my_schema.table1");
    when(mockResultSet.getString("column_name")).thenReturn("col_uuid");
    when(mockResultSet.getString("index_name")).thenReturn("table_uuid_idx");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true);
    when(mockResultSet.getLong("cardinality")).thenReturn(1L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L);
    when(mockResultSet.getString("type_name")).thenReturn("uuid");
    when(mockResultSet.getInt("type_length")).thenReturn(0);
    when(mockResultSet.wasNull()).thenReturn(true);

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables);

    // 1. Assert discovered index info matches expected schema mapping with standard C collation
    assertThat(indexes)
        .containsExactly(
            "my_schema.table1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_uuid")
                    .setIndexName("table_uuid_idx")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.BINARY)
                    .setColumnTypeName("uuid")
                    .build()));

    SourceColumnIndexInfo info = indexes.get("my_schema.table1").get(0);

    // 2. Assert that a PartitionColumn can be built successfully from this index info (precondition
    // check)
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName(info.columnName())
            .setColumnClass(byte[].class)
            .setColumnTypeName(info.columnTypeName())
            .build();

    assertThat(partitionColumn).isNotNull();
    assertThat(partitionColumn.columnClass()).isEqualTo(byte[].class);
    assertThat(partitionColumn.columnTypeName()).isEqualTo("uuid");

    // 3. Assert that getBoundaryQuery correctly wraps this discovered UUID column in optimized
    // subqueries
    assertThat(adapter.getBoundaryQuery("my_schema.table1", ImmutableList.of(), "col_uuid"))
        .isEqualTo(
            "SELECT (SELECT col_uuid FROM my_schema.table1 ORDER BY col_uuid ASC NULLS LAST LIMIT 1), "
                + "(SELECT col_uuid FROM my_schema.table1 ORDER BY col_uuid DESC NULLS LAST LIMIT 1)");

    // 3b. Assert that getBoundaryQuery generates correct partitioned query for UUID column (using
    // CTE with subqueries)
    assertThat(
            adapter.getBoundaryQuery(
                "my_schema.table1", ImmutableList.of("parent_col"), "col_uuid"))
        .isEqualTo(
            "WITH filtered_uuid AS NOT MATERIALIZED (SELECT col_uuid FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (parent_col >= ? AND (parent_col < ? OR (? = TRUE AND parent_col = ?))))) "
                + "SELECT (SELECT col_uuid FROM filtered_uuid ORDER BY col_uuid ASC NULLS LAST LIMIT 1), "
                + "(SELECT col_uuid FROM filtered_uuid ORDER BY col_uuid DESC NULLS LAST LIMIT 1)");

    // 4. Assert that getReadQuery generates the correct query for UUID column
    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of("col_uuid")))
        .isEqualTo(
            "SELECT * FROM my_schema.table1 WHERE ((? = FALSE) OR (col_uuid >= ? AND (col_uuid < ? OR (? = TRUE AND col_uuid = ?))))");

    // 5. Assert that getCountQuery generates the correct query for UUID column
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of("col_uuid"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM my_schema.table1 WHERE ((? = FALSE) OR (col_uuid >= ? AND (col_uuid < ? OR (? = TRUE AND col_uuid = ?))))");
  }

  @Test
  public void testDiscoverTableIndexesWithBit()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("table_name")).thenReturn("my_schema.table1");
    when(mockResultSet.getString("column_name")).thenReturn("col_bit");
    when(mockResultSet.getString("index_name")).thenReturn("table_bit_idx");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true);
    when(mockResultSet.getLong("cardinality")).thenReturn(1L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L);
    when(mockResultSet.getString("type_name")).thenReturn("bit");
    when(mockResultSet.getInt("type_length")).thenReturn(0);
    when(mockResultSet.wasNull()).thenReturn(true);

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables);

    // 1. Assert discovered index info matches expected schema mapping
    assertThat(indexes)
        .containsExactly(
            "my_schema.table1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("col_bit")
                    .setIndexName("table_bit_idx")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setCardinality(1L)
                    .setOrdinalPosition(1L)
                    .setIndexType(SourceColumnIndexInfo.IndexType.BIT)
                    .setColumnTypeName("bit")
                    .build()));

    SourceColumnIndexInfo info = indexes.get("my_schema.table1").get(0);

    // 2. Assert that a PartitionColumn can be built successfully from this index info (precondition
    // check)
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName(info.columnName())
            .setColumnClass(String.class)
            .setColumnTypeName(info.columnTypeName())
            .build();

    assertThat(partitionColumn).isNotNull();
    assertThat(partitionColumn.columnClass()).isEqualTo(String.class);
    assertThat(partitionColumn.columnTypeName()).isEqualTo("bit");

    // 3. Assert that getBoundaryQuery correctly wraps this discovered BIT column in optimized
    // subqueries
    assertThat(adapter.getBoundaryQuery("my_schema.table1", ImmutableList.of(), "col_bit"))
        .isEqualTo(
            "SELECT (SELECT col_bit FROM my_schema.table1 ORDER BY col_bit ASC NULLS LAST LIMIT 1), "
                + "(SELECT col_bit FROM my_schema.table1 ORDER BY col_bit DESC NULLS LAST LIMIT 1)");

    // 3b. Assert that getBoundaryQuery generates correct partitioned query for BIT column (using
    // CTE with subqueries)
    assertThat(
            adapter.getBoundaryQuery("my_schema.table1", ImmutableList.of("parent_col"), "col_bit"))
        .isEqualTo(
            "WITH filtered_uuid AS NOT MATERIALIZED (SELECT col_bit FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (parent_col >= ? AND (parent_col < ? OR (? = TRUE AND parent_col = ?))))) "
                + "SELECT (SELECT col_bit FROM filtered_uuid ORDER BY col_bit ASC NULLS LAST LIMIT 1), "
                + "(SELECT col_bit FROM filtered_uuid ORDER BY col_bit DESC NULLS LAST LIMIT 1)");

    // 4. Assert that getReadQuery generates the correct query for BIT column
    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of("col_bit")))
        .isEqualTo(
            "SELECT * FROM my_schema.table1 WHERE ((? = FALSE) OR (col_bit >= ? AND (col_bit < ? OR (? = TRUE AND col_bit = ?))))");

    // 5. Assert that getCountQuery generates the correct query for BIT column
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of("col_bit"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM my_schema.table1 WHERE ((? = FALSE) OR (col_bit >= ? AND (col_bit < ? OR (? = TRUE AND col_bit = ?))))");
  }

  @Test
  public void testDiscoverTableIndexesBulk()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> tables = ImmutableList.of("table1", "table2");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("table1", "table2");
    when(mockResultSet.getString("column_name")).thenReturn("id", "id");
    when(mockResultSet.getString("index_name")).thenReturn("pk1", "pk2");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true, true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true, true);
    when(mockResultSet.getLong("cardinality")).thenReturn(10L, 20L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L, 1L);
    when(mockResultSet.getString("type_name")).thenReturn("int8", "int8");

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> result =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables);

    assertThat(result).hasSize(2);
    assertThat(result).containsKey("table1");
    assertThat(result).containsKey("table2");
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
  public void testBoundaryQueryForParentTable() throws Exception {
    populateParentTableCache("my_parent_table");
    assertThat(adapter.getBoundaryQuery("my_parent_table", ImmutableList.of(), "id"))
        .isEqualTo("SELECT MIN(id), MAX(id) FROM ONLY my_parent_table");
    assertThat(adapter.getBoundaryQuery("my_parent_table", ImmutableList.of("col1", "col2"), "id"))
        .isEqualTo(
            "SELECT MIN(id), MAX(id) FROM ONLY my_parent_table "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches quoted identifiers passed from JdbcIoWrapper
    assertThat(adapter.getBoundaryQuery("\"my_parent_table\"", ImmutableList.of(), "id"))
        .isEqualTo("SELECT MIN(id), MAX(id) FROM ONLY \"my_parent_table\"");
    assertThat(
            adapter.getBoundaryQuery("\"my_parent_table\"", ImmutableList.of("col1", "col2"), "id"))
        .isEqualTo(
            "SELECT MIN(id), MAX(id) FROM ONLY \"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches schema-qualified quoted identifiers
    assertThat(adapter.getBoundaryQuery("\"public\".\"my_parent_table\"", ImmutableList.of(), "id"))
        .isEqualTo("SELECT MIN(id), MAX(id) FROM ONLY \"public\".\"my_parent_table\"");
    assertThat(
            adapter.getBoundaryQuery(
                "\"public\".\"my_parent_table\"", ImmutableList.of("col1", "col2"), "id"))
        .isEqualTo(
            "SELECT MIN(id), MAX(id) FROM ONLY \"public\".\"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches schema-qualified unquoted identifiers
    assertThat(adapter.getBoundaryQuery("public.my_parent_table", ImmutableList.of(), "id"))
        .isEqualTo("SELECT MIN(id), MAX(id) FROM ONLY public.my_parent_table");
    assertThat(
            adapter.getBoundaryQuery(
                "public.my_parent_table", ImmutableList.of("col1", "col2"), "id"))
        .isEqualTo(
            "SELECT MIN(id), MAX(id) FROM ONLY public.my_parent_table "
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
  public void testReadQueryWithMoneyType() throws Exception {
    ImmutableList<String> tables = ImmutableList.of("my_schema.table1");

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("my_schema.table1");
    when(mockResultSet.getString("column_name")).thenReturn("id", "amount");
    when(mockResultSet.getString("data_type")).thenReturn("bigint", "money");
    when(mockResultSet.getLong("character_maximum_length")).thenReturn(0L, 0L);
    when(mockResultSet.getLong("numeric_precision")).thenReturn(64L, 0L);
    when(mockResultSet.getLong("numeric_scale")).thenReturn(0L, 0L);
    when(mockResultSet.wasNull()).thenReturn(true, false, true, true, true, true);

    adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, tables);

    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of()))
        .isEqualTo("SELECT \"id\", \"amount\"::numeric AS \"amount\" FROM my_schema.table1");

    assertThat(adapter.getReadQuery("my_schema.table1", ImmutableList.of("id", "amount")))
        .isEqualTo(
            "SELECT \"id\", \"amount\"::numeric AS \"amount\" FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (id >= ? AND (id < ? OR (? = TRUE AND id = ?)))) "
                + "AND ((? = FALSE) OR (amount >= ? AND (amount < ? OR (? = TRUE AND amount = ?))))");
  }

  @Test
  public void testReadQueryForParentTable() throws Exception {
    populateParentTableCache("my_parent_table");
    assertThat(adapter.getReadQuery("my_parent_table", ImmutableList.of()))
        .isEqualTo("SELECT * FROM ONLY my_parent_table");
    assertThat(adapter.getReadQuery("my_parent_table", ImmutableList.of("col1", "col2")))
        .isEqualTo(
            "SELECT * FROM ONLY my_parent_table "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches quoted identifiers passed from JdbcIoWrapper
    assertThat(adapter.getReadQuery("\"my_parent_table\"", ImmutableList.of()))
        .isEqualTo("SELECT * FROM ONLY \"my_parent_table\"");
    assertThat(adapter.getReadQuery("\"my_parent_table\"", ImmutableList.of("col1", "col2")))
        .isEqualTo(
            "SELECT * FROM ONLY \"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches schema-qualified quoted identifiers
    assertThat(adapter.getReadQuery("\"public\".\"my_parent_table\"", ImmutableList.of()))
        .isEqualTo("SELECT * FROM ONLY \"public\".\"my_parent_table\"");
    assertThat(
            adapter.getReadQuery(
                "\"public\".\"my_parent_table\"", ImmutableList.of("col1", "col2")))
        .isEqualTo(
            "SELECT * FROM ONLY \"public\".\"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
    assertThat(adapter.getReadQuery("public.\"my_parent_table\"", ImmutableList.of()))
        .isEqualTo("SELECT * FROM ONLY public.\"my_parent_table\"");

    // Test that the adapter correctly matches schema-qualified unquoted identifiers
    assertThat(adapter.getReadQuery("public.my_parent_table", ImmutableList.of()))
        .isEqualTo("SELECT * FROM ONLY public.my_parent_table");
    assertThat(adapter.getReadQuery("public.my_parent_table", ImmutableList.of("col1", "col2")))
        .isEqualTo(
            "SELECT * FROM ONLY public.my_parent_table "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
  }

  @Test
  public void testCountQuery() {
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of(), 1000L))
        .isEqualTo("SELECT COUNT(*) FROM my_schema.table1");
    assertThat(adapter.getCountQuery("my_schema.table1", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM my_schema.table1 "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
  }

  @Test
  public void testCountQueryForParentTable() throws Exception {
    populateParentTableCache("my_parent_table");
    assertThat(adapter.getCountQuery("my_parent_table", ImmutableList.of(), 1000L))
        .isEqualTo("SELECT COUNT(*) FROM ONLY my_parent_table");
    assertThat(adapter.getCountQuery("my_parent_table", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM ONLY my_parent_table "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches quoted identifiers passed from JdbcIoWrapper
    assertThat(adapter.getCountQuery("\"my_parent_table\"", ImmutableList.of(), 1000L))
        .isEqualTo("SELECT COUNT(*) FROM ONLY \"my_parent_table\"");
    assertThat(
            adapter.getCountQuery("\"my_parent_table\"", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM ONLY \"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches schema-qualified quoted identifiers
    assertThat(adapter.getCountQuery("\"public\".\"my_parent_table\"", ImmutableList.of(), 1000L))
        .isEqualTo("SELECT COUNT(*) FROM ONLY \"public\".\"my_parent_table\"");
    assertThat(
            adapter.getCountQuery(
                "\"public\".\"my_parent_table\"", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM ONLY \"public\".\"my_parent_table\" "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");

    // Test that the adapter correctly matches schema-qualified unquoted identifiers
    assertThat(adapter.getCountQuery("public.my_parent_table", ImmutableList.of(), 1000L))
        .isEqualTo("SELECT COUNT(*) FROM ONLY public.my_parent_table");
    assertThat(
            adapter.getCountQuery(
                "public.my_parent_table", ImmutableList.of("col1", "col2"), 1000L))
        .isEqualTo(
            "SELECT COUNT(*) FROM ONLY public.my_parent_table "
                + "WHERE ((? = FALSE) OR (col1 >= ? AND (col1 < ? OR (? = TRUE AND col1 = ?)))) "
                + "AND ((? = FALSE) OR (col2 >= ? AND (col2 < ? OR (? = TRUE AND col2 = ?))))");
  }

  private void populateParentTableCache(String parentTableName) throws Exception {
    PreparedStatement mockParentPreparedStatement = mock(PreparedStatement.class);
    ResultSet mockParentResultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockConnection.prepareStatement(contains("pg_inherits")))
        .thenReturn(mockParentPreparedStatement);

    when(mockParentPreparedStatement.executeQuery()).thenReturn(mockParentResultSet);
    when(mockParentResultSet.next()).thenReturn(true, false);
    when(mockParentResultSet.getString("table_name")).thenReturn(parentTableName);

    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn(parentTableName, "normal_table");

    adapter.discoverTables(mockDataSource, sourceSchemaReference);
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

  @Test
  public void testDiscoverTableSchema_emptyTableList()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> result =
        adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, ImmutableList.of());
    assertThat(result).isEmpty();
  }

  @Test
  public void testDiscoverTableIndexes_emptyTableList()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> result =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, ImmutableList.of());
    assertThat(result).isEmpty();
  }

  @Test
  public void testDiscoverTableSchema_skipsExtraTables()
      throws SQLException, RetriableSchemaDiscoveryException {
    final String table1 = "table1";
    final String extraTable = "extraTable";
    ImmutableList<String> tables = ImmutableList.of(table1);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn(table1, extraTable);
    when(mockResultSet.getString("column_name")).thenReturn("id", "id");
    when(mockResultSet.getString("data_type")).thenReturn("bigint", "bigint");
    when(mockResultSet.wasNull()).thenReturn(true, true, true, true, true, true);

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> result =
        adapter.discoverTableSchema(mockDataSource, sourceSchemaReference, tables);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey(table1);
    assertThat(result).doesNotContainKey(extraTable);
  }

  @Test
  public void testDiscoverTableIndexes_skipsExtraTables()
      throws SQLException, RetriableSchemaDiscoveryException {
    final String table1 = "table1";
    final String extraTable = "extraTable";
    ImmutableList<String> tables = ImmutableList.of(table1);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn(table1, extraTable);
    when(mockResultSet.getString("column_name")).thenReturn("id", "id");
    when(mockResultSet.getString("index_name")).thenReturn("pk1", "pk2");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true, true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true, true);
    when(mockResultSet.getLong("cardinality")).thenReturn(10L, 20L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L, 1L);
    when(mockResultSet.getString("type_name")).thenReturn("int8", "int8");

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> result =
        adapter.discoverTableIndexes(mockDataSource, sourceSchemaReference, tables);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey(table1);
    assertThat(result).doesNotContainKey(extraTable);
  }

  @Test
  public void testSetDataSourceLoginTimeout() {
    BasicDataSource mockBasicDataSource = mock(BasicDataSource.class);
    when(mockBasicDataSource.getUrl()).thenReturn("jdbc://testIp:5432/testDB");

    long timeoutMs = 1000L;
    adapter.setDataSourceLoginTimeout(mockBasicDataSource, timeoutMs);

    verify(mockBasicDataSource).setMaxWaitMillis(timeoutMs);
    verify(mockBasicDataSource).addConnectionProperty("loginTimeout", "1");
    verify(mockBasicDataSource).addConnectionProperty("connectTimeout", "1");
    verify(mockBasicDataSource).addConnectionProperty("socketTimeout", "1");
  }

  @Test
  public void testSetDataSourceLoginTimeout_doesNotOverrideExistingUrlParams() {
    BasicDataSource mockBasicDataSource = mock(BasicDataSource.class);
    when(mockBasicDataSource.getUrl())
        .thenReturn("jdbc://testIp:5432/testDB?connectTimeout=2000&socketTimeout=2000");

    long timeoutMs = 1000L;
    adapter.setDataSourceLoginTimeout(mockBasicDataSource, timeoutMs);

    verify(mockBasicDataSource).setMaxWaitMillis(timeoutMs);
    verify(mockBasicDataSource, never()).addConnectionProperty(eq("connectTimeout"), anyString());
    verify(mockBasicDataSource, never()).addConnectionProperty(eq("socketTimeout"), anyString());
  }

  @Test
  public void testExtractBoundaryLocalTime() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.getBytes(1)).thenReturn("12:34:56.123456".getBytes(StandardCharsets.UTF_8));

    LocalTime localTime = adapter.extractBoundaryLocalTime(mockRs, 1);

    assertThat(localTime).isEqualTo(LocalTime.parse("12:34:56.123456"));
  }

  @Test
  public void testExtractBoundaryOffsetTime() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.getBytes(1)).thenReturn("12:34:56.123456+05:30".getBytes(StandardCharsets.UTF_8));

    OffsetTime offsetTime = adapter.extractBoundaryOffsetTime(mockRs, 1);

    assertThat(offsetTime).isEqualTo(OffsetTime.parse("12:34:56.123456+05:30"));
  }
}
