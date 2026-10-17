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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SQLServerInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceIndex;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SqlServerSinkConfig;
import com.google.cloud.teleport.v2.templates.sqlserver.SqlServerSchemaFetcher.ConnectionProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link SqlServerSchemaFetcher}. */
@RunWith(JUnit4.class)
public class SqlServerSchemaFetcherTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private ConnectionProvider mockConnectionProvider;
  @Mock private Connection mockConnection;
  @Mock private SQLServerInformationSchemaScanner mockScanner;

  private SqlServerSchemaFetcher fetcher;

  @Before
  public void setUp() throws SQLException {
    when(mockConnectionProvider.get()).thenReturn(mockConnection);
    fetcher =
        new SqlServerSchemaFetcher(mockConnectionProvider) {
          @Override
          protected SQLServerInformationSchemaScanner createScanner(
              Connection connection, String databaseName) {
            return mockScanner;
          }
        };
  }

  private static SourceColumn.Builder column(String name, String type) {
    return SourceColumn.builder(SourceDatabaseType.SQLSERVER)
        .name(name)
        .type(type)
        .isNullable(true)
        .isGenerated(false);
  }

  private static SourceSchema schemaOf(SourceTable... tables) {
    ImmutableMap.Builder<String, SourceTable> map = ImmutableMap.builder();
    for (SourceTable t : tables) {
      map.put(t.name(), t);
    }
    return SourceSchema.builder(SourceDatabaseType.SQLSERVER)
        .tables(map.build())
        .databaseName("testdb")
        .build();
  }

  @Test
  public void testInit_success() {
    Shard shard = new Shard("id", "host", "1433", "user", "pass", "db", null, null, null);
    fetcher.init(new SqlServerSinkConfig(ImmutableList.of(shard)));
  }

  @Test
  public void testInit_nullConfig() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> fetcher.init(null));
    assertThat(ex).hasMessageThat().contains("SinkConfig cannot be null");
  }

  @Test
  public void testInit_wrongConfigType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> fetcher.init(new MySqlSinkConfig(ImmutableList.of())));
    assertThat(ex).hasMessageThat().contains("Expected SqlServerSinkConfig");
  }

  @Test
  public void testInit_emptyShards() {
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () -> fetcher.init(new SqlServerSinkConfig(ImmutableList.of())));
    assertThat(ex).hasMessageThat().contains("No shards found");
  }

  @Test
  public void testGetSchema_success() throws Exception {
    SourceTable tableA = SourceTable.builder(SourceDatabaseType.SQLSERVER).name("TableA").build();
    when(mockScanner.scan()).thenReturn(schemaOf(tableA));
    when(mockConnection.getCatalog()).thenReturn("testdb");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    assertThat(schema.tables()).containsKey("TableA");
  }

  @Test
  public void testGetSchema_detailedMapping() throws Exception {
    SourceColumn id =
        column("Id", "int identity").isNullable(false).size(10L).precision(10).scale(0).build();
    SourceColumn name = column("Name", "nvarchar").size(255L).build();
    SourceColumn notes = column("Notes", "nvarchar").size(2147483647L).build();
    SourceColumn blob = column("Blob", "varbinary").size(2147483647L).build();
    SourceColumn amount = column("Amount", "decimal").size(12L).precision(12).scale(4).build();
    SourceColumn version = column("Version", "timestamp").size(8L).build();
    SourceColumn computed = column("Computed", "int").isGenerated(true).build();
    SourceForeignKey fk =
        SourceForeignKey.builder()
            .tableName("TableA")
            .name("FK_TableA_Ref")
            .referencedTable("RefTable")
            .keyColumns(ImmutableList.of("Id"))
            .referencedColumns(ImmutableList.of("RefId"))
            .build();
    SourceIndex idxUnique =
        SourceIndex.builder()
            .tableName("TableA")
            .name("UQ_Name")
            .isUnique(true)
            .isPrimary(false)
            .columns(ImmutableList.of("Name"))
            .build();
    SourceIndex idxPrimary =
        SourceIndex.builder()
            .tableName("TableA")
            .name("PK_TableA")
            .isUnique(true)
            .isPrimary(true)
            .columns(ImmutableList.of("Id"))
            .build();
    SourceTable tableA =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("TableA")
            .columns(ImmutableList.of(id, name, notes, blob, amount, version, computed))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of(fk))
            .indexes(ImmutableList.of(idxUnique, idxPrimary))
            .build();
    when(mockScanner.scan()).thenReturn(schemaOf(tableA));
    when(mockConnection.getCatalog()).thenReturn("testdb");

    DataGeneratorSchema schema = fetcher.getSchema();
    DataGeneratorTable table = schema.tables().get("TableA");
    assertThat(table).isNotNull();
    assertThat(table.primaryKeys()).containsExactly("Id");
    assertThat(table.foreignKeys()).hasSize(1);
    assertThat(table.foreignKeys().get(0).name()).isEqualTo("FK_TableA_Ref");
    assertThat(table.foreignKeys().get(0).referencedTable()).isEqualTo("RefTable");
    // Primary key index must not be reported as a unique key.
    assertThat(table.uniqueKeys()).hasSize(1);
    assertThat(table.uniqueKeys().get(0).name()).isEqualTo("UQ_Name");

    ImmutableMap.Builder<String, DataGeneratorColumn> byName = ImmutableMap.builder();
    for (DataGeneratorColumn c : table.columns()) {
      byName.put(c.name(), c);
    }
    ImmutableMap<String, DataGeneratorColumn> cols = byName.build();
    assertThat(cols).hasSize(7);

    DataGeneratorColumn idCol = cols.get("Id");
    assertThat(idCol.logicalType()).isEqualTo(LogicalType.INT64);
    assertThat(idCol.isPrimaryKey()).isTrue();
    assertThat(idCol.isNullable()).isFalse();
    assertThat(idCol.isGenerated()).isFalse();

    DataGeneratorColumn nameCol = cols.get("Name");
    assertThat(nameCol.logicalType()).isEqualTo(LogicalType.STRING);
    assertThat(nameCol.size()).isEqualTo(255L);

    // nvarchar(max) / varbinary(max) sizes are dropped so the generator uses its default length.
    assertThat(cols.get("Notes").size()).isNull();
    assertThat(cols.get("Blob").logicalType()).isEqualTo(LogicalType.BYTES);
    assertThat(cols.get("Blob").size()).isNull();

    DataGeneratorColumn amountCol = cols.get("Amount");
    assertThat(amountCol.logicalType()).isEqualTo(LogicalType.NUMERIC);
    assertThat(amountCol.precision()).isEqualTo(12);
    assertThat(amountCol.scale()).isEqualTo(4);

    // rowversion / timestamp columns are server-populated and must be skipped by writers.
    assertThat(cols.get("Version").isGenerated()).isTrue();
    assertThat(cols.get("Computed").isGenerated()).isTrue();
  }

  @Test
  public void testEffectiveSize() {
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.STRING, null)).isNull();
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.STRING, 100L)).isEqualTo(100L);
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.STRING, 8000L)).isEqualTo(8000L);
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.STRING, 8001L)).isNull();
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.BYTES, 2147483647L)).isNull();
    // Non-string types keep their size untouched.
    assertThat(SqlServerSchemaFetcher.effectiveSize(LogicalType.INT64, 2147483647L))
        .isEqualTo(2147483647L);
  }

  @Test
  public void testIsIdentity() {
    assertThat(SqlServerSchemaFetcher.isIdentity("int identity")).isTrue();
    assertThat(SqlServerSchemaFetcher.isIdentity("BIGINT IDENTITY")).isTrue();
    assertThat(SqlServerSchemaFetcher.isIdentity("int")).isFalse();
    assertThat(SqlServerSchemaFetcher.isIdentity("nvarchar")).isFalse();
    assertThat(SqlServerSchemaFetcher.isIdentity(null)).isFalse();
  }

  @Test
  public void testEffectivePrecision() {
    // money / smallmoney have hard maxima below their reported decimal precision.
    assertThat(SqlServerSchemaFetcher.effectivePrecision("money", 19)).isEqualTo(18);
    assertThat(SqlServerSchemaFetcher.effectivePrecision("MONEY", 19)).isEqualTo(18);
    assertThat(SqlServerSchemaFetcher.effectivePrecision("smallmoney", 10)).isEqualTo(9);
    // Everything else passes through untouched.
    assertThat(SqlServerSchemaFetcher.effectivePrecision("decimal", 12)).isEqualTo(12);
    assertThat(SqlServerSchemaFetcher.effectivePrecision("int identity", 10)).isEqualTo(10);
    assertThat(SqlServerSchemaFetcher.effectivePrecision("nvarchar", null)).isNull();
    assertThat(SqlServerSchemaFetcher.effectivePrecision(null, 5)).isEqualTo(5);
  }

  @Test
  public void testGetSchema_moneyPrecisionAdjusted() throws Exception {
    SourceColumn id = column("Id", "int").isNullable(false).precision(10).scale(0).build();
    SourceColumn credit = column("Credit", "money").precision(19).scale(4).build();
    SourceColumn price = column("Price", "smallmoney").precision(10).scale(4).build();
    SourceTable t =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("T")
            .columns(ImmutableList.of(id, credit, price))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .build();
    when(mockScanner.scan()).thenReturn(schemaOf(t));
    when(mockConnection.getCatalog()).thenReturn("testdb");

    DataGeneratorTable table = fetcher.getSchema().tables().get("T");
    ImmutableMap.Builder<String, DataGeneratorColumn> byName = ImmutableMap.builder();
    for (DataGeneratorColumn c : table.columns()) {
      byName.put(c.name(), c);
    }
    ImmutableMap<String, DataGeneratorColumn> cols = byName.build();
    assertThat(cols.get("Id").precision()).isEqualTo(10);
    assertThat(cols.get("Id").fakerExpression()).isNull();
    assertThat(cols.get("Credit").precision()).isEqualTo(18);
    assertThat(cols.get("Credit").scale()).isEqualTo(4);
    assertThat(cols.get("Price").precision()).isEqualTo(9);
    assertThat(cols.get("Price").scale()).isEqualTo(4);
  }

  @Test
  public void testGetSchema_connectionFailure() throws Exception {
    when(mockConnectionProvider.get()).thenThrow(new SQLException("Connection failed"));

    IOException ex = assertThrows(IOException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Failed to fetch SQL Server schema");
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  public void testGetSchema_scanFailure() throws Exception {
    when(mockConnection.getCatalog()).thenReturn("testdb");
    when(mockScanner.scan()).thenThrow(new RuntimeException("Scan failed"));

    RuntimeException ex = assertThrows(RuntimeException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Scan failed");
  }
}
