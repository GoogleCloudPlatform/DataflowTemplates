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
package com.google.cloud.teleport.v2.templates.mysql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceIndex;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher.ConnectionProvider;
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

@RunWith(JUnit4.class)
public class MySqlSchemaFetcherTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private ShardFileReader mockShardFileReader;
  @Mock private ConnectionProvider mockConnectionProvider;
  @Mock private Connection mockConnection;
  @Mock private MySqlInformationSchemaScanner mockScanner;

  private MySqlSchemaFetcher fetcher;

  @Before
  public void setUp() throws SQLException {
    when(mockConnectionProvider.get()).thenReturn(mockConnection);
    fetcher = new MySqlSchemaFetcher(mockShardFileReader, mockConnectionProvider);
    fetcher.setTestScanner(mockScanner);
  }

  @Test
  public void testInit_success() {
    Shard shard = new Shard("id", "host", "user", "pass", "port", "db", null, null, null);
    when(mockShardFileReader.getOrderedShardDetails(anyString()))
        .thenReturn(ImmutableList.of(shard));

    fetcher.init("options.json");
    // No exception thrown means success
  }

  @Test
  public void testInit_noOptionsFile() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> fetcher.init(null));
    assertThat(ex).hasMessageThat().contains("requires a valid shard configuration file path");

    ex = assertThrows(IllegalArgumentException.class, () -> fetcher.init(""));
    assertThat(ex).hasMessageThat().contains("requires a valid shard configuration file path");
  }

  @Test
  public void testInit_emptyShardFile() {
    when(mockShardFileReader.getOrderedShardDetails(anyString())).thenReturn(ImmutableList.of());
    RuntimeException ex = assertThrows(RuntimeException.class, () -> fetcher.init("options.json"));
    assertThat(ex).hasMessageThat().contains("No shards found");
  }

  @Test
  public void testGetSchema_success() throws Exception {
    SourceTable tableA = SourceTable.builder(SourceDatabaseType.MYSQL).name("TableA").build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("testdb")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getCatalog()).thenReturn("testdb");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    assertThat(schema.tables()).containsKey("TableA");
  }

  @Test
  public void testGetSchema_detailedMapping() throws Exception {
    SourceColumn col1 =
        SourceColumn.builder(SourceDatabaseType.MYSQL)
            .name("Id")
            .type("INT")
            .isNullable(false)
            .isGenerated(false)
            .size(0L)
            .build();
    SourceColumn col2 =
        SourceColumn.builder(SourceDatabaseType.MYSQL)
            .name("Name")
            .type("VARCHAR")
            .isNullable(true)
            .isGenerated(false)
            .size(255L)
            .build();
    SourceForeignKey fk =
        SourceForeignKey.builder()
            .tableName("TableA")
            .name("fk_test")
            .referencedTable("RefTable")
            .keyColumns(ImmutableList.of("Id"))
            .referencedColumns(ImmutableList.of("RefId"))
            .build();
    SourceIndex idxUnique =
        SourceIndex.builder()
            .tableName("TableA")
            .name("idx_unique")
            .isUnique(true)
            .isPrimary(false)
            .columns(ImmutableList.of("Name"))
            .build();
    SourceIndex idxPrimary =
        SourceIndex.builder()
            .tableName("TableA")
            .name("PRIMARY")
            .isUnique(true)
            .isPrimary(true)
            .columns(ImmutableList.of("Id"))
            .build();

    SourceTable tableA =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("TableA")
            .columns(ImmutableList.of(col1, col2))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of(fk))
            .indexes(ImmutableList.of(idxUnique, idxPrimary))
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("testdb")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getCatalog()).thenReturn("testdb");

    int testQps = 100;
    fetcher.setInsertQps(testQps);
    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    DataGeneratorTable actualTable = schema.tables().get("TableA");
    assertThat(actualTable).isNotNull();
    assertThat(actualTable.name()).isEqualTo("TableA");
    assertThat(actualTable.columns()).hasSize(2);
    assertThat(actualTable.columns().stream().map(DataGeneratorColumn::name))
        .containsExactly("Id", "Name");
    assertThat(actualTable.primaryKeys()).containsExactly("Id");
    assertThat(actualTable.foreignKeys()).hasSize(1);
    assertThat(actualTable.foreignKeys().get(0).name()).isEqualTo("fk_test");
    assertThat(actualTable.uniqueKeys()).hasSize(1);
    assertThat(actualTable.uniqueKeys().get(0).name()).isEqualTo("idx_unique");
    assertThat(actualTable.insertQps()).isEqualTo(testQps);
  }

  @Test
  public void testGetSchema_connectionFailure() throws Exception {
    when(mockConnectionProvider.get()).thenThrow(new SQLException("Connection failed"));

    IOException ex = assertThrows(IOException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Failed to fetch MySQL schema");
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  public void testGetSchema_scanFailure() throws Exception {
    when(mockConnection.getCatalog()).thenReturn("testdb");
    when(mockScanner.scan()).thenThrow(new RuntimeException("Scan failed"));

    RuntimeException ex = assertThrows(RuntimeException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Scan failed");
  }

  @Test
  public void testGetSchema_mappingNulls() throws Exception {
    SourceColumn col1 =
        SourceColumn.builder(SourceDatabaseType.MYSQL)
            .name("Id")
            .type("INT")
            .isNullable(false)
            .isGenerated(false)
            .size(0L)
            .build();
    SourceTable tableA =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("TableA")
            .columns(ImmutableList.of(col1))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of()) // Test empty foreign keys
            .indexes(ImmutableList.of()) // Test empty indexes
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("testdb")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getCatalog()).thenReturn("testdb");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    DataGeneratorTable actualTable = schema.tables().get("TableA");
    assertThat(actualTable.foreignKeys()).isEmpty();
    assertThat(actualTable.uniqueKeys()).isEmpty();
  }
}
