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
package com.google.cloud.teleport.v2.source.oracle;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.OracleInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceIndex;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
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
public class OracleSchemaFetcherTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private OracleSchemaFetcher.ConnectionProvider mockConnectionProvider;
  @Mock private Connection mockConnection;
  @Mock private OracleInformationSchemaScanner mockScanner;

  private OracleSchemaFetcher fetcher;

  @Before
  public void setUp() throws SQLException {
    when(mockConnectionProvider.get()).thenReturn(mockConnection);
    fetcher =
        new OracleSchemaFetcher(mockConnectionProvider) {
          @Override
          protected OracleInformationSchemaScanner createScanner(
              Connection connection, String schemaName) {
            return mockScanner;
          }
        };
  }

  @Test
  public void testInit_success() {
    Shard shard = new Shard("id", "host", "1521", "user", "pass", "XEPDB1", null, null, null);
    fetcher.init(new OracleSinkConfig(ImmutableList.of(shard)));
  }

  @Test
  public void testInit_nullConfig() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> fetcher.init(null));
    assertThat(ex).hasMessageThat().contains("SinkConfig cannot be null");
  }

  @Test
  public void testInit_emptyShards() {
    RuntimeException ex =
        assertThrows(
            RuntimeException.class, () -> fetcher.init(new OracleSinkConfig(ImmutableList.of())));
    assertThat(ex).hasMessageThat().contains("No shards found");
  }

  @Test
  public void testGetSchema_success() throws Exception {
    SourceTable tableA = SourceTable.builder(SourceDatabaseType.ORACLE).name("TableA").build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.ORACLE)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("ORACLE")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    assertThat(schema.tables()).containsKey("TableA");
  }

  @Test
  public void testGetSchema_detailedMapping() throws Exception {
    SourceColumn col1 =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("Id")
            .type("NUMBER")
            .isNullable(false)
            .isGenerated(false)
            .precision(10)
            .scale(0)
            .build();
    SourceColumn col2 =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("Name")
            .type("VARCHAR2")
            .isNullable(true)
            .isGenerated(false)
            .size(255L)
            .build();
    SourceColumn colInterval =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("IntervalCol")
            .type("INTERVAL YEAR(2) TO MONTH")
            .isNullable(true)
            .isGenerated(false)
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
        SourceTable.builder(SourceDatabaseType.ORACLE)
            .name("TableA")
            .columns(ImmutableList.of(col1, col2, colInterval))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of(fk))
            .indexes(ImmutableList.of(idxUnique, idxPrimary))
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.ORACLE)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("ORACLE")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    DataGeneratorTable actualTable = schema.tables().get("TableA");
    assertThat(actualTable).isNotNull();
    assertThat(actualTable.name()).isEqualTo("TableA");
    assertThat(actualTable.columns()).hasSize(3);
    assertThat(actualTable.columns().stream().map(DataGeneratorColumn::name))
        .containsExactly("Id", "Name", "IntervalCol");
    assertThat(actualTable.columns().get(1).size()).isEqualTo(20L);
    assertThat(actualTable.columns().get(2).fakerExpression())
        .isEqualTo("#{regexify '[+][0][0]-[0][0-9]'}");
    assertThat(actualTable.primaryKeys()).containsExactly("Id");
    assertThat(actualTable.foreignKeys()).hasSize(1);
    assertThat(actualTable.foreignKeys().get(0).name()).isEqualTo("fk_test");
    assertThat(actualTable.uniqueKeys()).hasSize(1);
    assertThat(actualTable.uniqueKeys().get(0).name()).isEqualTo("idx_unique");
  }

  @Test
  public void testGetSchema_numericAndSpecialTypeBounds() throws Exception {
    SourceColumn colPk =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("Id")
            .type("NUMBER")
            .isNullable(false)
            .isGenerated(false)
            .precision(38)
            .scale(0)
            .build();
    SourceColumn colSubUnit =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("SubUnitNum")
            .type("NUMBER")
            .isNullable(true)
            .isGenerated(false)
            .precision(3)
            .scale(4)
            .build();
    SourceColumn colNegScale =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("NegScaleNum")
            .type("NUMBER")
            .isNullable(true)
            .isGenerated(false)
            .precision(4)
            .scale(-2)
            .build();
    SourceColumn colXml =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("XmlCol")
            .type("XMLTYPE")
            .isNullable(true)
            .isGenerated(false)
            .build();
    SourceColumn colBfile =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("BfileCol")
            .type("BFILE")
            .isNullable(true)
            .isGenerated(false)
            .build();

    SourceTable table =
        SourceTable.builder(SourceDatabaseType.ORACLE)
            .name("SpecialTable")
            .columns(ImmutableList.of(colPk, colSubUnit, colNegScale, colXml, colBfile))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.ORACLE)
            .tables(ImmutableMap.of(table.name(), table))
            .databaseName("ORACLE")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");

    DataGeneratorSchema schema = fetcher.getSchema();
    DataGeneratorTable actualTable = schema.tables().get("SpecialTable");
    assertThat(actualTable).isNotNull();
    assertThat(actualTable.columns().get(0).precision()).isEqualTo(10);
    assertThat(actualTable.columns().get(0).scale()).isEqualTo(0);
    assertThat(actualTable.columns().get(1).precision()).isEqualTo(4);
    assertThat(actualTable.columns().get(1).scale()).isEqualTo(4);
    assertThat(actualTable.columns().get(1).fakerExpression())
        .isEqualTo("#{regexify '0[.]0[1-9][0-9]{2}'}");
    assertThat(actualTable.columns().get(2).precision()).isEqualTo(4);
    assertThat(actualTable.columns().get(2).scale()).isEqualTo(0);
    assertThat(actualTable.columns().get(3).fakerExpression())
        .isEqualTo("#{regexify '<root>[a-z0-9]{8}</root>'}");
    assertThat(actualTable.columns().get(4).isSkipped()).isTrue();
  }

  @Test
  public void testGetSchema_connectionFailure() throws Exception {
    when(mockConnectionProvider.get()).thenThrow(new SQLException("Connection failed"));

    IOException ex = assertThrows(IOException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Failed to fetch Oracle schema");
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  public void testGetSchema_scanFailure() throws Exception {
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");
    when(mockScanner.scan()).thenThrow(new RuntimeException("Scan failed"));

    RuntimeException ex = assertThrows(RuntimeException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Scan failed");
  }

  @Test
  public void testGetSchema_mappingNulls() throws Exception {
    SourceColumn col1 =
        SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name("Id")
            .type("NUMBER")
            .isNullable(false)
            .isGenerated(false)
            .size(0L)
            .build();
    SourceTable tableA =
        SourceTable.builder(SourceDatabaseType.ORACLE)
            .name("TableA")
            .columns(ImmutableList.of(col1))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.ORACLE)
            .tables(ImmutableMap.of(tableA.name(), tableA))
            .databaseName("ORACLE")
            .build();
    when(mockScanner.scan()).thenReturn(sourceSchema);
    when(mockConnection.getSchema()).thenReturn("C##DATASTREAM");

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    DataGeneratorTable actualTable = schema.tables().get("TableA");
    assertThat(actualTable.foreignKeys()).isEmpty();
    assertThat(actualTable.uniqueKeys()).isEmpty();
  }
}
