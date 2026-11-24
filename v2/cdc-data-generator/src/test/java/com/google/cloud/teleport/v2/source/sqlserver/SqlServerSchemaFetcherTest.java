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
package com.google.cloud.teleport.v2.source.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.sqlserver.SqlServerSchemaFetcher.ColumnInfo;
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
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.datafaker.Faker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(JUnit4.class)
public class SqlServerSchemaFetcherTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private SqlServerSchemaFetcher.ConnectionProvider mockConnectionProvider;
  @Mock private Connection mockConnection;
  @Mock private SQLServerInformationSchemaScanner mockScanner;

  private final Map<String, Map<String, ColumnInfo>> columnInfos = new HashMap<>();
  private String scannedSchema;

  private SqlServerSchemaFetcher fetcher;

  @Before
  public void setUp() throws SQLException {
    when(mockConnectionProvider.get()).thenReturn(mockConnection);
    when(mockConnection.getSchema()).thenReturn("dbo");
    fetcher =
        new SqlServerSchemaFetcher(mockConnectionProvider) {
          @Override
          protected SQLServerInformationSchemaScanner createScanner(
              Connection connection, String databaseName, String schemaName) {
            scannedSchema = schemaName;
            return mockScanner;
          }

          @Override
          protected Map<String, Map<String, ColumnInfo>> fetchColumnInfo(
              Connection connection, String schemaName) {
            return columnInfos;
          }
        };
  }

  private static SourceColumn col(String name, String type) {
    return SourceColumn.builder(SourceDatabaseType.SQLSERVER)
        .name(name)
        .type(type)
        .isNullable(true)
        .isGenerated(false)
        .build();
  }

  private static ColumnInfo info(
      boolean identity, boolean columnSet, boolean typedXml, boolean udt, boolean assembly) {
    return new ColumnInfo(identity, columnSet, typedXml, udt, assembly);
  }

  private DataGeneratorTable fetchSingleTable(SourceTable table) throws IOException {
    when(mockScanner.scan())
        .thenReturn(
            SourceSchema.builder(SourceDatabaseType.SQLSERVER)
                .tables(ImmutableMap.of(table.name(), table))
                .databaseName("testdb")
                .build());
    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    return schema.tables().get(table.name());
  }

  private static DataGeneratorColumn column(DataGeneratorTable table, String name) {
    return table.columns().stream().filter(c -> c.name().equals(name)).findFirst().get();
  }

  @Test
  public void testInit_success() {
    Shard shard = new Shard("id", "host", "1433", "user", "pass", "testdb", null, null, null);
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
    assertThrows(
        IllegalArgumentException.class,
        () -> fetcher.init(new com.google.cloud.teleport.v2.templates.model.SinkConfig() {}));
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
  public void testGetSchema_usesShardNamespace() throws Exception {
    Shard shard = new Shard("id", "host", "1433", "user", "pass", "testdb", "[sales]", null, null);
    fetcher.init(new SqlServerSinkConfig(ImmutableList.of(shard)));
    fetchSingleTable(SourceTable.builder(SourceDatabaseType.SQLSERVER).name("T").build());
    assertThat(scannedSchema).isEqualTo("sales");
  }

  @Test
  public void testGetSchema_defaultsToConnectionSchema() throws Exception {
    fetchSingleTable(SourceTable.builder(SourceDatabaseType.SQLSERVER).name("T").build());
    assertThat(scannedSchema).isEqualTo("dbo");
  }

  @Test
  public void testGetSchema_keysAndIndexes() throws Exception {
    SourceForeignKey fk =
        SourceForeignKey.builder()
            .tableName("Orders")
            .name("fk_customer")
            .referencedTable("Customers")
            .keyColumns(ImmutableList.of("CustomerId"))
            .referencedColumns(ImmutableList.of("Id"))
            .build();
    SourceIndex unique =
        SourceIndex.builder()
            .tableName("Orders")
            .name("uq_code")
            .isUnique(true)
            .isPrimary(false)
            .columns(ImmutableList.of("Code"))
            .build();
    SourceIndex primary =
        SourceIndex.builder()
            .tableName("Orders")
            .name("PK_Orders")
            .isUnique(true)
            .isPrimary(true)
            .columns(ImmutableList.of("Id"))
            .build();
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("Orders")
            .columns(
                ImmutableList.of(
                    col("Id", "int"), col("CustomerId", "int"), col("Code", "varchar")))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of(fk))
            .indexes(ImmutableList.of(unique, primary))
            .build();

    DataGeneratorTable actual = fetchSingleTable(table);
    assertThat(actual.primaryKeys()).containsExactly("Id");
    assertThat(actual.foreignKeys()).hasSize(1);
    assertThat(actual.foreignKeys().get(0).referencedTable()).isEqualTo("Customers");
    assertThat(actual.uniqueKeys()).hasSize(1);
    assertThat(actual.uniqueKeys().get(0).name()).isEqualTo("uq_code");
    assertThat(column(actual, "Id").isPrimaryKey()).isTrue();
  }

  @Test
  public void testGetSchema_typeRules() throws Exception {
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("AllTypes")
            .columns(
                ImmutableList.of(
                    col("Id", "int identity"),
                    col("SeqNo", "bigint identity"),
                    col("Tiny", "tinyint"),
                    col("Small", "smallint"),
                    col("Big", "bigint"),
                    col("Dec", "decimal").toBuilder().size(5L).precision(5).scale(2).build(),
                    col("Money", "money").toBuilder().size(19L).precision(19).scale(4).build(),
                    col("SmallMoney", "smallmoney").toBuilder()
                        .size(10L)
                        .precision(10)
                        .scale(4)
                        .build(),
                    col("Flt", "float").toBuilder().size(53L).build(),
                    col("Chr", "char").toBuilder().size(10L).build(),
                    col("NVarMax", "nvarchar").toBuilder().size(1073741823L).build(),
                    col("Img", "image").toBuilder().size(2147483647L).build(),
                    col("Var", "sql_variant").toBuilder().size(8000L).build(),
                    col("Dt", "datetime").toBuilder().size(23L).precision(23).scale(3).build(),
                    col("Sdt", "smalldatetime"),
                    col("Dt2", "datetime2").toBuilder().size(27L).precision(27).scale(7).build(),
                    col("Xml", "xml"),
                    col("TypedXml", "xml"),
                    col("Geom", "geometry"),
                    col("Geog", "geography"),
                    col("Hier", "hierarchyid"),
                    col("Vec", "vector"),
                    col("Alias", "ssn"),
                    col("Clr", "Point"),
                    col("Cs", "xml"),
                    col("Rv", "timestamp"),
                    col("Computed", "int").toBuilder().isGenerated(true).build(),
                    col("Guid", "uniqueidentifier"),
                    col("Json", "json"),
                    col("Flag", "bit")))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .build();
    columnInfos.put(
        "AllTypes",
        ImmutableMap.of(
            "Id", info(true, false, false, false, false),
            "SeqNo", info(true, false, false, false, false),
            "TypedXml", info(false, false, true, false, false),
            "Alias", info(false, false, false, true, false),
            "Clr", info(false, false, false, true, true),
            "Cs", info(false, true, false, false, false),
            "Geom", info(false, false, false, false, true)));

    DataGeneratorTable actual = fetchSingleTable(table);

    // IDENTITY PK is written (IDENTITY_INSERT); non-PK IDENTITY is left to the server.
    assertThat(column(actual, "Id").isGenerated()).isFalse();
    assertThat(column(actual, "Id").logicalType()).isEqualTo(LogicalType.INT64);
    assertThat(column(actual, "SeqNo").isGenerated()).isTrue();

    assertThat(column(actual, "Tiny").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.TINYINT_EXPRESSION);
    assertThat(column(actual, "Small").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.SMALLINT_EXPRESSION);
    assertThat(column(actual, "Big").fakerExpression()).isNull();

    assertThat(column(actual, "Dec").precision()).isEqualTo(5);
    assertThat(column(actual, "Dec").scale()).isEqualTo(2);
    assertThat(column(actual, "Money").precision()).isEqualTo(18);
    assertThat(column(actual, "Money").scale()).isEqualTo(4);
    assertThat(column(actual, "SmallMoney").precision()).isEqualTo(9);
    assertThat(column(actual, "SmallMoney").scale()).isEqualTo(4);
    assertThat(column(actual, "Flt").logicalType()).isEqualTo(LogicalType.FLOAT64);
    assertThat(column(actual, "Flt").precision()).isNull();
    assertThat(column(actual, "Dt2").precision()).isNull();

    assertThat(column(actual, "Chr").size()).isEqualTo(10L);
    assertThat(column(actual, "NVarMax").size()).isNull();
    assertThat(column(actual, "Img").size()).isNull();
    assertThat(column(actual, "Var").size()).isNull();

    assertThat(column(actual, "Dt").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.DATETIME_EXPRESSION);
    assertThat(column(actual, "Sdt").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.SMALLDATETIME_EXPRESSION);
    assertThat(column(actual, "Dt2").fakerExpression()).isNull();

    assertThat(column(actual, "Xml").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.XML_EXPRESSION);
    assertThat(column(actual, "Xml").isSkipped()).isFalse();
    assertThat(column(actual, "TypedXml").isSkipped()).isTrue();
    assertThat(column(actual, "Geom").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.GEOMETRY_EXPRESSION);
    assertThat(column(actual, "Geom").isSkipped()).isFalse();
    assertThat(column(actual, "Geog").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.GEOGRAPHY_EXPRESSION);
    assertThat(column(actual, "Hier").fakerExpression())
        .isEqualTo(SqlServerSchemaFetcher.HIERARCHYID_EXPRESSION);

    assertThat(column(actual, "Vec").isSkipped()).isTrue();
    assertThat(column(actual, "Alias").isSkipped()).isTrue();
    assertThat(column(actual, "Clr").isSkipped()).isTrue();
    assertThat(column(actual, "Cs").isSkipped()).isTrue();

    assertThat(column(actual, "Rv").logicalType()).isEqualTo(LogicalType.BYTES);
    assertThat(column(actual, "Rv").isGenerated()).isTrue();
    assertThat(column(actual, "Computed").isGenerated()).isTrue();

    assertThat(column(actual, "Guid").logicalType()).isEqualTo(LogicalType.UUID);
    assertThat(column(actual, "Json").logicalType()).isEqualTo(LogicalType.JSON);
    assertThat(column(actual, "Flag").logicalType()).isEqualTo(LogicalType.BOOLEAN);
  }

  @Test
  public void testGetSchema_identityFromTypeNameWithoutColumnInfo() throws Exception {
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("T")
            .columns(ImmutableList.of(col("Id", "int"), col("Seq", "int identity")))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .build();
    DataGeneratorTable actual = fetchSingleTable(table);
    assertThat(column(actual, "Seq").isGenerated()).isTrue();
    assertThat(column(actual, "Id").isGenerated()).isFalse();
  }

  @Test
  public void testGetSchema_primaryKeyIsNeverSkipped() throws Exception {
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("T")
            .columns(ImmutableList.of(col("Id", "ssn")))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .build();
    columnInfos.put("T", ImmutableMap.of("Id", info(false, false, false, true, false)));
    DataGeneratorTable actual = fetchSingleTable(table);
    assertThat(column(actual, "Id").isSkipped()).isFalse();
  }

  @Test
  public void testGetSchema_scannerFailureWrapsIOException() throws Exception {
    when(mockConnectionProvider.get()).thenThrow(new SQLException("boom"));
    assertThrows(IOException.class, () -> fetcher.getSchema());
  }

  @Test
  public void testFakerExpressions_produceValuesInRange() {
    Faker faker = new Faker();
    for (int i = 0; i < 200; i++) {
      long tiny =
          (Long) generate(faker, LogicalType.INT64, SqlServerSchemaFetcher.TINYINT_EXPRESSION);
      assertThat(tiny).isAtLeast(0L);
      assertThat(tiny).isAtMost(255L);
      long small =
          (Long) generate(faker, LogicalType.INT64, SqlServerSchemaFetcher.SMALLINT_EXPRESSION);
      assertThat(small).isAtLeast(-32768L);
      assertThat(small).isAtMost(32767L);

      Instant dt =
          (Instant)
              generate(faker, LogicalType.TIMESTAMP, SqlServerSchemaFetcher.DATETIME_EXPRESSION);
      assertThat(dt.getMillis() % 1000).isEqualTo(0);
      Instant sdt =
          (Instant)
              generate(
                  faker, LogicalType.TIMESTAMP, SqlServerSchemaFetcher.SMALLDATETIME_EXPRESSION);
      assertThat(sdt.getMillis() % 60000).isEqualTo(0);

      assertThat(
              (String) generate(faker, LogicalType.STRING, SqlServerSchemaFetcher.XML_EXPRESSION))
          .matches("<root>[a-z0-9]{8}</root>");
      assertThat(
              (String)
                  generate(faker, LogicalType.STRING, SqlServerSchemaFetcher.GEOMETRY_EXPRESSION))
          .matches("POINT\\([1-9][0-9] [1-9][0-9]\\)");
      assertThat(
              (String)
                  generate(faker, LogicalType.STRING, SqlServerSchemaFetcher.GEOGRAPHY_EXPRESSION))
          .matches("POINT\\([1-9][0-9] [1-8][0-9]\\)");
      assertThat(
              (String)
                  generate(
                      faker, LogicalType.STRING, SqlServerSchemaFetcher.HIERARCHYID_EXPRESSION))
          .matches("/[1-9]/[1-9]/");
    }
  }

  private static Object generate(Faker faker, LogicalType type, String expression) {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(type)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression(expression)
            .build();
    return DataGeneratorUtils.generateValue("t", column, faker, null);
  }

  @Test
  public void testFetchColumnInfo_readsFlags() throws Exception {
    SqlServerSchemaFetcher realFetcher = new SqlServerSchemaFetcher(mockConnectionProvider);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("table_name")).thenReturn("T", "T");
    when(rs.getString("column_name")).thenReturn("Id", "Cs");
    when(rs.getBoolean("is_identity")).thenReturn(true, false);
    when(rs.getBoolean("is_column_set")).thenReturn(false, true);
    when(rs.getInt("is_typed_xml")).thenReturn(0, 0);
    when(rs.getBoolean("is_user_defined")).thenReturn(false, false);
    when(rs.getBoolean("is_assembly_type")).thenReturn(false, false);

    Map<String, Map<String, ColumnInfo>> result =
        realFetcher.fetchColumnInfo(mockConnection, "dbo");

    verify(mockConnection).prepareStatement(SqlServerSchemaFetcher.COLUMN_INFO_QUERY);
    verify(statement).setString(1, "dbo");
    assertThat(result.get("T").get("Id").isIdentity).isTrue();
    assertThat(result.get("T").get("Cs").isColumnSet).isTrue();
  }

  @Test
  public void testFetchColumnInfo_failureReturnsEmpty() throws Exception {
    SqlServerSchemaFetcher realFetcher = new SqlServerSchemaFetcher(mockConnectionProvider);
    when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("denied"));
    assertThat(realFetcher.fetchColumnInfo(mockConnection, "dbo")).isEmpty();
  }

  @Test
  public void testMapSourceColumn_nullTypeDefaultsToString() {
    DataGeneratorColumn column =
        fetcher.mapSourceColumn(
            SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("c").type("").build(),
            List.of(),
            null);
    assertThat(column.logicalType()).isEqualTo(LogicalType.STRING);
  }
}
