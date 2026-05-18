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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/** Unit tests for {@link MySqlDataWriter}. */
@RunWith(JUnit4.class)
public class MySqlDataWriterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private ShardFileReader mockShardFileReader;

  @SuppressWarnings("unchecked")
  @Mock
  private IConnectionHelper<Connection> mockConnectionHelper;

  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockStatement;

  private static final String CONFIG_PATH = "shards.json";

  private Shard shardA() {
    return new Shard("shardA", "hostA", "3306", "userA", "passA", "dbA", null, null, null);
  }

  private Shard shardB() {
    return new Shard("shardB", "hostB", "3306", "userB", "passB", "dbB", null, null, null);
  }

  @Before
  public void setUp() throws Exception {
    when(mockShardFileReader.getOrderedShardDetails(anyString()))
        .thenReturn(ImmutableList.of(shardA(), shardB()));
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
  }

  private MySqlDataWriter writer() {
    return new MySqlDataWriter(CONFIG_PATH, mockShardFileReader, mockConnectionHelper);
  }

  private DataGeneratorTable simpleTable() {
    DataGeneratorColumn idCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorColumn nameCol =
        DataGeneratorColumn.builder()
            .name("name")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(false)
            .size(128L)
            .precision(null)
            .scale(null)
            .build();
    return DataGeneratorTable.builder()
        .name("users")
        .columns(ImmutableList.of(idCol, nameCol))
        .primaryKeys(ImmutableList.of("id"))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private DataGeneratorTable allTypesTable() {
    ImmutableList.Builder<DataGeneratorColumn> cols = ImmutableList.builder();
    for (LogicalType t : LogicalType.values()) {
      cols.add(
          DataGeneratorColumn.builder()
              .name(t.name().toLowerCase() + "_col")
              .logicalType(t)
              .isNullable(true)
              .isGenerated(false)
              .size(null)
              .precision(null)
              .scale(null)
              .build());
    }
    // Add a generated + skipped column to ensure they're excluded.
    cols.add(
        DataGeneratorColumn.builder()
            .name("generated_col")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build());
    cols.add(
        DataGeneratorColumn.builder()
            .name("skipped_col")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(false)
            .isSkipped(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build());
    return DataGeneratorTable.builder()
        .name("all_types")
        .columns(cols.build())
        .primaryKeys(ImmutableList.of("int64_col"))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private Row simpleRow(Long id, String name) {
    Schema schema =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT64)
            .addNullableField("name", Schema.FieldType.STRING)
            .build();
    return Row.withSchema(schema).addValues(id, name).build();
  }

  @Test
  public void testBuildUpsertSql_insertBranch() {
    String sql = MySqlDataWriter.buildUpsertSql(simpleTable());
    assertThat(sql)
        .isEqualTo(
            "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) "
                + "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)");
  }

  @Test
  public void testBuildDeleteSql() {
    String sql = MySqlDataWriter.buildDeleteSql(simpleTable());
    assertThat(sql).isEqualTo("DELETE FROM `users` WHERE `id` = ?");
  }

  @Test
  public void testBuildUpsertSql_multiPk() {
    DataGeneratorTable table =
        simpleTable().toBuilder().primaryKeys(ImmutableList.of("id", "name")).build();
    DataGeneratorColumn extra =
        DataGeneratorColumn.builder()
            .name("age")
            .logicalType(LogicalType.INT64)
            .isNullable(true)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    table =
        table.toBuilder()
            .columns(
                ImmutableList.<DataGeneratorColumn>builder()
                    .addAll(table.columns())
                    .add(extra)
                    .build())
            .build();
    String sql = MySqlDataWriter.buildUpsertSql(table);
    assertThat(sql)
        .isEqualTo(
            "INSERT INTO `users` (`id`, `name`, `age`) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE `age` = VALUES(`age`)");
  }

  @Test
  public void testBuildDeleteSql_noPkThrows() {
    DataGeneratorTable table = simpleTable().toBuilder().primaryKeys(ImmutableList.of()).build();
    assertThrows(IllegalStateException.class, () -> MySqlDataWriter.buildDeleteSql(table));
  }

  @Test
  public void testBuildUpsertSql_identifierWithBacktick() {
    DataGeneratorColumn weird =
        DataGeneratorColumn.builder()
            .name("weird`col")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable table =
        simpleTable().toBuilder()
            .columns(ImmutableList.of(weird))
            .primaryKeys(ImmutableList.of("weird`col"))
            .build();
    String sql = MySqlDataWriter.buildUpsertSql(table);
    assertThat(sql).startsWith("INSERT INTO `users` (`weird``col`) VALUES (?)");
    // Back-tick escapes must be preserved in the fallback self-assignment too.
    assertThat(sql).endsWith("ON DUPLICATE KEY UPDATE `weird``col` = `weird``col`");
  }

  @Test
  public void testBuildUpsertSql_noWritableColumnsThrows() {
    DataGeneratorColumn generated =
        DataGeneratorColumn.builder()
            .name("g")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable table =
        simpleTable().toBuilder().columns(ImmutableList.of(generated)).build();
    assertThrows(IllegalArgumentException.class, () -> MySqlDataWriter.buildUpsertSql(table));
  }

  @Test
  public void testWrite_insertRunsUpsertOnFirstShard() throws Exception {
    MySqlDataWriter w = writer();
    w.insert(
        ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")),
        simpleTable(),
        "",
        Constants.DEFAULT_JDBC_POOL_SIZE);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).startsWith("INSERT INTO");
    assertThat(sqlCaptor.getValue()).contains("ON DUPLICATE KEY UPDATE");

    // Shard A is the first shard.
    verify(mockConnectionHelper).getConnection(MySqlDataWriter.buildConnectionKey(shardA()));

    verify(mockStatement, times(2)).addBatch();
    verify(mockStatement).executeBatch();
    verify(mockStatement).close();
    verify(mockConnection).close();
  }

  @Test
  public void testWrite_routesToNamedShard() throws Exception {
    MySqlDataWriter w = writer();
    w.insert(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardB",
        Constants.DEFAULT_JDBC_POOL_SIZE);

    verify(mockConnectionHelper).getConnection(MySqlDataWriter.buildConnectionKey(shardB()));
  }

  @Test
  public void testWrite_unknownShardThrows() {
    MySqlDataWriter w = writer();
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                w.insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "missing",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("missing");
  }

  @Test
  public void testWrite_emptyRowsIsNoOp() throws Exception {
    MySqlDataWriter w = writer();
    w.insert(ImmutableList.of(), simpleTable(), "shardA", Constants.DEFAULT_JDBC_POOL_SIZE);
    verifyNoInteractions(mockConnection, mockStatement);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testWrite_nullRowsIsNoOp() throws Exception {
    MySqlDataWriter w = writer();
    w.insert(null, simpleTable(), "shardA", Constants.DEFAULT_JDBC_POOL_SIZE);
    verifyNoInteractions(mockConnection, mockStatement);
  }

  @Test
  public void testWrite_nullTableThrows() {
    MySqlDataWriter w = writer();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            w.insert(
                ImmutableList.of(simpleRow(1L, "a")),
                null,
                "shardA",
                Constants.DEFAULT_JDBC_POOL_SIZE));
  }

  @Test
  public void testWrite_updateUsesSameUpsertSqlAsInsert() throws Exception {
    MySqlDataWriter insertWriter = writer();
    insertWriter.insert(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<String> insertSql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(insertSql.capture());

    // Reset the connection mock so the second call's prepareStatement can be captured cleanly.
    org.mockito.Mockito.reset(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

    MySqlDataWriter updateWriter = writer();
    updateWriter.update(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<String> updateSql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(updateSql.capture());

    // INSERT and UPDATE should produce the same upsert SQL so that transient retries are safe.
    assertThat(updateSql.getValue()).isEqualTo(insertSql.getValue());
    assertThat(updateSql.getValue()).startsWith("INSERT INTO");
    assertThat(updateSql.getValue()).contains("ON DUPLICATE KEY UPDATE");
  }

  @Test
  public void testWrite_deleteUsesDeleteSql() throws Exception {
    MySqlDataWriter w = writer();
    w.delete(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sql.capture());
    assertThat(sql.getValue()).startsWith("DELETE FROM");
    verify(mockStatement).setLong(1, 1L); // PK value.
  }

  @Test
  public void testWrite_connectionHelperReturnsNullThrows() throws Exception {
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(null);
    MySqlDataWriter w = writer();
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                w.insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("No MySQL connection");
  }

  @Test
  public void testWrite_connectionExceptionWrapped() throws Exception {
    when(mockConnectionHelper.getConnection(anyString()))
        .thenThrow(new ConnectionException(new SQLException("boom")));
    MySqlDataWriter w = writer();
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                w.insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("Failed to write to MySQL shard shardA");
  }

  @Test
  public void testWrite_initializesConnectionHelperOnFirstCall() throws Exception {
    MySqlDataWriter w = writer();
    w.insert(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<ConnectionHelperRequest> req =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(req.capture());
    assertThat(req.getValue().getShards()).hasSize(2);
    assertThat(req.getValue().getDriver()).isEqualTo(Constants.MYSQL_JDBC_DRIVER);
    assertThat(req.getValue().getMaxConnections()).isEqualTo(Constants.DEFAULT_JDBC_POOL_SIZE);
  }

  @Test
  public void testWrite_initializesConnectionHelperWithCustomPoolSize() throws Exception {
    MySqlDataWriter w = writer();
    int customPoolSize = 42;
    w.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", customPoolSize);
    ArgumentCaptor<ConnectionHelperRequest> req =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(req.capture());
    assertThat(req.getValue().getMaxConnections()).isEqualTo(customPoolSize);
  }

  @Test
  public void testWrite_skipsInitIfPoolAlreadyInitialized() throws Exception {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);
    MySqlDataWriter w = writer();
    w.insert(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testLoadShards_missingPathThrows() {
    MySqlDataWriter w = new MySqlDataWriter(null, mockShardFileReader, mockConnectionHelper);
    assertThrows(
        IllegalArgumentException.class,
        () -> w.ensureInitialized(Constants.DEFAULT_JDBC_POOL_SIZE));

    MySqlDataWriter w2 = new MySqlDataWriter("", mockShardFileReader, mockConnectionHelper);
    assertThrows(
        IllegalArgumentException.class,
        () -> w2.ensureInitialized(Constants.DEFAULT_JDBC_POOL_SIZE));
  }

  @Test
  public void testLoadShards_emptyShardFileThrows() {
    when(mockShardFileReader.getOrderedShardDetails(anyString())).thenReturn(ImmutableList.of());
    MySqlDataWriter w = writer();
    assertThrows(
        RuntimeException.class, () -> w.ensureInitialized(Constants.DEFAULT_JDBC_POOL_SIZE));
  }

  @Test
  public void testResolveShard_emptyIdPicksFirst() {
    MySqlDataWriter w = writer();
    w.ensureInitialized(Constants.DEFAULT_JDBC_POOL_SIZE);
    Shard picked = w.resolveShard("");
    assertThat(picked.getLogicalShardId()).isEqualTo("shardA");

    Shard pickedNull = w.resolveShard(null);
    assertThat(pickedNull.getLogicalShardId()).isEqualTo("shardA");
  }

  @Test
  public void testResolveShard_beforeInitThrows() {
    MySqlDataWriter w = writer();
    assertThrows(IllegalStateException.class, () -> w.resolveShard("shardA"));
  }

  @Test
  public void testConstructor_defaultsToJdbcConnectionHelper() {
    // Basic sanity: public constructor should not blow up even if the file doesn't exist yet.
    MySqlDataWriter w = new MySqlDataWriter("not-used.json");
    // Close is a no-op by contract; calling it should not throw.
    w.close();
  }

  @Test
  public void testBuildConnectionKey_matchesExpectedFormat() {
    String key = MySqlDataWriter.buildConnectionKey(shardA());
    assertThat(key).isEqualTo(Constants.JDBC_MYSQL_URL_PREFIX + "hostA:3306/dbA/userA");
  }

  @Test
  public void testBuildConnectionKey_nullDbName() {
    Shard s = new Shard("id", "host", "3306", "user", "pass", null, null, null, null);
    String key = MySqlDataWriter.buildConnectionKey(s);
    assertThat(key).isEqualTo(Constants.JDBC_MYSQL_URL_PREFIX + "host:3306//user");
  }

  private static DataGeneratorColumn col(String name, LogicalType type) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isNullable(true)
        .isGenerated(false)
        .size(null)
        .precision(null)
        .scale(null)
        .build();
  }

  private static DataGeneratorTable singleColumnTable(DataGeneratorColumn c) {
    return DataGeneratorTable.builder()
        .name("t")
        .columns(ImmutableList.of(c))
        .primaryKeys(ImmutableList.of(c.name()))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private static Row singleValueRow(String field, Schema.FieldType type, Object value) {
    Schema schema = Schema.builder().addNullableField(field, type).build();
    return Row.withSchema(schema).addValue(value).build();
  }

  @Test
  public void testSetParameter_stringType() throws Exception {
    DataGeneratorColumn c = col("s", LogicalType.STRING);
    Row row = singleValueRow("s", Schema.FieldType.STRING, "hello");
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setString(1, "hello");
  }

  @Test
  public void testSetParameter_jsonType() throws Exception {
    DataGeneratorColumn c = col("j", LogicalType.JSON);
    Row row = singleValueRow("j", Schema.FieldType.STRING, "{\"a\":1}");
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setString(1, "{\"a\":1}");
  }

  @Test
  public void testSetParameter_int64Type() throws Exception {
    DataGeneratorColumn c = col("i", LogicalType.INT64);
    Row row = singleValueRow("i", Schema.FieldType.INT64, 42L);
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setLong(1, 42L);
  }

  @Test
  public void testSetParameter_float64Type() throws Exception {
    DataGeneratorColumn c = col("f", LogicalType.FLOAT64);
    Row row = singleValueRow("f", Schema.FieldType.DOUBLE, 3.14);
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setDouble(1, 3.14);
  }

  @Test
  public void testSetParameter_booleanType() throws Exception {
    DataGeneratorColumn c = col("b", LogicalType.BOOLEAN);
    Row row = singleValueRow("b", Schema.FieldType.BOOLEAN, true);
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setBoolean(1, true);
  }

  @Test
  public void testSetParameter_bytesType() throws Exception {
    DataGeneratorColumn c = col("bt", LogicalType.BYTES);
    byte[] bytes = "hi".getBytes(StandardCharsets.UTF_8);
    Row row = singleValueRow("bt", Schema.FieldType.BYTES, bytes);
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setBytes(1, bytes);
  }

  @Test
  public void testSetParameter_numericType_bigDecimal() throws Exception {
    DataGeneratorColumn c = col("n", LogicalType.NUMERIC);
    Row row = singleValueRow("n", Schema.FieldType.DECIMAL, new BigDecimal("1.23"));
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setBigDecimal(1, new BigDecimal("1.23"));
  }

  @Test
  public void testSetParameter_dateType_dateTime() throws Exception {
    DataGeneratorColumn c = col("d", LogicalType.DATE);
    DateTime dt = new DateTime(1_700_000_000_000L);
    Row row =
        Row.withSchema(Schema.builder().addNullableField("d", Schema.FieldType.DATETIME).build())
            .addValue(dt)
            .build();
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setDate(eq(1), any(java.sql.Date.class));
  }

  @Test
  public void testSetParameter_timestampType_dateTime() throws Exception {
    DataGeneratorColumn c = col("t", LogicalType.TIMESTAMP);
    DateTime dt = new DateTime(1_700_000_000_000L);
    Row row =
        Row.withSchema(Schema.builder().addNullableField("t", Schema.FieldType.DATETIME).build())
            .addValue(dt)
            .build();
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setTimestamp(eq(1), any(java.sql.Timestamp.class));
  }

  @Test
  public void testSetParameter_nullSetsTypedNull_string() throws Exception {
    DataGeneratorColumn c = col("s", LogicalType.STRING);
    Row row = singleValueRow("s", Schema.FieldType.STRING, null);
    writer()
        .setStatementParameters(
            mockStatement, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setNull(1, Types.VARCHAR);
  }

  @Test
  public void testSetParameter_nullSetsTypedNull_allTypes() throws Exception {
    int[] expectedJdbc = {
      Types.VARCHAR,
      Types.BIGINT,
      Types.DOUBLE,
      Types.BOOLEAN,
      Types.VARBINARY,
      Types.DATE,
      Types.TIMESTAMP,
      Types.NUMERIC,
      Types.VARCHAR,
      Types.VARCHAR
    };
    LogicalType[] types = LogicalType.values();
    // There are exactly as many expectedJdbc entries as there are LogicalType values.
    assertThat(types.length).isEqualTo(expectedJdbc.length);
    for (int i = 0; i < types.length; i++) {
      DataGeneratorColumn c = col("c", types[i]);
      Schema.FieldType beamType =
          types[i] == LogicalType.DATE || types[i] == LogicalType.TIMESTAMP
              ? Schema.FieldType.DATETIME
              : Schema.FieldType.STRING;
      Row row = singleValueRow("c", beamType, null);
      PreparedStatement stmt = mock(PreparedStatement.class);
      writer()
          .setStatementParameters(
              stmt, row, singleColumnTable(c), MySqlDataWriter.MutationType.INSERT);
      verify(stmt).setNull(1, expectedJdbc[i]);
    }
  }

  @Test
  public void testSetStatementParameters_updateBindsAllWritableColumns() throws Exception {
    // UPDATE is issued as the same upsert statement as INSERT, so parameters are bound in table
    // order (id then name) - not the old SET-first-then-WHERE ordering.
    DataGeneratorTable table = simpleTable();
    Row row = simpleRow(42L, "alice");
    writer().setStatementParameters(mockStatement, row, table, MySqlDataWriter.MutationType.UPDATE);

    verify(mockStatement).setLong(1, 42L);
    verify(mockStatement).setString(2, "alice");
  }

  @Test
  public void testSetStatementParameters_insertBindsAllWritableColumns() throws Exception {
    DataGeneratorTable table = simpleTable();
    Row row = simpleRow(42L, "alice");
    writer().setStatementParameters(mockStatement, row, table, MySqlDataWriter.MutationType.INSERT);

    verify(mockStatement).setLong(1, 42L);
    verify(mockStatement).setString(2, "alice");
  }

  @Test
  public void testSetStatementParameters_deleteOrdering() throws Exception {
    DataGeneratorTable table = simpleTable();
    Row row = simpleRow(42L, "alice");
    writer().setStatementParameters(mockStatement, row, table, MySqlDataWriter.MutationType.DELETE);

    // For DELETE: only PK columns.
    verify(mockStatement).setLong(1, 42L);
    verify(mockStatement, never()).setString(anyInt(), anyString());
  }

  @Test
  public void testSetStatementParameters_missingFieldBindsNull() throws Exception {
    // Row has no 'name' field; column 'name' should still bind a typed NULL.
    Schema schema = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    Row row = Row.withSchema(schema).addValues(1L).build();
    writer()
        .setStatementParameters(
            mockStatement, row, simpleTable(), MySqlDataWriter.MutationType.INSERT);
    verify(mockStatement).setLong(1, 1L);
    verify(mockStatement).setNull(2, Types.VARCHAR);
  }

  @Test
  public void testWrite_bigBatchAddsAllRows() throws Exception {
    MySqlDataWriter w = writer();
    List<Row> rows =
        ImmutableList.<Row>builder().addAll(Collections.nCopies(5, simpleRow(1L, "a"))).build();
    w.insert(rows, simpleTable(), "shardA", Constants.DEFAULT_JDBC_POOL_SIZE);
    verify(mockStatement, times(5)).addBatch();
    verify(mockStatement, atLeastOnce()).executeBatch();
  }

  @Test
  public void testBuildUpsertSql_allTypesOmitsGeneratedAndSkipped() {
    DataGeneratorTable table = allTypesTable();
    String sql = MySqlDataWriter.buildUpsertSql(table);
    // Should contain one placeholder for each supported LogicalType (and no generated/skipped).
    assertThat(sql).contains("int64_col");
    assertThat(sql).contains("string_col");
    assertThat(sql).doesNotContain("generated_col");
    assertThat(sql).doesNotContain("skipped_col");
    // All non-PK columns should show up in the ON DUPLICATE KEY UPDATE clause.
    assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
    assertThat(sql).contains("`string_col` = VALUES(`string_col`)");
    // The PK column (int64_col) should NOT appear in the SET clause.
    assertThat(sql).doesNotContain("`int64_col` = VALUES(`int64_col`)");
  }
}
