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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SqlServerSinkConfig;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
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

/** Unit tests for {@link SqlServerDataWriter}. */
@RunWith(JUnit4.class)
public class SqlServerDataWriterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @SuppressWarnings("unchecked")
  @Mock
  private IConnectionHelper<Connection> mockConnectionHelper;

  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockStatement;

  private Shard shardA() {
    return new Shard("shardA", "hostA", "1433", "userA", "passA", "dbA", null, null, null);
  }

  private Shard shardB() {
    return new Shard("shardB", "hostB", "1433", "userB", "passB", "dbB", null, null, null);
  }

  @Before
  public void setUp() throws Exception {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
  }

  private SqlServerDataWriter writer() {
    return new SqlServerDataWriter(
        new SqlServerSinkConfig(ImmutableList.of(shardA(), shardB())), mockConnectionHelper);
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

  private DataGeneratorTable simpleTable() {
    return DataGeneratorTable.builder()
        .name("users")
        .columns(ImmutableList.of(col("id", LogicalType.INT64), col("name", LogicalType.STRING)))
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
      cols.add(col(t.name().toLowerCase() + "_col", t));
    }
    cols.add(col("generated_col", LogicalType.STRING).toBuilder().isGenerated(true).build());
    cols.add(col("skipped_col", LogicalType.STRING).toBuilder().isSkipped(true).build());
    return simpleTable().toBuilder()
        .name("all_types")
        .columns(cols.build())
        .primaryKeys(ImmutableList.of("int64_col"))
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
  public void testBuildUpsertSql_simple() {
    String sql = SqlServerDataWriter.buildUpsertSql(simpleTable());
    assertThat(sql)
        .isEqualTo(
            "MERGE INTO [users] AS target "
                + "USING (SELECT ? AS [id], ? AS [name]) AS source "
                + "ON (target.[id] = source.[id]) "
                + "WHEN MATCHED THEN UPDATE SET target.[name] = source.[name] "
                + "WHEN NOT MATCHED THEN INSERT ([id], [name]) "
                + "VALUES (source.[id], source.[name]);");
  }

  @Test
  public void testBuildUpsertSql_pkOnlyTableOmitsMatchedClause() {
    DataGeneratorTable table =
        simpleTable().toBuilder().columns(ImmutableList.of(col("id", LogicalType.INT64))).build();
    String sql = SqlServerDataWriter.buildUpsertSql(table);
    assertThat(sql).doesNotContain("WHEN MATCHED");
    assertThat(sql).endsWith("WHEN NOT MATCHED THEN INSERT ([id]) VALUES (source.[id]);");
  }

  @Test
  public void testBuildUpsertSql_multiPk() {
    DataGeneratorTable table =
        simpleTable().toBuilder()
            .columns(
                ImmutableList.of(
                    col("id", LogicalType.INT64),
                    col("name", LogicalType.STRING),
                    col("age", LogicalType.INT64)))
            .primaryKeys(ImmutableList.of("id", "name"))
            .build();
    String sql = SqlServerDataWriter.buildUpsertSql(table);
    assertThat(sql).contains("ON (target.[id] = source.[id] AND target.[name] = source.[name])");
    assertThat(sql).contains("WHEN MATCHED THEN UPDATE SET target.[age] = source.[age] ");
  }

  @Test
  public void testBuildUpsertSql_identifierWithBracketEscaped() {
    DataGeneratorTable table =
        simpleTable().toBuilder()
            .columns(ImmutableList.of(col("weird]col", LogicalType.STRING)))
            .primaryKeys(ImmutableList.of("weird]col"))
            .build();
    String sql = SqlServerDataWriter.buildUpsertSql(table);
    assertThat(sql).contains("SELECT ? AS [weird]]col]");
    assertThat(sql).contains("ON (target.[weird]]col] = source.[weird]]col])");
  }

  @Test
  public void testBuildUpsertSql_noWritableColumnsThrows() {
    DataGeneratorTable table =
        simpleTable().toBuilder()
            .columns(
                ImmutableList.of(
                    col("g", LogicalType.STRING).toBuilder().isGenerated(true).build()))
            .build();
    assertThrows(IllegalArgumentException.class, () -> SqlServerDataWriter.buildUpsertSql(table));
  }

  @Test
  public void testBuildUpsertSql_noPkThrows() {
    DataGeneratorTable table = simpleTable().toBuilder().primaryKeys(ImmutableList.of()).build();
    assertThrows(IllegalStateException.class, () -> SqlServerDataWriter.buildUpsertSql(table));
  }

  @Test
  public void testBuildDeleteSql() {
    assertThat(SqlServerDataWriter.buildDeleteSql(simpleTable()))
        .isEqualTo("DELETE FROM [users] WHERE [id] = ?");
  }

  @Test
  public void testBuildDeleteSql_noPkThrows() {
    DataGeneratorTable table = simpleTable().toBuilder().primaryKeys(ImmutableList.of()).build();
    assertThrows(IllegalStateException.class, () -> SqlServerDataWriter.buildDeleteSql(table));
  }

  @Test
  public void testBuildConnectionUrl_defaults() {
    assertThat(SqlServerDataWriter.buildConnectionUrl(shardA()))
        .isEqualTo(
            "jdbc:sqlserver://hostA:1433;databaseName=dbA;trustServerCertificate=true;encrypt=false");
  }

  @Test
  public void testBuildConnectionUrl_respectsUserProperties() {
    Shard shard = shardA();
    shard.setConnectionProperties("encrypt=true;trustServerCertificate=false;loginTimeout=30");
    assertThat(SqlServerDataWriter.buildConnectionUrl(shard))
        .isEqualTo(
            "jdbc:sqlserver://hostA:1433;databaseName=dbA;"
                + "encrypt=true;trustServerCertificate=false;loginTimeout=30");
  }

  @Test
  public void testBuildConnectionUrl_noDbName() {
    Shard shard = new Shard("s", "h", "1433", "u", "p", "", null, null, null);
    assertThat(SqlServerDataWriter.buildConnectionUrl(shard))
        .isEqualTo("jdbc:sqlserver://h:1433;trustServerCertificate=true;encrypt=false");
  }

  @Test
  public void testBuildConnectionKey() {
    assertThat(SqlServerDataWriter.buildConnectionKey(shardA()))
        .isEqualTo(SqlServerDataWriter.buildConnectionUrl(shardA()) + "/userA");
  }

  @Test
  public void testWrite_insertRunsPlainInsertOnFirstShard() throws Exception {
    SqlServerDataWriter w = writer();
    w.insert(
        ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")),
        simpleTable(),
        "",
        Constants.DEFAULT_JDBC_POOL_SIZE);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).isEqualTo("INSERT INTO [users] ([id], [name]) VALUES (?, ?)");

    verify(mockConnectionHelper).getConnection(SqlServerDataWriter.buildConnectionKey(shardA()));
    verify(mockStatement, times(2)).addBatch();
    verify(mockStatement).executeBatch();
    verify(mockStatement).close();
    verify(mockConnection).close();
  }

  @Test
  public void testWrite_routesToNamedShard() throws Exception {
    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a")),
            simpleTable(),
            "shardB",
            Constants.DEFAULT_JDBC_POOL_SIZE);
    verify(mockConnectionHelper).getConnection(SqlServerDataWriter.buildConnectionKey(shardB()));
  }

  @Test
  public void testWrite_unknownShardThrows() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                writer()
                    .insert(
                        ImmutableList.of(simpleRow(1L, "a")),
                        simpleTable(),
                        "missing",
                        Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("missing");
  }

  @Test
  public void testWrite_emptyRowsIsNoOp() throws Exception {
    writer().insert(ImmutableList.of(), simpleTable(), "shardA", Constants.DEFAULT_JDBC_POOL_SIZE);
    verifyNoInteractions(mockConnection, mockStatement);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testWrite_nullTableThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            writer()
                .insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    null,
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
  }

  @Test
  public void testWrite_updateUsesMergeSql() throws Exception {
    writer()
        .update(
            ImmutableList.of(simpleRow(1L, "a")),
            simpleTable(),
            "shardA",
            Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sql.capture());
    assertThat(sql.getValue()).startsWith("MERGE INTO [users] AS target");
    assertThat(sql.getValue()).isEqualTo(SqlServerDataWriter.buildUpsertSql(simpleTable()));
  }

  @Test
  public void testBuildInsertSql() {
    assertThat(SqlServerDataWriter.buildInsertSql(simpleTable()))
        .isEqualTo("INSERT INTO [users] ([id], [name]) VALUES (?, ?)");
    assertThat(SqlServerDataWriter.buildInsertSql(allTypesTable())).doesNotContain("generated_col");
    assertThat(SqlServerDataWriter.buildInsertSql(allTypesTable())).doesNotContain("skipped_col");
  }

  @Test
  public void testWrite_insertFallsBackToMergeOnDuplicateKey() throws Exception {
    PreparedStatement insertStmt = mock(PreparedStatement.class);
    PreparedStatement mergeStmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(SqlServerDataWriter.buildInsertSql(simpleTable())))
        .thenReturn(insertStmt);
    when(mockConnection.prepareStatement(SqlServerDataWriter.buildUpsertSql(simpleTable())))
        .thenReturn(mergeStmt);
    SQLException dup = new SQLException("Violation of PRIMARY KEY constraint", "23000", 2627);
    when(insertStmt.executeBatch()).thenThrow(new java.sql.BatchUpdateException(dup));

    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")),
            simpleTable(),
            "shardA",
            Constants.DEFAULT_JDBC_POOL_SIZE);

    verify(insertStmt).executeBatch();
    verify(mockConnection).rollback();
    verify(mergeStmt, times(2)).addBatch();
    verify(mergeStmt).executeBatch();
    verify(mockConnection).commit();
    // No retries were needed: a single connection acquisition.
    verify(mockConnectionHelper, times(1)).getConnection(anyString());
  }

  @Test
  public void testWrite_transientFailureIsRetriedThenSucceeds() throws Exception {
    SQLException deadlock = new SQLException("deadlock victim", "40001", 1205);
    when(mockStatement.executeBatch())
        .thenThrow(new java.sql.BatchUpdateException(deadlock))
        .thenReturn(new int[] {1});
    SqlServerDataWriter w =
        new SqlServerDataWriter(
            new SqlServerSinkConfig(ImmutableList.of(shardA(), shardB())), mockConnectionHelper) {
          @Override
          protected void sleepBeforeRetry(int attempt) {}
        };
    w.insert(
        ImmutableList.of(simpleRow(1L, "a")),
        simpleTable(),
        "shardA",
        Constants.DEFAULT_JDBC_POOL_SIZE);
    verify(mockConnectionHelper, times(2)).getConnection(anyString());
    verify(mockStatement, times(2)).executeBatch();
    verify(mockConnection).rollback();
    verify(mockConnection).commit();
  }

  @Test
  public void testWrite_transientFailureExhaustsRetries() throws Exception {
    SQLException lockTimeout = new SQLException("lock request time out", "HYT00", 1222);
    when(mockStatement.executeBatch()).thenThrow(new java.sql.BatchUpdateException(lockTimeout));
    SqlServerDataWriter w =
        new SqlServerDataWriter(
            new SqlServerSinkConfig(ImmutableList.of(shardA(), shardB())), mockConnectionHelper) {
          @Override
          protected void sleepBeforeRetry(int attempt) {}
        };
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                w.insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("Failed to write to SQL Server shard shardA");
    verify(mockConnectionHelper, times(SqlServerDataWriter.MAX_ATTEMPTS))
        .getConnection(anyString());
    verify(mockConnection, never()).commit();
  }

  @Test
  public void testWrite_nonTransientFailureIsNotRetried() throws Exception {
    SQLException conversion = new SQLException("Conversion failed", "22018", 245);
    when(mockStatement.executeBatch()).thenThrow(new java.sql.BatchUpdateException(conversion));
    assertThrows(
        RuntimeException.class,
        () ->
            writer()
                .insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    verify(mockConnectionHelper, times(1)).getConnection(anyString());
  }

  @Test
  public void testErrorClassification() {
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "23000", 2627))).isTrue();
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "23000", 2601))).isTrue();
    assertThat(
            SqlServerDataWriter.isDuplicateKey(
                new java.sql.BatchUpdateException(new SQLException("x", "23000", 2627))))
        .isTrue();
    // Bulk-copy path: no vendor code, message only.
    assertThat(
            SqlServerDataWriter.isDuplicateKey(
                new java.sql.BatchUpdateException(
                    new SQLException(
                        "Violation of PRIMARY KEY constraint 'PK_T'. Cannot insert duplicate key",
                        null,
                        0))))
        .isTrue();
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "40001", 1205))).isFalse();
    assertThat(SqlServerDataWriter.isTransient(new SQLException("x", "40001", 1205))).isTrue();
    assertThat(SqlServerDataWriter.isTransient(new SQLException("x", "HYT00", 1222))).isTrue();
    assertThat(
            SqlServerDataWriter.isTransient(
                new ConnectionException(
                    new java.sql.SQLTransientConnectionException("pool timeout"))))
        .isTrue();
    assertThat(SqlServerDataWriter.isTransient(new SQLException("x", "23000", 2627))).isFalse();
  }

  @Test
  public void testWrite_deleteUsesDeleteSql() throws Exception {
    writer()
        .delete(
            ImmutableList.of(simpleRow(1L, "a")),
            simpleTable(),
            "shardA",
            Constants.DEFAULT_JDBC_POOL_SIZE);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sql.capture());
    assertThat(sql.getValue()).startsWith("DELETE FROM");
    verify(mockStatement).setLong(1, 1L);
    verify(mockStatement, never()).setString(eq(2), anyString());
  }

  @Test
  public void testWrite_batchRunsInSingleTransaction() throws Exception {
    when(mockConnection.getAutoCommit()).thenReturn(true);
    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")),
            simpleTable(),
            "shardA",
            Constants.DEFAULT_JDBC_POOL_SIZE);
    org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(mockConnection, mockStatement);
    inOrder.verify(mockConnection).setAutoCommit(false);
    inOrder.verify(mockStatement, times(2)).addBatch();
    inOrder.verify(mockStatement).executeBatch();
    inOrder.verify(mockConnection).commit();
    inOrder.verify(mockConnection).setAutoCommit(true);
    verify(mockConnection, never()).rollback();
  }

  @Test
  public void testWrite_batchFailureRollsBackAndRestoresAutoCommit() throws Exception {
    when(mockConnection.getAutoCommit()).thenReturn(true);
    when(mockStatement.executeBatch()).thenThrow(new SQLException("boom"));
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                writer()
                    .insert(
                        ImmutableList.of(simpleRow(1L, "a")),
                        simpleTable(),
                        "shardA",
                        Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("Failed to write to SQL Server shard shardA");
    verify(mockConnection).rollback();
    verify(mockConnection, never()).commit();
    verify(mockConnection).setAutoCommit(true);
  }

  @Test
  public void testWrite_connectionHelperReturnsNullThrows() throws Exception {
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(null);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                writer()
                    .insert(
                        ImmutableList.of(simpleRow(1L, "a")),
                        simpleTable(),
                        "shardA",
                        Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("No SQL Server connection");
  }

  @Test
  public void testWrite_connectionExceptionWrapped() throws Exception {
    when(mockConnectionHelper.getConnection(anyString()))
        .thenThrow(new ConnectionException(new SQLException("boom")));
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                writer()
                    .insert(
                        ImmutableList.of(simpleRow(1L, "a")),
                        simpleTable(),
                        "shardA",
                        Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("Failed to write to SQL Server shard shardA");
  }

  @Test
  public void testWrite_initializesConnectionHelperWithSqlServerDriver() throws Exception {
    writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 42);
    ArgumentCaptor<ConnectionHelperRequest> req =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(req.capture());
    assertThat(req.getValue().getShards()).hasSize(2);
    assertThat(req.getValue().getDriver()).isEqualTo(Constants.SQLSERVER_JDBC_DRIVER);
    assertThat(req.getValue().getMaxConnections()).isEqualTo(42);
    assertThat(req.getValue().getShards().get(0).getConnectionUrl())
        .isEqualTo(SqlServerDataWriter.buildConnectionUrl(shardA()));
  }

  @Test
  public void testWrite_skipsInitWhenPoolAlreadyInitialized() throws Exception {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);
    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a")),
            simpleTable(),
            "shardA",
            Constants.DEFAULT_JDBC_POOL_SIZE);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testWrite_emptyShardConfigThrows() {
    SqlServerDataWriter w =
        new SqlServerDataWriter(new SqlServerSinkConfig(ImmutableList.of()), mockConnectionHelper);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                w.insert(
                    ImmutableList.of(simpleRow(1L, "a")),
                    simpleTable(),
                    "shardA",
                    Constants.DEFAULT_JDBC_POOL_SIZE));
    assertThat(ex).hasMessageThat().contains("No shards found");
  }

  @Test
  public void testSetStatementParameters_allTypes() throws Exception {
    DataGeneratorTable table = allTypesTable();
    DateTime ts = new DateTime(1_700_000_000_000L);
    byte[] bytes = "xyz".getBytes(StandardCharsets.UTF_8);
    Schema.Builder sb = Schema.builder();
    sb.addNullableField("string_col", Schema.FieldType.STRING);
    sb.addNullableField("int64_col", Schema.FieldType.INT64);
    sb.addNullableField("float64_col", Schema.FieldType.DOUBLE);
    sb.addNullableField("boolean_col", Schema.FieldType.BOOLEAN);
    sb.addNullableField("bytes_col", Schema.FieldType.BYTES);
    sb.addNullableField("date_col", Schema.FieldType.DATETIME);
    sb.addNullableField("timestamp_col", Schema.FieldType.DATETIME);
    sb.addNullableField("numeric_col", Schema.FieldType.DECIMAL);
    sb.addNullableField("json_col", Schema.FieldType.STRING);
    sb.addNullableField("uuid_col", Schema.FieldType.STRING);
    Row row =
        Row.withSchema(sb.build())
            .addValues(
                "s",
                7L,
                1.5d,
                true,
                bytes,
                ts,
                ts,
                new BigDecimal("12.34"),
                "{\"a\":1}",
                "0f8fad5b-d9cb-469f-a165-70867728950e")
            .build();

    writer()
        .setStatementParameters(mockStatement, row, table, SqlServerDataWriter.MutationType.INSERT);

    // Column order follows LogicalType.values(); generated/skipped columns are excluded.
    verify(mockStatement).setString(1, "s");
    verify(mockStatement).setLong(2, 7L);
    verify(mockStatement).setDouble(3, 1.5d);
    verify(mockStatement).setBoolean(4, true);
    verify(mockStatement).setBytes(5, bytes);
    verify(mockStatement).setDate(6, new java.sql.Date(ts.getMillis()));
    verify(mockStatement).setTimestamp(7, new java.sql.Timestamp(ts.getMillis()));
    verify(mockStatement).setBigDecimal(8, new BigDecimal("12.34"));
    verify(mockStatement).setString(9, "{\"a\":1}");
    verify(mockStatement).setString(10, "0f8fad5b-d9cb-469f-a165-70867728950e");
    verify(mockStatement, never()).setString(eq(11), anyString());
  }

  @Test
  public void testSetStatementParameters_nullsUseSqlServerJdbcTypes() throws Exception {
    DataGeneratorTable table = allTypesTable();
    Schema.Builder sb = Schema.builder();
    for (LogicalType t : LogicalType.values()) {
      sb.addNullableField(t.name().toLowerCase() + "_col", Schema.FieldType.STRING);
    }
    Row.Builder rb = Row.withSchema(sb.build());
    for (int i = 0; i < LogicalType.values().length; i++) {
      rb.addValue(null);
    }
    writer()
        .setStatementParameters(
            mockStatement, rb.build(), table, SqlServerDataWriter.MutationType.INSERT);

    verify(mockStatement).setNull(1, Types.VARCHAR); // STRING
    verify(mockStatement).setNull(2, Types.BIGINT); // INT64
    verify(mockStatement).setNull(3, Types.DOUBLE); // FLOAT64
    verify(mockStatement).setNull(4, Types.BIT); // BOOLEAN
    verify(mockStatement).setNull(5, Types.VARBINARY); // BYTES
    verify(mockStatement).setNull(6, Types.DATE); // DATE
    verify(mockStatement).setNull(7, Types.TIMESTAMP); // TIMESTAMP
    verify(mockStatement).setNull(8, Types.NUMERIC); // NUMERIC
    verify(mockStatement).setNull(9, Types.VARCHAR); // JSON
    verify(mockStatement).setNull(10, Types.VARCHAR); // UUID
  }

  @Test
  public void testSetStatementParameters_missingFieldBindsNull() throws Exception {
    Schema schema = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    Row row = Row.withSchema(schema).addValue(5L).build();
    writer()
        .setStatementParameters(
            mockStatement, row, simpleTable(), SqlServerDataWriter.MutationType.UPDATE);
    verify(mockStatement).setLong(1, 5L);
    verify(mockStatement).setNull(2, Types.VARCHAR);
  }

  @Test
  public void testQuote() {
    assertThat(SqlServerDataWriter.quote("plain")).isEqualTo("[plain]");
    assertThat(SqlServerDataWriter.quote("a]b")).isEqualTo("[a]]b]");
    assertThat(SqlServerDataWriter.quote("Select")).isEqualTo("[Select]");
  }
}
