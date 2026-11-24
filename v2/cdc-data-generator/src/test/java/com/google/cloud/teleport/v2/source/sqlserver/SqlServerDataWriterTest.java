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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
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
  @Mock private PreparedStatement mockIdentityStatement;
  @Mock private ResultSet mockIdentityRs;
  @Mock private Statement mockPlainStatement;

  private Shard shardA() {
    return new Shard("shardA", "hostA", "1433", "userA", "passA", "db1", null, null, null);
  }

  private Shard shardB() {
    return new Shard("shardB", "hostB", "1433", "userB", "passB", "db2", "sales", null, null);
  }

  @Before
  public void setUp() throws Exception {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockConnection.prepareStatement(SqlServerDataWriter.COLUMN_INFO_QUERY))
        .thenReturn(mockIdentityStatement);
    when(mockIdentityStatement.executeQuery()).thenReturn(mockIdentityRs);
    when(mockIdentityRs.next()).thenReturn(false);
    when(mockConnection.createStatement()).thenReturn(mockPlainStatement);
  }

  private void identityColumn(String name) throws SQLException {
    when(mockIdentityRs.next()).thenReturn(true, false);
    when(mockIdentityRs.getString("name")).thenReturn(name);
    when(mockIdentityRs.getString("type_name")).thenReturn("int");
    when(mockIdentityRs.getBoolean("is_identity")).thenReturn(true);
  }

  private SqlServerDataWriter writer() {
    return new SqlServerDataWriter(
        new SqlServerSinkConfig(ImmutableList.of(shardA(), shardB())), mockConnectionHelper);
  }

  private static DataGeneratorColumn column(String name, LogicalType type) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isNullable(true)
        .isGenerated(false)
        .build();
  }

  private static DataGeneratorTable table(
      String name, List<DataGeneratorColumn> columns, List<String> pks) {
    return DataGeneratorTable.builder()
        .name(name)
        .columns(ImmutableList.copyOf(columns))
        .primaryKeys(ImmutableList.copyOf(pks))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private DataGeneratorTable simpleTable() {
    return table(
        "users",
        ImmutableList.of(column("id", LogicalType.INT64), column("name", LogicalType.STRING)),
        ImmutableList.of("id"));
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
  public void testBuildUpsertSql() {
    assertThat(SqlServerDataWriter.buildUpsertSql(simpleTable()))
        .isEqualTo(
            "MERGE INTO [users] WITH (HOLDLOCK) AS t USING (VALUES (?, ?)) AS s ([id], [name]) "
                + "ON (t.[id] = s.[id]) "
                + "WHEN MATCHED THEN UPDATE SET t.[name] = s.[name] "
                + "WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (s.[id], s.[name]);");
  }

  @Test
  public void testBuildUpsertSql_pkOnlyTableOmitsUpdate() {
    DataGeneratorTable t =
        table("k", ImmutableList.of(column("a", LogicalType.INT64)), ImmutableList.of("a"));
    assertThat(SqlServerDataWriter.buildUpsertSql(t))
        .isEqualTo(
            "MERGE INTO [k] WITH (HOLDLOCK) AS t USING (VALUES (?)) AS s ([a]) "
                + "ON (t.[a] = s.[a]) "
                + "WHEN NOT MATCHED THEN INSERT ([a]) VALUES (s.[a]);");
  }

  @Test
  public void testBuildUpsertSql_skipsGeneratedAndSkippedColumns() {
    DataGeneratorTable t =
        table(
            "t",
            ImmutableList.of(
                column("id", LogicalType.INT64),
                column("rv", LogicalType.BYTES).toBuilder().isGenerated(true).build(),
                column("vec", LogicalType.STRING).toBuilder().isSkipped(true).build(),
                column("v", LogicalType.STRING)),
            ImmutableList.of("id"));
    String sql = SqlServerDataWriter.buildUpsertSql(t);
    assertThat(sql).doesNotContain("[rv]");
    assertThat(sql).doesNotContain("[vec]");
    assertThat(sql).contains("AS s ([id], [v])");
  }

  @Test
  public void testBuildUpsertSql_multiPkAndNamespace() {
    DataGeneratorTable t =
        table(
            "orders",
            ImmutableList.of(
                column("tenant", LogicalType.INT64),
                column("id", LogicalType.INT64),
                column("amount", LogicalType.NUMERIC)),
            ImmutableList.of("tenant", "id"));
    String sql = SqlServerDataWriter.buildUpsertSql(t, "[sales]");
    assertThat(sql).startsWith("MERGE INTO [sales].[orders] WITH (HOLDLOCK) AS t");
    assertThat(sql).contains("ON (t.[tenant] = s.[tenant] AND t.[id] = s.[id])");
    assertThat(sql).contains("UPDATE SET t.[amount] = s.[amount]");
  }

  @Test
  public void testBuildUpsertSql_errors() {
    DataGeneratorTable noPk =
        table("t", ImmutableList.of(column("a", LogicalType.INT64)), ImmutableList.of());
    assertThrows(IllegalStateException.class, () -> SqlServerDataWriter.buildUpsertSql(noPk));

    DataGeneratorTable noWritable =
        table(
            "t",
            ImmutableList.of(column("a", LogicalType.INT64).toBuilder().isGenerated(true).build()),
            ImmutableList.of("a"));
    assertThrows(
        IllegalArgumentException.class, () -> SqlServerDataWriter.buildUpsertSql(noWritable));

    DataGeneratorTable generatedPk =
        table(
            "t",
            ImmutableList.of(
                column("a", LogicalType.BYTES).toBuilder().isGenerated(true).build(),
                column("b", LogicalType.STRING)),
            ImmutableList.of("a"));
    assertThrows(
        IllegalStateException.class, () -> SqlServerDataWriter.buildUpsertSql(generatedPk));
  }

  @Test
  public void testBuildDeleteSql() {
    assertThat(SqlServerDataWriter.buildDeleteSql(simpleTable()))
        .isEqualTo("DELETE FROM [users] WHERE [id] = ?");
    assertThat(SqlServerDataWriter.buildDeleteSql(simpleTable(), " dbo "))
        .isEqualTo("DELETE FROM [dbo].[users] WHERE [id] = ?");
    DataGeneratorTable noPk =
        table("t", ImmutableList.of(column("a", LogicalType.INT64)), ImmutableList.of());
    assertThrows(IllegalStateException.class, () -> SqlServerDataWriter.buildDeleteSql(noPk));
  }

  @Test
  public void testQuoteEscapesClosingBracket() {
    assertThat(SqlServerDataWriter.quote("a]b")).isEqualTo("[a]]b]");
    assertThat(SqlServerDataWriter.quote("order")).isEqualTo("[order]");
  }

  @Test
  public void testUnquoteIdentifier() {
    assertThat(SqlServerDataWriter.unquoteIdentifier("[dbo]")).isEqualTo("dbo");
    assertThat(SqlServerDataWriter.unquoteIdentifier("\"dbo\"")).isEqualTo("dbo");
    assertThat(SqlServerDataWriter.unquoteIdentifier("dbo")).isEqualTo("dbo");
    assertThat(SqlServerDataWriter.unquoteIdentifier("[")).isEqualTo("[");
  }

  @Test
  public void testBuildConnectionUrl() {
    assertThat(SqlServerDataWriter.buildConnectionUrl(shardA()))
        .isEqualTo(
            "jdbc:sqlserver://hostA:1433;databaseName=db1;trustServerCertificate=true;encrypt=false");
    Shard withProps =
        new Shard(
            "s",
            "h",
            "1433",
            "u",
            "p",
            "db",
            null,
            null,
            "encrypt=true;trustServerCertificate=false");
    assertThat(SqlServerDataWriter.buildConnectionUrl(withProps))
        .isEqualTo("jdbc:sqlserver://h:1433;databaseName=db");
    Shard noDb = new Shard("s", "h", "1433", "u", "p", null, null, null, null);
    assertThat(SqlServerDataWriter.buildConnectionUrl(noDb))
        .isEqualTo("jdbc:sqlserver://h:1433;trustServerCertificate=true;encrypt=false");
    assertThat(SqlServerDataWriter.buildConnectionKey(shardA())).endsWith(";encrypt=false/userA");
  }

  @Test
  public void testInsert_initializesPoolAndExecutesBatch() throws Exception {
    SqlServerDataWriter writer = writer();
    writer.insert(
        ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, null)), simpleTable(), "shardA", 7);

    ArgumentCaptor<ConnectionHelperRequest> captor =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(captor.capture());
    assertThat(captor.getValue().getDriver()).isEqualTo(Constants.SQLSERVER_JDBC_DRIVER);
    assertThat(captor.getValue().getMaxConnections()).isEqualTo(7);

    verify(mockConnectionHelper).getConnection(SqlServerDataWriter.buildConnectionKey(shardA()));
    verify(mockConnection).prepareStatement(SqlServerDataWriter.buildInsertSql(simpleTable()));
    verify(mockStatement).setLong(1, 1L);
    verify(mockStatement).setString(2, "a");
    verify(mockStatement).setLong(1, 2L);
    verify(mockStatement).setNull(2, Types.VARCHAR);
    verify(mockStatement, times(2)).addBatch();
    verify(mockStatement).executeBatch();
    // No IDENTITY column -> no IDENTITY_INSERT.
    verify(mockPlainStatement, never()).execute(anyString());
  }

  @Test
  public void testUpdateAndDelete_useNamespaceOfShard() throws Exception {
    SqlServerDataWriter writer = writer();
    writer.update(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardB", 1);
    verify(mockConnection)
        .prepareStatement(SqlServerDataWriter.buildUpsertSql(simpleTable(), "sales"));

    writer.delete(ImmutableList.of(simpleRow(5L, "x")), simpleTable(), "shardB", 1);
    verify(mockConnection).prepareStatement("DELETE FROM [sales].[users] WHERE [id] = ?");
    verify(mockStatement).setLong(1, 5L);
  }

  @Test
  public void testInsert_identityPrimaryKeyUsesIdentityInsert() throws Exception {
    identityColumn("id");

    SqlServerDataWriter writer = writer();
    writer.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);
    writer.insert(ImmutableList.of(simpleRow(2L, "b")), simpleTable(), "shardA", 1);

    verify(mockIdentityStatement).setString(1, "[users]");
    // Lookup is cached per connection key + table.
    verify(mockIdentityStatement, times(1)).executeQuery();
    InOrder order = inOrder(mockPlainStatement, mockStatement);
    order.verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] ON");
    order.verify(mockStatement).executeBatch();
    order.verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] OFF");
  }

  @Test
  public void testInsert_identityOffEvenWhenBatchFails() throws Exception {
    identityColumn("id");
    when(mockStatement.executeBatch()).thenThrow(new SQLException("dup"));

    SqlServerDataWriter writer = writer();
    assertThrows(
        RuntimeException.class,
        () -> writer.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
    verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] OFF");
  }

  @Test
  public void testBuildInsertSql() {
    assertThat(SqlServerDataWriter.buildInsertSql(simpleTable()))
        .isEqualTo("INSERT INTO [users] ([id], [name]) VALUES (?, ?)");
    assertThat(SqlServerDataWriter.buildInsertSql(simpleTable(), "[sales]"))
        .isEqualTo("INSERT INTO [sales].[users] ([id], [name]) VALUES (?, ?)");
    DataGeneratorTable t =
        table(
            "t",
            ImmutableList.of(
                column("id", LogicalType.INT64),
                column("rv", LogicalType.BYTES).toBuilder().isGenerated(true).build(),
                column("vec", LogicalType.STRING).toBuilder().isSkipped(true).build(),
                column("v", LogicalType.STRING)),
            ImmutableList.of("id"));
    assertThat(SqlServerDataWriter.buildInsertSql(t))
        .isEqualTo("INSERT INTO [t] ([id], [v]) VALUES (?, ?)");
    DataGeneratorTable noWritable =
        table(
            "t",
            ImmutableList.of(column("a", LogicalType.INT64).toBuilder().isGenerated(true).build()),
            ImmutableList.of("a"));
    assertThrows(
        IllegalArgumentException.class, () -> SqlServerDataWriter.buildInsertSql(noWritable));
  }

  /** Separate statements for the INSERT and the MERGE fallback of {@link #simpleTable()}. */
  private PreparedStatement stubInsertStatement() throws SQLException {
    PreparedStatement insert = org.mockito.Mockito.mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(SqlServerDataWriter.buildInsertSql(simpleTable())))
        .thenReturn(insert);
    return insert;
  }

  private PreparedStatement stubMergeStatement() throws SQLException {
    PreparedStatement merge = org.mockito.Mockito.mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(SqlServerDataWriter.buildUpsertSql(simpleTable())))
        .thenReturn(merge);
    return merge;
  }

  private static BatchUpdateException batchError(int errorCode) {
    return new BatchUpdateException("batch failed", "23000", errorCode, new int[] {1});
  }

  @Test
  public void testInsert_usesPlainInsertNotMerge() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();

    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")), simpleTable(), "shardA", 1);

    verify(insert, times(2)).addBatch();
    verify(insert).executeBatch();
    verify(mockConnection, never())
        .prepareStatement(SqlServerDataWriter.buildUpsertSql(simpleTable()));
    verify(merge, never()).executeBatch();
  }

  @Test
  public void testUpdate_stillUsesMerge() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    writer().update(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);
    verify(merge).executeBatch();
    verify(insert, never()).executeBatch();
  }

  @Test
  public void testInsert_duplicateKey2627FallsBackToMergeForWholeBatch() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    when(insert.executeBatch()).thenThrow(batchError(2627));

    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b"), simpleRow(3L, "c")),
            simpleTable(),
            "shardA",
            1);

    // Every row of the batch is re-bound and re-run through the MERGE.
    verify(merge).setLong(1, 1L);
    verify(merge).setLong(1, 2L);
    verify(merge).setLong(1, 3L);
    verify(merge, times(3)).addBatch();
    verify(merge).executeBatch();
  }

  @Test
  public void testInsert_duplicateKey2601InNextExceptionFallsBackToMerge() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    BatchUpdateException error = batchError(0);
    error.setNextException(new SQLException("Cannot insert duplicate key row", "23000", 2601));
    when(insert.executeBatch()).thenThrow(error);

    writer()
        .insert(
            ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")), simpleTable(), "shardA", 1);

    verify(merge, times(2)).addBatch();
    verify(merge).executeBatch();
  }

  @Test
  public void testInsert_duplicateKeyInCauseFallsBackToMerge() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    BatchUpdateException error = batchError(0);
    error.initCause(new SQLException("Violation of PRIMARY KEY constraint", "23000", 2627));
    when(insert.executeBatch()).thenThrow(error);

    writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);

    verify(merge).executeBatch();
  }

  @Test
  public void testInsert_nonDuplicateErrorsAreNotSwallowed() throws Exception {
    // 547 = FOREIGN KEY / CHECK conflict, 515 = NULL into NOT NULL: same SQLState 23000.
    for (int code : new int[] {547, 515, 0}) {
      PreparedStatement insert = stubInsertStatement();
      PreparedStatement merge = stubMergeStatement();
      when(insert.executeBatch()).thenThrow(batchError(code));

      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () ->
                  writer()
                      .insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
      assertThat(ex).hasCauseThat().isInstanceOf(BatchUpdateException.class);
      verify(merge, never()).executeBatch();
    }
  }

  @Test
  public void testInsert_fallbackFailureIsThrown() throws Exception {
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    when(insert.executeBatch()).thenThrow(batchError(2627));
    when(merge.executeBatch()).thenThrow(new SQLException("deadlock", "40001", 1205));

    assertThrows(
        RuntimeException.class,
        () -> writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
    verify(merge).executeBatch();
  }

  @Test
  public void testUpdateAndDelete_duplicateKeyIsNotRetried() throws Exception {
    PreparedStatement merge = stubMergeStatement();
    when(merge.executeBatch()).thenThrow(batchError(2627));
    assertThrows(
        RuntimeException.class,
        () -> writer().update(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
    verify(merge, times(1)).executeBatch();
  }

  @Test
  public void testInsert_identityInsertWrapsInsertAndFallback() throws Exception {
    identityColumn("id");
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    when(insert.executeBatch()).thenThrow(batchError(2627));

    writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);

    InOrder order = inOrder(mockPlainStatement, insert, merge);
    order.verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] ON");
    order.verify(insert).executeBatch();
    order.verify(merge).executeBatch();
    order.verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] OFF");
    verify(mockPlainStatement, times(1)).execute("SET IDENTITY_INSERT [users] ON");
  }

  @Test
  public void testInsert_identityOffWhenFallbackFails() throws Exception {
    identityColumn("id");
    PreparedStatement insert = stubInsertStatement();
    PreparedStatement merge = stubMergeStatement();
    when(insert.executeBatch()).thenThrow(batchError(2601));
    when(merge.executeBatch()).thenThrow(new SQLException("boom", "S0001", 50000));

    assertThrows(
        RuntimeException.class,
        () -> writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
    InOrder order = inOrder(mockPlainStatement, merge);
    order.verify(merge).executeBatch();
    order.verify(mockPlainStatement).execute("SET IDENTITY_INSERT [users] OFF");
  }

  @Test
  public void testIsDuplicateKey() {
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "23000", 2627))).isTrue();
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "23000", 2601))).isTrue();
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("x", "23000", 547))).isFalse();
    assertThat(SqlServerDataWriter.isDuplicateKey(new SQLException("duplicate key", "23000", 0)))
        .isFalse();
    assertThat(
            SqlServerDataWriter.isDuplicateKey(
                new RuntimeException(new SQLException("x", "23000", 2627))))
        .isTrue();
    SQLException self = new SQLException("x", "23000", 0);
    self.setNextException(self);
    assertThat(SqlServerDataWriter.isDuplicateKey(self)).isFalse();
  }

  @Test
  public void testDelete_neverUsesIdentityInsert() throws Exception {
    identityColumn("id");
    writer().delete(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);
    verify(mockPlainStatement, never()).execute(anyString());
  }

  @Test
  public void testTimeColumnsBoundAsString() throws Exception {
    when(mockIdentityRs.next()).thenReturn(true, false);
    when(mockIdentityRs.getString("name")).thenReturn("Id");
    when(mockIdentityRs.getString("type_name")).thenReturn("time");
    when(mockIdentityRs.getBoolean("is_identity")).thenReturn(false);
    DataGeneratorTable t =
        table(
            "times",
            ImmutableList.of(column("Id", LogicalType.TIMESTAMP), column("v", LogicalType.INT64)),
            ImmutableList.of("Id"));
    Schema schema =
        Schema.builder()
            .addNullableField("Id", Schema.FieldType.DATETIME)
            .addNullableField("v", Schema.FieldType.INT64)
            .build();
    Instant instant = new Instant(1_700_000_000_123L);
    Row row = Row.withSchema(schema).addValues(instant, 4L).build();

    SqlServerDataWriter writer = writer();
    writer.insert(ImmutableList.of(row), t, "shardA", 1);
    writer.delete(ImmutableList.of(row), t, "shardA", 1);

    String expected = SqlServerDataWriter.formatTime(instant);
    assertThat(expected).matches("\\d{2}:\\d{2}:\\d{2}\\.123");
    verify(mockStatement, times(2)).setString(1, expected);
    verify(mockStatement, never()).setTimestamp(eq(1), any());
    verify(mockIdentityStatement, times(1)).executeQuery();
  }

  @Test
  public void testInsert_identityColumnNotWritten() throws Exception {
    identityColumn("seq");
    writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1);
    verify(mockPlainStatement, never()).execute(anyString());
  }

  @Test
  public void testWrite_emptyRowsIsNoOp() throws Exception {
    SqlServerDataWriter writer = writer();
    writer.insert(ImmutableList.of(), simpleTable(), "shardA", 1);
    writer.insert(null, simpleTable(), "shardA", 1);
    verify(mockConnectionHelper, never()).getConnection(anyString());
  }

  @Test
  public void testWrite_nullTableThrows() {
    SqlServerDataWriter writer = writer();
    assertThrows(
        IllegalArgumentException.class,
        () -> writer.insert(ImmutableList.of(simpleRow(1L, "a")), null, "shardA", 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> writer.update(ImmutableList.of(simpleRow(1L, "a")), null, "shardA", 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> writer.delete(ImmutableList.of(simpleRow(1L, "a")), null, "shardA", 1));
  }

  @Test
  public void testWrite_connectionFailuresWrapped() throws Exception {
    when(mockConnectionHelper.getConnection(anyString()))
        .thenThrow(new ConnectionException("pool"));
    assertThrows(
        RuntimeException.class,
        () -> writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
  }

  @Test
  public void testWrite_nullConnectionThrows() throws Exception {
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(null);
    assertThrows(
        RuntimeException.class,
        () -> writer().insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "shardA", 1));
  }

  @Test
  public void testResolveShard() {
    SqlServerDataWriter writer = writer();
    assertThrows(IllegalStateException.class, () -> writer.resolveShard("shardA"));
    writer.ensureInitialized(1);
    assertThat(writer.resolveShard(null).getLogicalShardId()).isEqualTo("shardA");
    assertThat(writer.resolveShard("").getLogicalShardId()).isEqualTo("shardA");
    assertThat(writer.resolveShard("shardB").getLogicalShardId()).isEqualTo("shardB");
    assertThrows(IllegalArgumentException.class, () -> writer.resolveShard("missing"));
  }

  @Test
  public void testEnsureInitialized_errors() {
    SqlServerDataWriter empty =
        new SqlServerDataWriter(new SqlServerSinkConfig(ImmutableList.of()), mockConnectionHelper);
    assertThrows(RuntimeException.class, () -> empty.ensureInitialized(1));

    SqlServerDataWriter duplicate =
        new SqlServerDataWriter(
            new SqlServerSinkConfig(ImmutableList.of(shardA(), shardA())), mockConnectionHelper);
    assertThrows(IllegalArgumentException.class, () -> duplicate.ensureInitialized(1));
  }

  @Test
  public void testEnsureInitialized_skipsInitWhenPoolReady() throws Exception {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);
    writer().ensureInitialized(1);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testSetParameter_allLogicalTypes() throws Exception {
    long millis = 1_700_000_000_123L;
    byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
    SqlServerDataWriter.setParameter(mockStatement, 1, column("c", LogicalType.INT64), 7L);
    SqlServerDataWriter.setParameter(mockStatement, 2, column("c", LogicalType.FLOAT64), 1.5d);
    SqlServerDataWriter.setParameter(mockStatement, 3, column("c", LogicalType.BOOLEAN), true);
    SqlServerDataWriter.setParameter(mockStatement, 4, column("c", LogicalType.BYTES), bytes);
    SqlServerDataWriter.setParameter(
        mockStatement, 5, column("c", LogicalType.NUMERIC), new BigDecimal("12.34"));
    SqlServerDataWriter.setParameter(mockStatement, 6, column("c", LogicalType.NUMERIC), "5.6");
    SqlServerDataWriter.setParameter(
        mockStatement, 7, column("c", LogicalType.DATE), new Instant(millis));
    SqlServerDataWriter.setParameter(
        mockStatement, 8, column("c", LogicalType.TIMESTAMP), new Instant(millis));
    SqlServerDataWriter.setParameter(mockStatement, 9, column("c", LogicalType.STRING), "s");
    SqlServerDataWriter.setParameter(mockStatement, 10, column("c", LogicalType.JSON), "{}");
    String uuid = "0f7df523-faba-4efa-bfa9-8c39cc7fa6a6";
    SqlServerDataWriter.setParameter(mockStatement, 11, column("c", LogicalType.UUID), uuid);

    verify(mockStatement).setLong(1, 7L);
    verify(mockStatement).setDouble(2, 1.5d);
    verify(mockStatement).setBoolean(3, true);
    verify(mockStatement).setBytes(4, bytes);
    verify(mockStatement).setBigDecimal(5, new BigDecimal("12.34"));
    verify(mockStatement).setBigDecimal(6, new BigDecimal("5.6"));
    verify(mockStatement).setDate(7, new java.sql.Date(millis));
    verify(mockStatement).setTimestamp(8, new java.sql.Timestamp(millis));
    verify(mockStatement).setString(9, "s");
    verify(mockStatement).setString(10, "{}");
    verify(mockStatement).setString(11, uuid);
  }

  @Test
  public void testSetParameter_nulls() throws Exception {
    SqlServerDataWriter.setParameter(mockStatement, 1, column("c", LogicalType.BOOLEAN), null);
    SqlServerDataWriter.setParameter(mockStatement, 2, column("c", LogicalType.INT64), null);
    SqlServerDataWriter.setParameter(mockStatement, 3, column("c", LogicalType.BYTES), null);
    SqlServerDataWriter.setParameter(mockStatement, 4, column("c", LogicalType.TIMESTAMP), null);
    SqlServerDataWriter.setParameter(mockStatement, 5, column("c", LogicalType.UUID), null);
    verify(mockStatement).setNull(1, Types.BIT);
    verify(mockStatement).setNull(2, Types.BIGINT);
    verify(mockStatement).setNull(3, Types.VARBINARY);
    verify(mockStatement).setNull(4, Types.TIMESTAMP);
    verify(mockStatement).setNull(5, Types.VARCHAR);
  }

  @Test
  public void testSetStatementParameters_deleteBindsPrimaryKeysOnly() throws Exception {
    writer()
        .setStatementParameters(
            mockStatement,
            simpleRow(9L, "z"),
            simpleTable(),
            SqlServerDataWriter.MutationType.DELETE);
    verify(mockStatement).setLong(1, 9L);
    verify(mockStatement, never()).setString(eq(2), anyString());
  }

  @Test
  public void testSetStatementParameters_missingFieldBindsNull() throws Exception {
    Schema schema = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    Row row = Row.withSchema(schema).addValues(3L).build();
    writer()
        .setStatementParameters(
            mockStatement, row, simpleTable(), SqlServerDataWriter.MutationType.INSERT);
    verify(mockStatement).setLong(1, 3L);
    verify(mockStatement).setNull(2, Types.VARCHAR);
  }

  @Test
  public void testClose_isNoOp() {
    writer().close();
  }
}
