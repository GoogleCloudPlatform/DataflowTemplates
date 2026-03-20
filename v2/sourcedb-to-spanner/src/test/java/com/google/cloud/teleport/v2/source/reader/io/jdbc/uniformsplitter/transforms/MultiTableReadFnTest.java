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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiTableReadFnTest {

  @Test
  public void testExtractTableFromReadQuery_null() {
    assertThat(MultiTableReadFn.extractTableFromReadQuery(null)).isNull();
  }

  @Test
  public void testExtractTableFromReadQuery_variousFormats() {
    // Case 1: Simple SELECT with table name
    assertThat(MultiTableReadFn.extractTableFromReadQuery("SELECT * FROM mytable"))
        .isEqualTo(KV.of(null, "mytable"));

    // Case 2: SELECT with schema and table name
    assertThat(MultiTableReadFn.extractTableFromReadQuery("SELECT a, b FROM schema.mytable"))
        .isEqualTo(KV.of("schema", "mytable"));

    // Case 3: SELECT with backticks for schema and table
    assertThat(MultiTableReadFn.extractTableFromReadQuery("SELECT * FROM `db`.`mytable`"))
        .isEqualTo(KV.of("db", "mytable"));

    // Case 4: matchRead.find() is false - Invalid query format (missing FROM)
    assertThat(MultiTableReadFn.extractTableFromReadQuery("SELECT * mytable")).isNull();

    // Case 5: matchRead.find() is false - Invalid query format (not a SELECT)
    assertThat(MultiTableReadFn.extractTableFromReadQuery("INSERT INTO mytable VALUES (1)"))
        .isNull();

    // Case 6: matchRead.find() is false - Random text
    assertThat(MultiTableReadFn.extractTableFromReadQuery("INVALID QUERY")).isNull();

    // Case 7: matchRead.find() is false - Missing SELECT keyword
    assertThat(MultiTableReadFn.extractTableFromReadQuery("FROM mytable")).isNull();
  }

  @Test
  public void testReportLineage() throws Exception {
    String element = "someElement";
    Connection mockConnection = mock(Connection.class);
    DataSource mockDataSource = mock(DataSource.class);
    Lineage mockLineage = mock(Lineage.class);
    FQNComponents mockFqn = mock(FQNComponents.class);
    java.util.Set<KV<String, String>> reportedLineages = new java.util.HashSet<>();

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class);
        MockedStatic<FQNComponents> mockedFqnComponents = mockStatic(FQNComponents.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);

      // 1. Success path: schemaWithTable not null, fqn from DataSource not null, reportedLineages
      // adds.
      mockedFqnComponents.when(() -> FQNComponents.of(mockDataSource)).thenReturn(mockFqn);
      MultiTableReadAll.QueryProvider<String> queryProvider = el -> "SELECT * FROM schema1.table1";
      MultiTableReadFn.reportLineage(
          element,
          mockConnection,
          mockDataSource,
          StaticValueProvider.of(queryProvider),
          reportedLineages);

      verify(mockFqn).reportLineage(mockLineage, KV.of("schema1", "table1"));
      assertThat(reportedLineages).contains(KV.of("schema1", "table1"));

      // 2. Already reported path: reportedLineages.add(schemaWithTable) returns false.
      MultiTableReadFn.reportLineage(
          element,
          mockConnection,
          mockDataSource,
          StaticValueProvider.of(queryProvider),
          reportedLineages);
      // FQN.reportLineage should NOT be called again (it was called once in step 1)
      verify(mockFqn, times(1)).reportLineage(eq(mockLineage), any(KV.class));

      // 3. Fail to extract path: extractTableFromReadQuery returns null.
      MultiTableReadAll.QueryProvider<String> invalidQueryProvider = el -> "INVALID QUERY";
      MultiTableReadFn.reportLineage(
          element,
          mockConnection,
          mockDataSource,
          StaticValueProvider.of(invalidQueryProvider),
          reportedLineages);
      // FQN.reportLineage should NOT be called for invalid query.
      verify(mockFqn, times(1)).reportLineage(eq(mockLineage), any(KV.class));

      // 4. FQN from DataSource is null, FQN from Connection is not null.
      java.util.Set<KV<String, String>> reportedLineagesNew = new java.util.HashSet<>();
      mockedFqnComponents.when(() -> FQNComponents.of(mockDataSource)).thenReturn(null);
      mockedFqnComponents.when(() -> FQNComponents.of(mockConnection)).thenReturn(mockFqn);
      MultiTableReadFn.reportLineage(
          element,
          mockConnection,
          mockDataSource,
          StaticValueProvider.of(queryProvider),
          reportedLineagesNew);

      verify(mockFqn, times(2)).reportLineage(eq(mockLineage), eq(KV.of("schema1", "table1")));

      // 5. Both DataSource and Connection return null FQN.
      java.util.Set<KV<String, String>> reportedLineagesEmpty = new java.util.HashSet<>();
      mockedFqnComponents.when(() -> FQNComponents.of(mockDataSource)).thenReturn(null);
      mockedFqnComponents.when(() -> FQNComponents.of(mockConnection)).thenReturn(null);
      MultiTableReadFn.reportLineage(
          element,
          mockConnection,
          mockDataSource,
          StaticValueProvider.of(queryProvider),
          reportedLineagesEmpty);
      // FQN.reportLineage should NOT be called again (still 2 calls total from before)
      verify(mockFqn, times(2)).reportLineage(any(Lineage.class), any(KV.class));
    }
  }

  @Test
  public void testGetConnection() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    SerializableFunction<Void, DataSource> dataSourceProviderFn = (v) -> mockDataSource;
    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            dataSourceProviderFn,
            StaticValueProvider.of(new TestQueryProvider()),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(),
            (el) -> TableIdentifier.builder().setTableName("test").build(),
            true);

    readFn.setup();

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);

      // First call: connection is null, should initialize connection and report lineage
      Connection conn1 = readFn.getConnection("someElement");
      assertThat(conn1).isEqualTo(mockConnection);
      verify(mockConnection).setAutoCommit(false); // disableAutoCommit is true
      verify(mockDataSource, times(1)).getConnection();
      verify(mockLineage, times(1)).add(eq("mysql"), anyList());

      // Second call: connection is NOT null, should return existing connection and NOT report
      // lineage again
      Connection conn2 = readFn.getConnection("someElement");
      assertThat(conn2).isEqualTo(mockConnection);
      verify(mockDataSource, times(1)).getConnection(); // No additional call
      verify(mockLineage, times(1)).add(anyString(), anyList()); // No additional call
    }
  }

  @Test
  public void testGetConnection_lineageReportingVariations() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    SerializableFunction<Void, DataSource> dataSourceProviderFn = (v) -> mockDataSource;

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);

      // Path: schemaWithTable is NOT null, fqn is NOT null, reportedLineages.add returns true
      MultiTableReadFn<String, String> readFn1 =
          new MultiTableReadFn<>(
              dataSourceProviderFn,
              StaticValueProvider.of((el) -> "SELECT * FROM schema1.table1"),
              mock(JdbcIO.PreparedStatementSetter.class),
              ImmutableMap.of(),
              (el) -> TableIdentifier.builder().setTableName("table1").build(),
              false);
      readFn1.setup();
      readFn1.getConnection("el1");
      verify(mockLineage, times(1))
          .add(eq("mysql"), eq(List.of("localhost:3306", "testdb", "schema1", "table1")));

      // Path: schemaWithTable is null (invalid query)
      MultiTableReadFn<String, String> readFn2 =
          new MultiTableReadFn<>(
              dataSourceProviderFn,
              StaticValueProvider.of((el) -> "INVALID QUERY"),
              mock(JdbcIO.PreparedStatementSetter.class),
              ImmutableMap.of(),
              (el) -> TableIdentifier.builder().setTableName("table1").build(),
              false);
      readFn2.setup();
      readFn2.getConnection("el2");
      // Lineage.add should not be called more than once (from previous readFn1)
      verify(mockLineage, times(1)).add(anyString(), anyList());

      // Path: reportedLineages.add returns false (duplicate table)
      // We reuse readFn1 which already reported schema1.table1
      readFn1.getConnection("el1_again");
      verify(mockLineage, times(1)).add(anyString(), anyList());
    }
  }

  @Test
  public void testProcessElement_success() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    PreparedStatement mockStatement = mock(PreparedStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(
            anyString(), eq(ResultSet.TYPE_FORWARD_ONLY), eq(ResultSet.CONCUR_READ_ONLY)))
        .thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false); // 2 rows, then end
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setFetchSize(100)
            .setRowMapper(rs -> "row")
            .build();

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            v -> mockDataSource,
            StaticValueProvider.of(el -> "SELECT * FROM testTable"),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(tableId, spec),
            el -> tableId,
            false);

    readFn.setup();
    DoFn<String, String>.ProcessContext mockContext = mock(DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn("element");

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);
      readFn.processElement(mockContext);
    }

    verify(mockContext, times(2)).output("row");
    verify(mockStatement).setFetchSize(100);
    verify(mockResultSet, times(3)).next(); // 2 true, 1 false
  }

  @Test
  public void testProcessElement_noRows() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    PreparedStatement mockStatement = mock(PreparedStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(
            anyString(), eq(ResultSet.TYPE_FORWARD_ONLY), eq(ResultSet.CONCUR_READ_ONLY)))
        .thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false); // No rows
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setFetchSize(100)
            .setRowMapper(rs -> "row")
            .build();

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            v -> mockDataSource,
            StaticValueProvider.of(el -> "SELECT * FROM testTable"),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(tableId, spec),
            el -> tableId,
            false);

    readFn.setup();
    DoFn<String, String>.ProcessContext mockContext = mock(DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn("element");

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);
      readFn.processElement(mockContext);
    }

    verify(mockContext, times(0)).output(anyString());
    verify(mockResultSet, times(1)).next();
  }

  @Test
  public void testProcessElement_throwsOnSqlException() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenThrow(new java.sql.SQLException("SQL Error"));
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setFetchSize(100)
            .setRowMapper(rs -> "row")
            .build();

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            v -> mockDataSource,
            StaticValueProvider.of(el -> "SELECT * FROM testTable"),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(tableId, spec),
            el -> tableId,
            false);

    readFn.setup();
    DoFn<String, String>.ProcessContext mockContext = mock(DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn("element");

    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mock(Lineage.class));
      assertThrows(java.sql.SQLException.class, () -> readFn.processElement(mockContext));
    }
  }

  @Test
  public void testFinishBundle_and_TearDown() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            v -> mockDataSource,
            StaticValueProvider.of(el -> "SELECT * FROM test"),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(),
            el -> TableIdentifier.builder().setTableName("test").build(),
            false);

    readFn.setup();
    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);
      readFn.getConnection("element"); // initialize connection
    }

    readFn.finishBundle();
    verify(mockConnection, times(1)).close();

    // After finishBundle, connection should be null, so another call to tearDown shouldn't close it
    // again
    readFn.tearDown();
    verify(mockConnection, times(1)).close();
  }

  @Test
  public void testTearDown_twice() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    Lineage mockLineage = mock(Lineage.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            v -> mockDataSource,
            StaticValueProvider.of(el -> "SELECT * FROM test"),
            mock(JdbcIO.PreparedStatementSetter.class),
            ImmutableMap.of(),
            el -> TableIdentifier.builder().setTableName("test").build(),
            false);

    readFn.setup();
    try (MockedStatic<Lineage> mockedLineage = mockStatic(Lineage.class)) {
      mockedLineage.when(Lineage::getSources).thenReturn(mockLineage);
      readFn.getConnection("element"); // initialize connection
    }

    readFn.tearDown();
    readFn.tearDown(); // second call

    verify(mockConnection, times(1)).close();
  }

  @Test
  public void testProcessElement_throwsOnMissingSpec() throws Exception {
    SerializableFunction<Void, DataSource> mockProvider = mock(SerializableFunction.class);
    JdbcIO.PreparedStatementSetter<String> mockSetter = mock(JdbcIO.PreparedStatementSetter.class);
    TableIdentifier knownTable = TableIdentifier.builder().setTableName("knownTable").build();
    TableIdentifier unknownTable = TableIdentifier.builder().setTableName("unknownTable").build();

    MultiTableReadFn<String, String> readFn =
        new MultiTableReadFn<>(
            mockProvider,
            StaticValueProvider.of(new TestQueryProvider()),
            mockSetter,
            ImmutableMap.of(knownTable, mock(TableReadSpecification.class)),
            (element) -> unknownTable,
            true);

    DoFn<String, String>.ProcessContext mockContext = mock(DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn("someElement");

    assertThrows(RuntimeException.class, () -> readFn.processElement(mockContext));
  }

  private static class TestQueryProvider implements MultiTableReadAll.QueryProvider<String> {
    @Override
    public String getQuery(String element) throws Exception {
      return "SELECT * FROM test";
    }
  }
}
