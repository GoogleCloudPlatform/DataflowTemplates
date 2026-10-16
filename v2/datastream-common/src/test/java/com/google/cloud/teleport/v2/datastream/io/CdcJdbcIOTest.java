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
package com.google.cloud.teleport.v2.datastream.io;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link CdcJdbcIO} class. */
@RunWith(JUnit4.class)
public final class CdcJdbcIOTest {

  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final String URL = "jdbc:mysql://localhost:3306/test";
  private static final String USERNAME = "testuser";
  private static final String PASSWORD = "testpass";
  private static final Integer LOGIN_TIMEOUT = 30;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * In-memory fault-injecting JDBC {@link Driver} used to test {@link BasicDataSource} connection
   * pooling, {@code testOnBorrow} validation, connection invalidation, and {@link Statement}
   * lifecycle across simulated Cloud SQL server restarts without requiring Docker.
   */
  public static final class FakeJdbcDriver implements Driver {
    private static final String PREFIX = "jdbc:fakecdc:";
    private static final AtomicInteger SERVER_EPOCH = new AtomicInteger(1);
    private static final AtomicInteger CREATED_CONNECTIONS = new AtomicInteger(0);
    private static final AtomicBoolean FAIL_NEXT_BATCH_WITH_RESTART = new AtomicBoolean(false);
    private static final List<String> COMMITTED_SQL =
        Collections.synchronizedList(new ArrayList<>());
    private static final List<Statement> CREATED_STATEMENTS =
        Collections.synchronizedList(new ArrayList<>());

    static {
      try {
        DriverManager.registerDriver(new FakeJdbcDriver());
      } catch (SQLException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    static void reset() {
      SERVER_EPOCH.set(1);
      CREATED_CONNECTIONS.set(0);
      FAIL_NEXT_BATCH_WITH_RESTART.set(false);
      COMMITTED_SQL.clear();
      CREATED_STATEMENTS.clear();
    }

    static void simulateServerRestart() {
      SERVER_EPOCH.incrementAndGet();
    }

    static void failNextBatchWithServerRestart() {
      FAIL_NEXT_BATCH_WITH_RESTART.set(true);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }
      final int connEpoch = SERVER_EPOCH.get();
      final int connId = CREATED_CONNECTIONS.incrementAndGet();
      final AtomicBoolean closed = new AtomicBoolean(false);
      final AtomicBoolean autoCommit = new AtomicBoolean(true);
      final List<String> pendingSql = new ArrayList<>();

      return (Connection)
          Proxy.newProxyInstance(
              FakeJdbcDriver.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                String name = method.getName();
                switch (name) {
                  case "equals":
                    return proxy == args[0];
                  case "hashCode":
                    return System.identityHashCode(proxy);
                  case "toString":
                    return "FakeConnection#" + connId + "@epoch" + connEpoch;
                  case "isClosed":
                    return closed.get();
                  case "close":
                    closed.set(true);
                    pendingSql.clear();
                    return null;
                  case "isValid":
                    return !closed.get() && SERVER_EPOCH.get() == connEpoch;
                  case "getAutoCommit":
                    return autoCommit.get();
                  case "setAutoCommit":
                    autoCommit.set((Boolean) args[0]);
                    return null;
                  case "commit":
                    if (closed.get() || SERVER_EPOCH.get() != connEpoch) {
                      throw new SQLException("Connection closed or reset", "08006");
                    }
                    COMMITTED_SQL.addAll(pendingSql);
                    pendingSql.clear();
                    return null;
                  case "rollback":
                    pendingSql.clear();
                    if (closed.get() || SERVER_EPOCH.get() != connEpoch) {
                      throw new SQLException("Connection closed or reset", "08006");
                    }
                    return null;
                  case "createStatement":
                    if (closed.get()) {
                      throw new SQLException("This connection has been closed.", "08003");
                    }
                    if (SERVER_EPOCH.get() != connEpoch) {
                      throw new SQLException(
                          "FATAL: terminating connection due to administrator command", "57P01");
                    }
                    Statement stmt = createStatementProxy(connEpoch, closed, pendingSql);
                    CREATED_STATEMENTS.add(stmt);
                    return stmt;
                  case "isReadOnly":
                  case "isWrapperFor":
                    return false;
                  case "getTransactionIsolation":
                    return Connection.TRANSACTION_READ_COMMITTED;
                  default:
                    return defaultReturnValue(method.getReturnType());
                }
              });
    }

    private static Statement createStatementProxy(
        int connEpoch, AtomicBoolean connClosed, List<String> pendingSql) {
      final AtomicBoolean stmtClosed = new AtomicBoolean(false);
      final List<String> batchSql = new ArrayList<>();
      return (Statement)
          Proxy.newProxyInstance(
              FakeJdbcDriver.class.getClassLoader(),
              new Class<?>[] {Statement.class},
              (proxy, method, args) -> {
                String name = method.getName();
                switch (name) {
                  case "equals":
                    return proxy == args[0];
                  case "hashCode":
                    return System.identityHashCode(proxy);
                  case "toString":
                    return "FakeStatement@epoch" + connEpoch;
                  case "close":
                    stmtClosed.set(true);
                    batchSql.clear();
                    return null;
                  case "isClosed":
                    return stmtClosed.get();
                  case "addBatch":
                    batchSql.add((String) args[0]);
                    return null;
                  case "executeBatch":
                    if (stmtClosed.get() || connClosed.get()) {
                      throw new SQLException("Statement or connection closed", "08003");
                    }
                    if (FAIL_NEXT_BATCH_WITH_RESTART.compareAndSet(true, false)) {
                      SERVER_EPOCH.incrementAndGet();
                    }
                    if (SERVER_EPOCH.get() != connEpoch) {
                      throw new SQLException(
                          "FATAL: terminating connection due to administrator command", "57P01");
                    }
                    pendingSql.addAll(batchSql);
                    int[] counts = new int[batchSql.size()];
                    batchSql.clear();
                    return counts;
                  case "executeUpdate":
                    if (stmtClosed.get() || connClosed.get()) {
                      throw new SQLException("Statement or connection closed", "08003");
                    }
                    if (SERVER_EPOCH.get() != connEpoch) {
                      throw new SQLException(
                          "FATAL: terminating connection due to administrator command", "57P01");
                    }
                    pendingSql.add((String) args[0]);
                    return 1;
                  default:
                    return defaultReturnValue(method.getReturnType());
                }
              });
    }

    private static Object defaultReturnValue(Class<?> returnType) {
      if (returnType == boolean.class) {
        return false;
      } else if (returnType == int.class) {
        return 0;
      } else if (returnType == long.class) {
        return 0L;
      }
      return null;
    }

    @Override
    public boolean acceptsURL(String url) {
      return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }
  }

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_staticValue() {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(LOGIN_TIMEOUT);

    assertNotNull(config.getLoginTimeout());
    assertEquals(LOGIN_TIMEOUT, config.getLoginTimeout().get());
  }

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_valueProvider() {
    ValueProvider<Integer> loginTimeoutProvider =
        ValueProvider.StaticValueProvider.of(LOGIN_TIMEOUT);

    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(loginTimeoutProvider);

    assertNotNull(config.getLoginTimeout());
    assertEquals(LOGIN_TIMEOUT, config.getLoginTimeout().get());
  }

  @Test
  public void testDataSourceConfiguration_buildDatasource_withLoginTimeout() throws SQLException {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(LOGIN_TIMEOUT);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // BasicDataSource.getLoginTimeout() throws UnsupportedOperationException,
    // so we verify that loginTimeout is included in connection properties instead
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      assertNotNull(connectionProperties);
      assertEquals(LOGIN_TIMEOUT.toString(), connectionProperties.getProperty("loginTimeout"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
    assertEquals(DRIVER_CLASS_NAME, basicDataSource.getDriverClassName());
    assertEquals(URL, basicDataSource.getUrl());
    assertEquals(USERNAME, basicDataSource.getUsername());
    assertEquals(PASSWORD, basicDataSource.getPassword());
    assertTrue(basicDataSource.getTestOnBorrow());
  }

  @Test
  public void testDataSourceConfiguration_buildDatasource_withoutLoginTimeout()
      throws SQLException {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // When no loginTimeout is set, connection properties should be null or empty
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      // Connection properties should be null or empty when no loginTimeout and no connection
      // properties are set
      if (connectionProperties != null) {
        assertEquals(0, connectionProperties.size());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
    assertEquals(DRIVER_CLASS_NAME, basicDataSource.getDriverClassName());
    assertEquals(URL, basicDataSource.getUrl());
    assertEquals(USERNAME, basicDataSource.getUsername());
    assertEquals(PASSWORD, basicDataSource.getPassword());
    assertTrue(basicDataSource.getTestOnBorrow());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataSourceConfiguration_withLoginTimeout_nullValue() {
    CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
        .withLoginTimeout((ValueProvider<Integer>) null);
  }

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_nullValueProvider() throws SQLException {
    ValueProvider<Integer> nullProvider = ValueProvider.StaticValueProvider.of(null);

    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withLoginTimeout(nullProvider);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // Note: BasicDataSource.getLoginTimeout() throws UnsupportedOperationException
    // so we cannot directly verify the login timeout value
  }

  @Test
  public void testDefaultDlqJsonFormatter_formatsValidJsonString() throws IOException {
    CdcJdbcIO.DefaultDlqJsonFormatter<String> formatter = new CdcJdbcIO.DefaultDlqJsonFormatter<>();
    String inputRecord = "{\"id\": 1, \"name\": \"test\"}";

    String result = formatter.apply(inputRecord);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode resultNode = mapper.readTree(result);

    // Verify strict JSON structure required by FileBasedDeadLetterQueueReconsumer
    // It must wrap the payload in "message"
    assertTrue(resultNode.has("message"));
    assertTrue(resultNode.get("message").isObject());
    assertEquals(1, resultNode.get("message").get("id").asInt());

    assertTrue(resultNode.has("error_message"));
    assertEquals("Failed insert in CdcJdbcIO", resultNode.get("error_message").asText());

    assertTrue(resultNode.has("timestamp"));
  }

  @Test
  public void testDefaultDlqJsonFormatter_formatsInvalidJsonString() throws IOException {
    CdcJdbcIO.DefaultDlqJsonFormatter<String> formatter = new CdcJdbcIO.DefaultDlqJsonFormatter<>();
    String inputRecord = "Not a JSON string";

    String result = formatter.apply(inputRecord);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode resultNode = mapper.readTree(result);

    assertTrue(resultNode.has("message"));
    // In the fallback case, "message" is just the string, not a nested object
    assertEquals("Not a JSON string", resultNode.get("message").asText());

    assertTrue(resultNode.has("error_message"));
    assertThat(resultNode.get("error_message").asText(), containsString("Serialization failed"));
  }

  @Test
  public void testWrite_buildsPipeline() {
    // Disable the check for pipeline.run() because we only want to verify
    // that the graph constructs successfully without executing it.
    pipeline.enableAbandonedNodeEnforcement(false);

    // This test ensures that the PTransform expands successfully without errors.
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);

    PCollection<String> input = pipeline.apply(Create.of("record1", "record2"));

    CdcJdbcIO.WriteResult result =
        input.apply(
            CdcJdbcIO.<String>write()
                .withDataSourceConfiguration(config)
                .withStatementFormatter(record -> "INSERT INTO table VALUES ('" + record + "')"));

    assertNotNull(result);
    assertNotNull(result.getFailedInserts());
  }

  @Test
  public void testRetryStrategy_defaultDetectsDeadlock() {
    CdcJdbcIO.RetryStrategy strategy = new CdcJdbcIO.DefaultRetryStrategy();

    SQLException deadlockException = new SQLException("Deadlock found", "40001");
    SQLException otherException = new SQLException("Syntax error", "42601");

    assertTrue("Should retry on 40001", strategy.apply(deadlockException));
    assertTrue("Should not retry on other states", !strategy.apply(otherException));
  }

  @Test
  public void testWriteFn_reconnectsWhenConnectionClosedDuringBatch() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection closedConnection = mock(Connection.class);
    Connection freshConnection = mock(Connection.class);
    Statement brokenStatement = mock(Statement.class);
    Statement freshStatement = mock(Statement.class);

    when(mockDataSource.getConnection()).thenReturn(closedConnection, freshConnection);
    when(closedConnection.isClosed()).thenReturn(false, true);
    when(closedConnection.createStatement()).thenReturn(brokenStatement);
    doThrow(new SQLException("This connection has been closed.", "08006"))
        .when(brokenStatement)
        .executeBatch();
    doThrow(new SQLException("This connection has been closed.", "08006"))
        .when(closedConnection)
        .rollback();

    when(freshConnection.isClosed()).thenReturn(false);
    when(freshConnection.createStatement()).thenReturn(freshStatement);
    when(freshStatement.executeBatch()).thenReturn(new int[] {1});

    SerializableFunction<Void, DataSource> providerFn = ignored -> mockDataSource;
    CdcJdbcIO.WriteVoid<String> spec =
        CdcJdbcIO.<String>writeVoid()
            .withDataSourceProviderFn(providerFn)
            .withStatementFormatter(record -> "INSERT INTO t VALUES ('" + record + "')")
            .withBatchSize(10L);

    CdcJdbcIO.WriteVoid.WriteFn<String> writeFn =
        new CdcJdbcIO.WriteVoid.WriteFn<>(spec, 2, 1L);

    @SuppressWarnings("unchecked")
    DoFn<String, Void>.ProcessContext processContext = mock(DoFn.ProcessContext.class);
    @SuppressWarnings("unchecked")
    DoFn<String, Void>.FinishBundleContext finishBundleContext =
        mock(DoFn.FinishBundleContext.class);
    BoundedWindow window = mock(BoundedWindow.class);

    when(processContext.element()).thenReturn("{\"id\": 1}");
    when(processContext.timestamp()).thenReturn(Instant.now());
    when(processContext.pane()).thenReturn(PaneInfo.NO_FIRING);

    writeFn.setup();
    writeFn.startBundle();
    writeFn.processElement(processContext, window);
    writeFn.finishBundle(finishBundleContext);

    verify(mockDataSource, times(2)).getConnection();
    verify(brokenStatement).close();
    verify(closedConnection).close();
    verify(freshStatement).addBatch("INSERT INTO t VALUES ('{\"id\": 1}')");
    verify(freshStatement).executeBatch();
    verify(freshStatement).close();
    verify(freshConnection).commit();
    verify(freshConnection).close();
    verify(finishBundleContext, never())
        .output(eq(CdcJdbcIO.WriteVoid.DLQ_TAG), any(), any(), any());
  }

  @Test
  public void testWriteFn_singleStatementMode_reusesConnectionOnDataErrorAndRoutesFailedRecordToDlq()
      throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection connection1 = mock(Connection.class);
    Statement batchStatement = mock(Statement.class);
    Statement singleStatementFail = mock(Statement.class);
    Statement singleStatementSuccess = mock(Statement.class);

    when(mockDataSource.getConnection()).thenReturn(connection1);
    when(connection1.isClosed()).thenReturn(false);
    when(connection1.createStatement())
        .thenReturn(batchStatement, singleStatementFail, singleStatementSuccess);
    doThrow(new SQLException("Batch execution failed: duplicate key", "23505"))
        .when(batchStatement)
        .executeBatch();
    doThrow(new SQLException("duplicate key value violates unique constraint", "23505"))
        .when(singleStatementFail)
        .executeUpdate("INSERT INTO t VALUES ('{\"id\": 1}')");
    when(singleStatementSuccess.executeUpdate("INSERT INTO t VALUES ('{\"id\": 2}')"))
        .thenReturn(1);

    SerializableFunction<Void, DataSource> providerFn = ignored -> mockDataSource;
    CdcJdbcIO.WriteVoid<String> spec =
        CdcJdbcIO.<String>writeVoid()
            .withDataSourceProviderFn(providerFn)
            .withStatementFormatter(record -> "INSERT INTO t VALUES ('" + record + "')")
            .withBatchSize(10L);

    // Even when maxRetries > 0, a deterministic data error (23505) on a healthy connection
    // transitions immediately to single-statement mode without tearing down connection1.
    CdcJdbcIO.WriteVoid.WriteFn<String> writeFn =
        new CdcJdbcIO.WriteVoid.WriteFn<>(spec, 3, 1L);

    @SuppressWarnings("unchecked")
    DoFn<String, Void>.ProcessContext processContext = mock(DoFn.ProcessContext.class);
    @SuppressWarnings("unchecked")
    DoFn<String, Void>.FinishBundleContext finishBundleContext =
        mock(DoFn.FinishBundleContext.class);
    BoundedWindow window = mock(BoundedWindow.class);

    when(processContext.element()).thenReturn("{\"id\": 1}", "{\"id\": 2}");
    when(processContext.timestamp()).thenReturn(Instant.now());
    when(processContext.pane()).thenReturn(PaneInfo.NO_FIRING);

    writeFn.setup();
    writeFn.startBundle();
    writeFn.processElement(processContext, window);
    writeFn.processElement(processContext, window);
    writeFn.finishBundle(finishBundleContext);

    // Only 1 connection is borrowed for the entire bundle: rolled back after batch failure and
    // after record 1's data error, reused for record 2, and closed once in finishBundle.
    verify(mockDataSource, times(1)).getConnection();
    verify(connection1, times(2)).rollback();
    verify(batchStatement).close();
    verify(singleStatementFail).close();
    verify(singleStatementSuccess).executeUpdate("INSERT INTO t VALUES ('{\"id\": 2}')");
    verify(singleStatementSuccess).close();
    verify(connection1).commit();
    verify(connection1, times(1)).close();
    verify(finishBundleContext, times(1))
        .output(eq(CdcJdbcIO.WriteVoid.DLQ_TAG), any(), any(), eq(window));
  }

  @Test
  public void testWriteFn_singleStatementMode_reconnectsAndRetriesRecordOnConnectionError()
      throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection connection1 = mock(Connection.class);
    Connection connection2 = mock(Connection.class);
    Statement batchStatement = mock(Statement.class);
    Statement record1BadDataStmt = mock(Statement.class);
    Statement record2ConnDropStmt = mock(Statement.class);
    Statement record2RetrySuccessStmt = mock(Statement.class);

    when(mockDataSource.getConnection()).thenReturn(connection1, connection2);

    when(connection1.isClosed()).thenReturn(false);
    when(connection1.createStatement())
        .thenReturn(batchStatement, record1BadDataStmt, record2ConnDropStmt);
    doThrow(new SQLException("Batch execution failed: duplicate key", "23505"))
        .when(batchStatement)
        .executeBatch();
    doThrow(new SQLException("duplicate key value violates unique constraint", "23505"))
        .when(record1BadDataStmt)
        .executeUpdate("INSERT INTO t VALUES ('{\"id\": 1}')");
    doThrow(new SQLException("FATAL: terminating connection due to administrator command", "57P01"))
        .when(record2ConnDropStmt)
        .executeUpdate("INSERT INTO t VALUES ('{\"id\": 2}')");

    when(connection2.isClosed()).thenReturn(false);
    when(connection2.createStatement()).thenReturn(record2RetrySuccessStmt);
    when(record2RetrySuccessStmt.executeUpdate("INSERT INTO t VALUES ('{\"id\": 2}')"))
        .thenReturn(1);

    SerializableFunction<Void, DataSource> providerFn = ignored -> mockDataSource;
    CdcJdbcIO.WriteVoid<String> spec =
        CdcJdbcIO.<String>writeVoid()
            .withDataSourceProviderFn(providerFn)
            .withStatementFormatter(record -> "INSERT INTO t VALUES ('" + record + "')")
            .withBatchSize(10L);

    CdcJdbcIO.WriteVoid.WriteFn<String> writeFn =
        new CdcJdbcIO.WriteVoid.WriteFn<>(spec, 1, 1L);

    @SuppressWarnings("unchecked")
    DoFn<String, Void>.ProcessContext processContext = mock(DoFn.ProcessContext.class);
    @SuppressWarnings("unchecked")
    DoFn<String, Void>.FinishBundleContext finishBundleContext =
        mock(DoFn.FinishBundleContext.class);
    BoundedWindow window = mock(BoundedWindow.class);

    when(processContext.element()).thenReturn("{\"id\": 1}", "{\"id\": 2}");
    when(processContext.timestamp()).thenReturn(Instant.now());
    when(processContext.pane()).thenReturn(PaneInfo.NO_FIRING);

    writeFn.setup();
    writeFn.startBundle();
    writeFn.processElement(processContext, window);
    writeFn.processElement(processContext, window);
    writeFn.finishBundle(finishBundleContext);

    // Record 1 (23505) goes to DLQ; Record 2 hits 57P01 on connection1, invalidates connection1,
    // reconnects on connection2, retries Record 2, and commits without losing Record 2 to DLQ.
    verify(mockDataSource, times(2)).getConnection();
    verify(connection1).close();
    verify(record2RetrySuccessStmt).executeUpdate("INSERT INTO t VALUES ('{\"id\": 2}')");
    verify(connection2).commit();
    verify(connection2).close();
    verify(finishBundleContext, times(1))
        .output(eq(CdcJdbcIO.WriteVoid.DLQ_TAG), any(), any(), eq(window));
  }

  @Test(expected = SQLException.class)
  public void testWriteFn_singleStatementMode_throwsWhenConnectionErrorRetriesExhausted()
      throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection brokenConnection = mock(Connection.class);
    Statement brokenStatement = mock(Statement.class);

    when(mockDataSource.getConnection()).thenReturn(brokenConnection);
    when(brokenConnection.isClosed()).thenReturn(false);
    when(brokenConnection.createStatement()).thenReturn(brokenStatement);
    doThrow(new SQLException("FATAL: terminating connection due to administrator command", "57P01"))
        .when(brokenStatement)
        .executeBatch();
    doThrow(new SQLException("FATAL: terminating connection due to administrator command", "57P01"))
        .when(brokenStatement)
        .executeUpdate(any());

    SerializableFunction<Void, DataSource> providerFn = ignored -> mockDataSource;
    CdcJdbcIO.WriteVoid<String> spec =
        CdcJdbcIO.<String>writeVoid()
            .withDataSourceProviderFn(providerFn)
            .withStatementFormatter(record -> "INSERT INTO t VALUES ('" + record + "')")
            .withBatchSize(10L);

    CdcJdbcIO.WriteVoid.WriteFn<String> writeFn =
        new CdcJdbcIO.WriteVoid.WriteFn<>(spec, 0, 1L);

    @SuppressWarnings("unchecked")
    DoFn<String, Void>.ProcessContext processContext = mock(DoFn.ProcessContext.class);
    @SuppressWarnings("unchecked")
    DoFn<String, Void>.FinishBundleContext finishBundleContext =
        mock(DoFn.FinishBundleContext.class);
    BoundedWindow window = mock(BoundedWindow.class);

    when(processContext.element()).thenReturn("{\"id\": 1}");
    when(processContext.timestamp()).thenReturn(Instant.now());
    when(processContext.pane()).thenReturn(PaneInfo.NO_FIRING);

    writeFn.setup();
    writeFn.startBundle();
    writeFn.processElement(processContext, window);
    writeFn.finishBundle(finishBundleContext);
  }

  private static class TestStatementFormatter implements CdcJdbcIO.StatementFormatter<String> {
    @Override
    public String formatStatement(String record) {
      return "INSERT INTO t VALUES ('" + record + "')";
    }
  }

  @Test
  public void testWriteFn_realBasicDataSourcePool_evictsDeadIdleAndActiveConnectionsAcrossRestarts()
      throws Exception {
    FakeJdbcDriver.reset();
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(
                FakeJdbcDriver.class.getName(), "jdbc:fakecdc:pool_test")
            .withMaxIdleConnections(2);
    BasicDataSource realPool =
        (BasicDataSource)
            CdcJdbcIO.DataSourceProviderFromDataSourceConfiguration.of(config).apply(null);
    try {
      CdcJdbcIO.WriteVoid<String> spec =
          CdcJdbcIO.<String>writeVoid()
              .withDataSourceConfiguration(config)
              .withStatementFormatter(new TestStatementFormatter())
              .withBatchSize(10L);
      CdcJdbcIO.WriteVoid.WriteFn<String> writeFn =
          SerializableUtils.ensureSerializable(new CdcJdbcIO.WriteVoid.WriteFn<>(spec, 2, 1L));

      @SuppressWarnings("unchecked")
      DoFn<String, Void>.ProcessContext processContext = mock(DoFn.ProcessContext.class);
      @SuppressWarnings("unchecked")
      DoFn<String, Void>.FinishBundleContext finishBundleContext =
          mock(DoFn.FinishBundleContext.class);
      BoundedWindow window = mock(BoundedWindow.class);
      when(processContext.timestamp()).thenReturn(Instant.now());
      when(processContext.pane()).thenReturn(PaneInfo.NO_FIRING);

      writeFn.setup();

      // 1. Bundle 1 initializes the pool (DBCP2 validateConnectionFactory creates & destroys 1
      // probe connection, then borrows 1 pooled connection) and returns the pooled connection to
      // the BasicDataSource idle pool on finishBundle().
      when(processContext.element()).thenReturn("{\"id\": 1}");
      writeFn.startBundle();
      writeFn.processElement(processContext, window);
      writeFn.finishBundle(finishBundleContext);
      int connectionsAfterBundle1 = FakeJdbcDriver.CREATED_CONNECTIONS.get();
      assertTrue(connectionsAfterBundle1 >= 1);
      assertEquals(0, realPool.getNumActive());
      assertEquals(1, realPool.getNumIdle());

      // 2. Simulate Cloud SQL restart while the pooled connection is sitting idle in DBCP2.
      // Because testOnBorrow=true is enabled, borrowing on Bundle 2 detects isValid()==false,
      // evicts the dead idle connection, and transparently creates 1 new connection.
      FakeJdbcDriver.simulateServerRestart();
      when(processContext.element()).thenReturn("{\"id\": 2}");
      writeFn.startBundle();
      writeFn.processElement(processContext, window);
      writeFn.finishBundle(finishBundleContext);
      int connectionsAfterBundle2 = FakeJdbcDriver.CREATED_CONNECTIONS.get();
      assertEquals(connectionsAfterBundle1 + 1, connectionsAfterBundle2);
      assertEquals(0, realPool.getNumActive());
      assertEquals(1, realPool.getNumIdle());

      // 3. Simulate Cloud SQL restart mid-bundle (after the connection is borrowed, during
      // executeBatch()). WriteFn catches 57P01, calls BasicDataSource.invalidateConnection()
      // so the broken connection is NOT returned to the idle pool, borrows 1 fresh connection,
      // and succeeds.
      FakeJdbcDriver.failNextBatchWithServerRestart();
      when(processContext.element()).thenReturn("{\"id\": 3}");
      writeFn.startBundle();
      writeFn.processElement(processContext, window);
      writeFn.finishBundle(finishBundleContext);
      int connectionsAfterBundle3 = FakeJdbcDriver.CREATED_CONNECTIONS.get();
      assertEquals(connectionsAfterBundle2 + 1, connectionsAfterBundle3);
      assertEquals(0, realPool.getNumActive());
      assertEquals(1, realPool.getNumIdle());

      // All 3 records were committed once and 0 records went to DLQ.
      assertEquals(3, FakeJdbcDriver.COMMITTED_SQL.size());
      verify(finishBundleContext, never())
          .output(eq(CdcJdbcIO.WriteVoid.DLQ_TAG), any(), any(), any());

      // Every Statement created across all bundles and retries was closed via try-with-resources.
      assertTrue(!FakeJdbcDriver.CREATED_STATEMENTS.isEmpty());
      for (Statement stmt : FakeJdbcDriver.CREATED_STATEMENTS) {
        assertTrue("Statement should be closed", stmt.isClosed());
      }
      writeFn.teardown();
    } finally {
      realPool.close();
    }
  }
}
