/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link CDCCorrectnessTestUtil}. */
@RunWith(JUnit4.class)
public class CDCCorrectnessTestUtilTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private CloudSqlResourceManager sourceDBResourceManager;
  @Mock private SpannerResourceManager spannerResourceManager;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private Random mockRandom;

  private CDCCorrectnessTestUtil testUtil;

  @Before
  public void setUp() throws SQLException {
    // Mock JDBC driver and connection
    when(sourceDBResourceManager.getUri()).thenReturn("jdbc:mysql://localhost:3306/test");
    when(sourceDBResourceManager.getUsername()).thenReturn("user");
    when(sourceDBResourceManager.getPassword()).thenReturn("password");
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
  }

  /**
   * Mocks the ExecutorService to run submitted tasks synchronously on the calling thread. This
   * avoids race conditions and simplifies testing of multi-threaded logic.
   */
  private void mockSynchronousExecutor(MockedStatic<Executors> mockedExecutors) {
    ExecutorService mockExecutor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return mock(Future.class);
            })
        .when(mockExecutor)
        .submit(any(Runnable.class));
    mockedExecutors.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(mockExecutor);
  }

  @Test
  public void testGenerateLoad_updatePath() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil(mockRandom);
    // Control random flow: 0.74 < 0.75, so choose update path
    when(mockRandom.nextDouble()).thenReturn(0.74);
    when(mockRandom.nextInt(anyInt())).thenReturn(0); // for unique ID and column selection

    try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class);
        MockedStatic<Executors> mockedExecutors = Mockito.mockStatic(Executors.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);
      mockSynchronousExecutor(mockedExecutors);

      testUtil.generateLoad(1, 1, sourceDBResourceManager);

      // Verify initial insert and one update
      verify(mockConnection, times(2)).prepareStatement(anyString());
      verify(mockPreparedStatement, times(2)).executeUpdate();
    }
  }

  @Test
  public void testGenerateLoad_deleteAndReinsertPath() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil(mockRandom);
    // Control random flow:
    // 1. 0.76 > 0.75 (delete)
    // 2. 0.74 < 0.75 (re-insert)
    when(mockRandom.nextDouble()).thenReturn(0.76, 0.74);
    when(mockRandom.nextInt(anyInt())).thenReturn(0); // for unique ID

    try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class);
        MockedStatic<Executors> mockedExecutors = Mockito.mockStatic(Executors.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);
      mockSynchronousExecutor(mockedExecutors);

      testUtil.generateLoad(1, 2, sourceDBResourceManager);

      // Verify initial insert, one delete, and one re-insert
      verify(mockConnection, times(3)).prepareStatement(anyString());
      verify(mockPreparedStatement, times(3)).executeUpdate();
    }
  }

  @Test
  public void testGenerateLoad_deleteAndDoNothingPath() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil(mockRandom);
    // Control random flow:
    // 1. 0.76 > 0.75 (delete)
    // 2. 0.76 > 0.76 (do nothing)
    when(mockRandom.nextDouble()).thenReturn(0.76, 0.76);
    when(mockRandom.nextInt(anyInt())).thenReturn(0); // for unique ID

    try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class);
        MockedStatic<Executors> mockedExecutors = Mockito.mockStatic(Executors.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);
      mockSynchronousExecutor(mockedExecutors);

      testUtil.generateLoad(1, 2, sourceDBResourceManager);

      // Verify initial insert and one delete. No second insert.
      verify(mockConnection, times(2)).prepareStatement(anyString());
      verify(mockPreparedStatement, times(2)).executeUpdate();
    }
  }

  @Test
  public void testGenerateLoad_handlesSqlExceptionInThread() {
    testUtil = new CDCCorrectnessTestUtil();
    SQLException sqlException = new SQLException("Connection failed");

    try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class);
        MockedStatic<Executors> mockedExecutors = Mockito.mockStatic(Executors.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenThrow(sqlException);
      mockSynchronousExecutor(mockedExecutors);

      // The method should catch the exception from the thread and wrap it.
      RuntimeException e =
          assertThrows(
              RuntimeException.class, () -> testUtil.generateLoad(1, 1, sourceDBResourceManager));

      assertThat(e).hasMessageThat().contains("Task execution failed");
      assertThat(e).hasCauseThat().isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  public void testAssertRows_whenRowsMatch_thenPass() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil();
    CDCCorrectnessTestUtil.User user = CDCCorrectnessTestUtil.User.generateRandom(1);

    // Mock source DB fetch
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt("id")).thenReturn(user.id);
    when(mockResultSet.getString("first_name")).thenReturn(user.firstName);
    when(mockResultSet.getString("last_name")).thenReturn(user.lastName);
    when(mockResultSet.getInt("age")).thenReturn(user.age);
    when(mockResultSet.getInt("status")).thenReturn(user.status ? 1 : 0);
    when(mockResultSet.getLong("col1")).thenReturn(user.col1);
    when(mockResultSet.getLong("col2")).thenReturn(user.col2);

    // Mock Spanner fetch
    Struct spannerStruct =
        Struct.newBuilder()
            .set("id")
            .to(user.id)
            .set("first_name")
            .to(user.firstName)
            .set("last_name")
            .to(user.lastName)
            .set("age")
            .to(user.age)
            .set("status")
            .to(user.status)
            .set("col1")
            .to(user.col1)
            .set("col2")
            .to(user.col2)
            .build();
    when(spannerResourceManager.runQuery(anyString())).thenReturn(ImmutableList.of(spannerStruct));

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);

      // Act & Assert: No exception should be thrown
      testUtil.assertRows(spannerResourceManager, sourceDBResourceManager);
    }
  }

  @Test
  public void testAssertRows_whenRowCountMismatch_thenFail() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil();
    CDCCorrectnessTestUtil.User user = CDCCorrectnessTestUtil.User.generateRandom(1);

    // Mock source DB to return one row
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt("id")).thenReturn(user.id);

    // Mock Spanner to return zero rows
    when(spannerResourceManager.runQuery(anyString())).thenReturn(ImmutableList.of());

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);

      AssertionError e =
          assertThrows(
              AssertionError.class,
              () -> testUtil.assertRows(spannerResourceManager, sourceDBResourceManager));
    }
  }

  @Test
  public void testAssertRows_whenRowContentMismatch_thenFail() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil();
    CDCCorrectnessTestUtil.User sourceUser = CDCCorrectnessTestUtil.User.generateRandom(1);
    sourceUser.firstName = "SourceFirstName";
    CDCCorrectnessTestUtil.User spannerUser = CDCCorrectnessTestUtil.User.generateRandom(1);
    spannerUser.firstName = "SpannerFirstName";

    // Mock source DB fetch
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt("id")).thenReturn(sourceUser.id);
    when(mockResultSet.getString("first_name")).thenReturn(sourceUser.firstName);

    // Mock Spanner fetch with different data
    Struct spannerStruct =
        Struct.newBuilder()
            .set("id")
            .to(spannerUser.id)
            .set("first_name")
            .to(spannerUser.firstName)
            .set("last_name")
            .to(spannerUser.lastName)
            .set("age")
            .to(spannerUser.age)
            .set("status")
            .to(spannerUser.status)
            .set("col1")
            .to(spannerUser.col1)
            .set("col2")
            .to(spannerUser.col2)
            .build();
    when(spannerResourceManager.runQuery(anyString())).thenReturn(ImmutableList.of(spannerStruct));

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);

      AssertionError e =
          assertThrows(
              AssertionError.class,
              () -> testUtil.assertRows(spannerResourceManager, sourceDBResourceManager));
    }
  }

  @Test
  public void testAssertRows_throwsRuntimeException_onSqlException() throws SQLException {
    testUtil = new CDCCorrectnessTestUtil();
    SQLException sqlException = new SQLException("DB connection failed");

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenThrow(sqlException);

      // Act & Assert: Should throw a RuntimeException wrapping the SQLException
      RuntimeException e =
          assertThrows(
              RuntimeException.class,
              () -> testUtil.assertRows(spannerResourceManager, sourceDBResourceManager));
      assertThat(e).hasCauseThat().isInstanceOf(SQLException.class);
    }
  }
}
