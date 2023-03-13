/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** Unit tests for {@link com.google.cloud.teleport.it.jdbc.AbstractJDBCResourceManager}. */
@RunWith(JUnit4.class)
public class AbstractJDBCResourceManagerTest<T extends JdbcDatabaseContainer<T>> {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private JDBCDriverFactory driver;

  @Mock private T container;
  @Mock private Connection connection;
  @Mock private Statement statement;
  @Mock private ResultSet result;

  private static final String TEST_ID = "test_id";
  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "test-table";
  private static final String HOST = "localhost";
  private static final int JDBC_PORT = 1234;
  private static final int MAPPED_PORT = 4321;
  private static final String JDBC_PREFIX = "mysql";

  private AbstractJDBCResourceManager<T> testManager;

  @Before
  public void setUp() {
    when(container.withUsername(anyString())).thenReturn(container);
    when(container.withPassword(anyString())).thenReturn(container);
    when(container.withDatabaseName(anyString())).thenReturn(container);
    when(container.getDatabaseName()).thenReturn(DATABASE_NAME);

    testManager =
        new AbstractJDBCResourceManager<T>(
            container,
            new AbstractJDBCResourceManager.Builder<>(TEST_ID) {
              @Override
              public AbstractJDBCResourceManager<T> build() {
                return new AbstractJDBCResourceManager<>(container, this) {
                  @Override
                  protected int getJDBCPort() {
                    return JDBC_PORT;
                  }

                  @Override
                  public String getJDBCPrefix() {
                    return JDBC_PREFIX;
                  }
                };
              }
            },
            driver) {
          @Override
          protected int getJDBCPort() {
            return JDBC_PORT;
          }

          @Override
          public String getJDBCPrefix() {
            return JDBC_PREFIX;
          }
        };
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    assertThat(testManager.getUri())
        .matches("jdbc:" + JDBC_PREFIX + "://" + HOST + ":" + MAPPED_PORT + "/" + DATABASE_NAME);
  }

  @Test
  public void testGetDatabaseNameShouldReturnCorrectValue() {
    assertThat(testManager.getDatabaseName()).matches(DATABASE_NAME);
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableNameIsInvalid() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            testManager.createTable(
                "invalid/name",
                new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAlreadyExists() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    testManager.createTable(
        TABLE_NAME, new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id"));

    assertThrows(
        IllegalStateException.class,
        () ->
            testManager.createTable(
                TABLE_NAME,
                new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenDriverFailsToEstablishConnection()
      throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    doThrow(SQLException.class).when(driver).getConnection(any(), any(), any());

    assertThrows(
        JDBCResourceManagerException.class,
        () ->
            testManager.createTable(
                TABLE_NAME,
                new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenJDBCFailsToExecuteSQL() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doThrow(SQLException.class).when(statement).executeUpdate(anyString());

    assertThrows(
        JDBCResourceManagerException.class,
        () ->
            testManager.createTable(
                TABLE_NAME,
                new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id")));
  }

  @Test
  public void testCreateTableShouldReturnTrueIfJDBCDoesNotThrowAnyError() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    assertTrue(
        testManager.createTable(
            TABLE_NAME,
            new JDBCResourceManager.JDBCSchema(ImmutableMap.of("id", "INTEGER"), "id")));

    verify(statement).executeUpdate(anyString());
  }

  @Test
  public void testWriteShouldThrowErrorWhenDriverFailsToEstablishConnection() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    doThrow(SQLException.class).when(driver).getConnection(any(), any(), any());

    assertThrows(
        JDBCResourceManagerException.class, () -> testManager.write(TABLE_NAME, 0, "test"));
  }

  @Test
  public void testWriteShouldThrowErrorWhenJDBCFailsToExecuteSQL() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doThrow(SQLException.class).when(statement).executeUpdate(anyString());

    assertThrows(
        JDBCResourceManagerException.class, () -> testManager.write(TABLE_NAME, 0, "test"));
  }

  @Test
  public void testWriteShouldReturnTrueIfJDBCDoesNotThrowAnyError() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    assertTrue(testManager.write(TABLE_NAME, 0, "test"));

    verify(statement).executeUpdate(anyString());
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDriverFailsToEstablishConnection()
      throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    doThrow(SQLException.class).when(driver).getConnection(any(), any(), any());

    assertThrows(JDBCResourceManagerException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenJDBCFailsToExecuteSQL() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doThrow(SQLException.class).when(statement).executeQuery(anyString());

    assertThrows(JDBCResourceManagerException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldNotThrowErrorIfJDBCDoesNotThrowAnyError() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(result);

    testManager.readTable(TABLE_NAME);

    verify(statement).executeQuery(anyString());
  }

  @Test
  public void testRunSQLStatementShouldThrowErrorWhenDriverFailsToEstablishConnection()
      throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    doThrow(SQLException.class).when(driver).getConnection(any(), any(), any());

    assertThrows(
        JDBCResourceManagerException.class, () -> testManager.runSQLQuery("SQL statement"));
  }

  @Test
  public void testRunSQLStatementShouldThrowErrorWhenJDBCFailsToExecuteSQL() throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doThrow(SQLException.class).when(statement).executeQuery(anyString());

    assertThrows(
        JDBCResourceManagerException.class, () -> testManager.runSQLQuery("SQL statement"));
  }

  @Test
  public void testRunSQLStatementShouldNotThrowErrorIfJDBCDoesNotThrowAnyError()
      throws SQLException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(JDBC_PORT)).thenReturn(MAPPED_PORT);
    when(driver.getConnection(any(), any(), any())).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(result);

    testManager.runSQLQuery("SQL statement");

    verify(statement).executeQuery(anyString());
  }

  @Test
  public void testCleanupAllShouldReturnTrueIfJDBCDoesNotThrowAnyError() {
    testManager.cleanupAll();
    verify(container).close();
  }
}
