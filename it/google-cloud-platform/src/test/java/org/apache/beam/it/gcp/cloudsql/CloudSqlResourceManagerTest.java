/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql;

import static com.google.common.truth.Truth.assertThat;

import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CloudSqlResourceManager}. */
@RunWith(JUnit4.class)
public class CloudSqlResourceManagerTest {

  private static final String TEST_ID = "test_id";
  private static final String HOST = "127.0.0.1";
  private static final String PORT = "1234";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String JDBC_PREFIX = "fake";
  private static final String DATABASE = "mockDatabase";

  /**
   * Helper mock implementation of {@link CloudSqlResourceManager} for testing relevant implemented
   * methods without exposing underlying {@link org.apache.beam.it.jdbc.AbstractJDBCResourceManager}
   * or JDBC drivers.
   */
  private static class MockCloudSqlResourceManager extends CloudSqlResourceManager {

    private final boolean initialized;
    private boolean createdDatabase;
    private String lastRunSqlCommand;

    private MockCloudSqlResourceManager(Builder builder) {
      super(builder);
      this.initialized = true;
    }

    @Override
    public void createDatabase(@NonNull String databaseName) {
      // Avoid creating database during initialization of CloudSqlResourceManager
      if (initialized) {
        super.createDatabase(databaseName);
      }
      createdDatabase = true;
    }

    @Override
    public synchronized void runSQLUpdate(@NonNull String sql) {
      // Keep track of sql statement to ensure caller invoked proper SQL function.
      this.lastRunSqlCommand = sql;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getJDBCPrefix() {
      return JDBC_PREFIX;
    }
  }

  /**
   * Helper method for creating a test resource manager either with or without creating a DB.
   *
   * @param useCustomDb create a DB.
   * @return the initialized resource manager.
   */
  private MockCloudSqlResourceManager createTestManager(boolean useCustomDb) {
    CloudSqlResourceManager.Builder testManagerBuilder =
        new CloudSqlResourceManager.Builder(TEST_ID) {
          @Override
          public @NonNull CloudSqlResourceManager build() {
            return new MockCloudSqlResourceManager(this);
          }

          @Override
          protected void configurePort() {
            this.setPort(1234);
          }
        };

    if (useCustomDb) {
      testManagerBuilder.setDatabaseName(DATABASE);
    }

    return (MockCloudSqlResourceManager)
        testManagerBuilder
            .setUsername(USERNAME)
            .setPassword(PASSWORD)
            .setHost(HOST)
            .setPort(1234)
            .build();
  }

  @Test
  public void testUsingStaticDBDoesNotCreateDB() {
    assertThat(createTestManager(false).createdDatabase).isTrue();
  }

  @Test
  public void testNotUsingStaticDBDoesCreateDB() {
    assertThat(createTestManager(true).createdDatabase).isFalse();
  }

  @Test
  public void testGetUri() {
    assertThat(createTestManager(false).getUri())
        .matches(
            String.format(
                "jdbc:%s://%s:%s/%s_" + "\\d{8}_\\d{6}_[a-zA-Z0-9]{6}",
                JDBC_PREFIX, HOST, PORT, TEST_ID));
  }

  @Test
  public void testGetUriUsingStaticDB() {
    assertThat(createTestManager(true).getUri())
        .isEqualTo(String.format("jdbc:%s://%s:%s/%s", JDBC_PREFIX, HOST, PORT, DATABASE));
  }

  @Test
  public void testDropTable() {
    MockCloudSqlResourceManager testManager = createTestManager(false);

    testManager.createdTables.add("test_table");
    testManager.dropTable("test_table");

    assertThat(testManager.lastRunSqlCommand).contains("DROP TABLE");
    assertThat(testManager.createdTables).isEmpty();
  }

  @Test
  public void testCreateDatabase() {
    MockCloudSqlResourceManager testManager = createTestManager(false);

    testManager.createDatabase("test_db");

    assertThat(testManager.lastRunSqlCommand).contains("CREATE DATABASE");
  }

  @Test
  public void testDropDatabase() {
    MockCloudSqlResourceManager testManager = createTestManager(false);

    testManager.dropDatabase("test_db");

    assertThat(testManager.lastRunSqlCommand).contains("DROP DATABASE test_db");
  }

  @Test
  public void testCleanupAllDropsDBWhenCreated() {
    MockCloudSqlResourceManager testManager = createTestManager(false);

    testManager.cleanupAll();
    assertThat(testManager.lastRunSqlCommand)
        .containsMatch(String.format("DROP DATABASE %s_\\d{8}_\\d{6}_[a-zA-Z0-9]{6}", TEST_ID));
  }

  @Test
  public void testCleanupAllRemovesAllTablesWhenDBNotCreated() {
    MockCloudSqlResourceManager testManager = createTestManager(true);

    testManager.createdTables.add("test_table_1");
    testManager.createdTables.add("test_table_2");
    testManager.cleanupAll();

    assertThat(testManager.lastRunSqlCommand).contains("DROP TABLE");
    assertThat(testManager.createdTables).isEmpty();
  }

  /*
   * Currently only supports static Cloud SQL instance which means jdbc port uses system property.
   */
  @Test
  public void testGetJDBCPort() {
    assertThat(String.valueOf(createTestManager(true).getJDBCPort())).isEqualTo(PORT);
  }

  /*
   * Currently only supports static Cloud SQL instance which means port uses system property.
   */
  @Test
  public void testGetPort() {
    assertThat(String.valueOf(createTestManager(true).getPort())).isEqualTo(PORT);
  }
}
