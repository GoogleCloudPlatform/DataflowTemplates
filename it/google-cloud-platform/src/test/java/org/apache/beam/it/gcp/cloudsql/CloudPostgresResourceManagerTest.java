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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link CloudPostgresResourceManager}. */
@RunWith(JUnit4.class)
public class CloudPostgresResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;

  private static final String TEST_ID = "test_id";
  private static final String HOST = "127.0.0.1";
  private static final String PORT = "5432";
  private static final String USERNAME = "postgres";
  private static final String PASSWORD = "password";
  private static final String DATABASE = "test_db";

  private MockCloudPostgresResourceManager testManager;

  @Before
  public void setUp() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockConnection.isValid(0)).thenReturn(true);

    testManager = createTestManager();
  }

  private MockCloudPostgresResourceManager createTestManager() {
    CloudPostgresResourceManager.Builder builder =
        new CloudPostgresResourceManager.Builder(TEST_ID);
    builder.setDatabaseName(DATABASE);
    builder.setUsername(USERNAME);
    builder.setPassword(PASSWORD);
    builder.setHost(HOST);
    builder.setPort(Integer.parseInt(PORT));
    return new MockCloudPostgresResourceManager(builder, this);
  }

  @Test
  public void testCreateLogicalReplicationWithSpecificTables() throws SQLException {
    List<String> tables = List.of("table1", "table2");
    CloudPostgresResourceManager.ReplicationInfo info =
        testManager.createLogicalReplication(tables);

    assertThat(info.getReplicationSlotName()).startsWith("slot_");
    assertThat(info.getPublicationName()).startsWith("pub_");

    // Verify timeouts are set
    verify(mockStatement).execute("SET statement_timeout = '60s'");
    verify(mockStatement).execute("SET idle_in_transaction_session_timeout = '60s'");

    // Verify 3 separate transactions (lock + commit for each)
    verify(mockStatement, times(3)).execute("SELECT pg_advisory_xact_lock(12345)");
    verify(mockStatement).execute("ALTER USER CURRENT_USER WITH REPLICATION");
    verify(mockStatement)
        .execute(
            String.format(
                "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                info.getReplicationSlotName()));
    verify(mockStatement)
        .execute(
            String.format(
                "CREATE PUBLICATION %s FOR TABLE table1, table2", info.getPublicationName()));
    verify(mockConnection, times(3)).commit();
  }

  @Test
  public void testCreateLogicalReplicationWithAllTables() throws SQLException {
    CloudPostgresResourceManager.ReplicationInfo info = testManager.createLogicalReplication();

    assertThat(info.getReplicationSlotName()).startsWith("slot_");
    assertThat(info.getPublicationName()).startsWith("pub_");

    // Verify timeouts are set
    verify(mockStatement).execute("SET statement_timeout = '60s'");
    verify(mockStatement).execute("SET idle_in_transaction_session_timeout = '60s'");

    // Verify 3 separate transactions (lock + commit for each)
    verify(mockStatement, times(3)).execute("SELECT pg_advisory_xact_lock(12345)");
    verify(mockStatement).execute("ALTER USER CURRENT_USER WITH REPLICATION");
    verify(mockStatement)
        .execute(
            String.format(
                "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                info.getReplicationSlotName()));
    verify(mockStatement)
        .execute(String.format("CREATE PUBLICATION %s FOR ALL TABLES", info.getPublicationName()));
    verify(mockConnection, times(3)).commit();
  }

  @Test
  public void testCleanupAllDropsResources() throws SQLException {
    // Create resources first
    CloudPostgresResourceManager.ReplicationInfo info = testManager.createLogicalReplication();

    testManager.cleanupAll();

    verify(mockStatement, times(5))
        .execute("SELECT pg_advisory_xact_lock(12345)"); // 3 for creation, 2 for cleanup
    verify(mockStatement, times(3)).execute("SET statement_timeout = '60s'");
    verify(mockStatement, times(3)).execute("SET idle_in_transaction_session_timeout = '60s'");
    verify(mockStatement)
        .execute(String.format("DROP PUBLICATION IF EXISTS %s", info.getPublicationName()));
    verify(mockStatement)
        .execute(
            String.format("SELECT pg_drop_replication_slot('%s')", info.getReplicationSlotName()));
    verify(mockConnection, times(5)).commit(); // 3 for creation, 2 for cleanup
  }

  @Test
  public void testCreateLogicalReplicationRollbackOnError() throws SQLException {
    when(mockStatement.execute(anyString())).thenThrow(new SQLException("Mock error"));

    assertThrows(RuntimeException.class, () -> testManager.createLogicalReplication());

    verify(mockConnection).rollback();
  }

  /** Helper mock implementation of {@link CloudPostgresResourceManager} for testing. */
  private static class MockCloudPostgresResourceManager extends CloudPostgresResourceManager {

    private final CloudPostgresResourceManagerTest testInstance;

    private MockCloudPostgresResourceManager(
        Builder builder, CloudPostgresResourceManagerTest testInstance) {
      super(builder);
      this.testInstance = testInstance;
    }

    @Override
    Connection getConnection() throws SQLException {
      return testInstance.mockConnection;
    }

    @Override
    public void runSQLUpdate(@NonNull String sql) {
      // No-op for schema creation in constructor
    }
  }
}
