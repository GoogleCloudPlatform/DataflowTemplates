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
package com.google.cloud.teleport.v2.templates.loadtesting;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Load test for {@link DataStreamToSpanner} template with 5000 tables. */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpanner5kTablesLT extends DataStreamToSpannerLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpanner5kTablesLT.class);
  private static final int NUM_TABLES = 5000;
  private CloudMySQLResourceManager jdbcResourceManager;
  private Instant startTime;

  @Before
  public void setUp() throws IOException {
    super.setUp();
    LOG.info("Began Setup for 5K Table test");
    startTime = Instant.now();

    setUpResourceManagers(null, true);
    jdbcResourceManager = CloudMySQLResourceManager.builder(testName).build();
  }

  @Override
  protected boolean shouldUsePrivateConnectivity() {
    return true;
  }

  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(jdbcResourceManager);
    super.cleanUp();
    LOG.info(
        "CleanupCompleted for 5K Table test. Test took {}",
        Duration.between(startTime, Instant.now()));
  }

  @Test
  public void test5kTablesCDC() throws Exception {
    // 1. Create 5k tables in MySQL
    LOG.info("Creating master table in MySQL...");
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE table_1 (id BIGINT NOT NULL PRIMARY KEY)");
    }

    LOG.info("Creating remaining {} tables in MySQL...", NUM_TABLES - 1);
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      for (int j = 2; j <= NUM_TABLES; j++) {
        String mySqlDdl = String.format("CREATE TABLE table_%d LIKE table_1", j);
        stmt.addBatch(mySqlDdl);
      }
      stmt.executeBatch();
    }

    // 2. Create 5k tables in Spanner
    LOG.info("Creating 5k tables in Spanner...");
    List<String> spannerDdls = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      spannerDdls.add(
          String.format("CREATE TABLE table_%d (id INT64 NOT NULL) PRIMARY KEY(id)", i));
    }
    spannerResourceManager.executeDdlStatements(spannerDdls);

    // 3. Setup Datastream source
    List<String> tableNames = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      tableNames.add("table_" + i);
    }
    java.util.Map<String, List<String>> allowedTables =
        java.util.Map.of(jdbcResourceManager.getDatabaseName(), tableNames);

    JDBCSource mySQLSource =
        new MySQLSource.Builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .setAllowedTables(allowedTables)
            .build();

    HashMap<String, Integer> tables = new HashMap<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      tables.put("table_" + i, 1);
    }

    // 4. Run Load Test with CDC callback
    runLoadTest(
        tables,
        mySQLSource,
        new HashMap<>(),
        new HashMap<>(),
        () -> {
          LOG.info("Inserting 1 row into all {} tables in MySQL (CDC mode)...", NUM_TABLES);
          try (Connection conn = getJdbcConnection(jdbcResourceManager);
              Statement stmt = conn.createStatement()) {
            for (int i = 1; i <= NUM_TABLES; i++) {
              String sql = String.format("INSERT INTO table_%d (id) VALUES (%d)", i, i);
              stmt.addBatch(sql);
            }
            stmt.executeBatch();
          } catch (SQLException e) {
            throw new RuntimeException("Failed to insert CDC data", e);
          }
        });
  }

  private static Connection getJdbcConnection(CloudMySQLResourceManager mySQLResourceManager)
      throws SQLException {
    return DriverManager.getConnection(
        mySQLResourceManager.getUri(),
        mySQLResourceManager.getUsername(),
        mySQLResourceManager.getPassword());
  }
}
