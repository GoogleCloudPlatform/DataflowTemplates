/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.io;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link DynamicJdbcIO}. */
public class DynamicJdbcIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcIOTest.class);
  private static final int EXPECTED_ROW_COUNT = 1000;
  private static final String TEST_ROW_SUFFIX = "my-test-row";

  private static NetworkServerControl derbyServer;
  private static ClientDataSource dataSource;

  private static int port;
  private static String readTableName;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startDatabase() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    LOG.info("Starting Derby database on {}", port);

    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "target/derby.log");

    derbyServer = new NetworkServerControl(InetAddress.getByName("localhost"), port);
    StringWriter out = new StringWriter();
    derbyServer.start(new PrintWriter(out));
    boolean started = false;
    int count = 0;
    // Use two different methods to detect when server is started:
    // 1) Check the server stdout for the "started" string
    // 2) wait up to 15 seconds for the derby server to start based on a ping
    // on faster machines and networks, this may return very quick, but on slower
    // networks where the DNS lookups are slow, this may take a little time
    while (!started && count < 30) {
      if (out.toString().contains("started")) {
        started = true;
      } else {
        count++;
        TimeUnit.MILLISECONDS.sleep(500);
        try {
          derbyServer.ping();
          started = true;
        } catch (Throwable t) {
          // ignore, still trying to start
        }
      }
    }

    if (!started) {
      // Server has not started in the expected time frame
      throw new IllegalStateException("Derby server failed to start.");
    }

    dataSource = new ClientDataSource();
    dataSource.setCreateDatabase("create");
    dataSource.setDatabaseName("target/beam");
    dataSource.setServerName("localhost");
    dataSource.setPortNumber(port);

    readTableName = getTestTableName("UT_READ");

    createTable(dataSource, readTableName);
    addInitialData(dataSource, readTableName);
  }

  @AfterClass
  public static void shutDownDatabase() throws Exception {
    try {
      deleteTable(dataSource, readTableName);
    } finally {
      if (derbyServer != null) {
        derbyServer.shutdown();
      }
    }
  }

  @Test
  public void testDataSourceConfigurationDriverAndUrl() throws Exception {
    DynamicJdbcIO.DynamicDataSourceConfiguration config =
        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
            "org.apache.derby.jdbc.ClientDriver",
            maybeDecrypt("jdbc:derby://localhost:" + port + "/target/beam", null).get());
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationUsernameAndPassword() throws Exception {
    String username = "sa";
    String password = "sa";
    DynamicJdbcIO.DynamicDataSourceConfiguration config =
        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                maybeDecrypt("jdbc:derby://localhost:" + port + "/target/beam", null).get())
            .withUsername(maybeDecrypt(username, null).get())
            .withPassword(maybeDecrypt(password, null).get());

    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullPassword() throws Exception {
    String usename = "sa";
    String password = null;
    DynamicJdbcIO.DynamicDataSourceConfiguration config =
        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                maybeDecrypt("jdbc:derby://localhost:" + port + "/target/beam", null).get())
            .withUsername(maybeDecrypt(usename, null).get())
            .withPassword(maybeDecrypt(password, null).get());

    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullUsernameAndPassword() throws Exception {
    String usename = null;
    String password = null;
    DynamicJdbcIO.DynamicDataSourceConfiguration config =
        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                maybeDecrypt("jdbc:derby://localhost:" + port + "/target/beam", null).get())
            .withUsername(maybeDecrypt(usename, null).get())
            .withPassword(maybeDecrypt(password, null).get());

    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  /** A helper method to add some test data. */
  private static void addInitialData(DataSource dataSource, String tableName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(String.format("insert into %s values (?,?)", tableName))) {
        for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
          preparedStatement.clearParameters();
          preparedStatement.setInt(1, i);
          preparedStatement.setString(2, TEST_ROW_SUFFIX + "-" + i);
          preparedStatement.executeUpdate();
        }
      }
      connection.commit();
    }
  }

  private static void createTable(DataSource dataSource, String tableName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }
    }
  }

  private static void deleteTable(DataSource dataSource, String tableName) throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("drop table %s", tableName));
      }
    }
  }

  private static String getTestTableName(String testIdentifier) {
    SimpleDateFormat formatter = new SimpleDateFormat();
    formatter.applyPattern("yyyy_MM_dd_HH_mm_ss_S");
    return String.format("DYNJDBCTEST_%s_%s", testIdentifier, formatter.format(new Date()));
  }

  private static class TestRowMapper implements JdbcIO.RowMapper<KV<Integer, String>> {

    @Override
    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
      return KV.of(resultSet.getInt("id"), resultSet.getString("name"));
    }
  }
}
