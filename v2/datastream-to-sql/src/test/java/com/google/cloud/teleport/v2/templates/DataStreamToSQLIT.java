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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.Strings;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(DataStreamToSQL.class)
@RunWith(JUnit4.class)
public class DataStreamToSQLIT extends TemplateTestBase {

  enum JDBCType {
    MYSQL,
    ORACLE,
    POSTGRES
  }

  private static final int NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private CloudOracleResourceManager cloudOracleSysUser;
  private CloudSqlResourceManager cloudSqlSourceResourceManager;
  private CloudSqlResourceManager cloudSqlDestinationResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();

    String password = System.getProperty("cloudOracleSysPassword");
    if (Strings.isNullOrEmpty(password)) {
      throw new IllegalStateException("Missing -DcloudOracleSysPassword");
    }
    cloudOracleSysUser =
        (CloudOracleResourceManager)
            CloudOracleResourceManager.builder(testName)
                .setUsername("sys as sysdba")
                .setPassword(password)
                .build();

    gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
    dlqGcsPrefix = getGcsPath(testName + "/dlq/").replace("gs://" + artifactBucketName, "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        cloudOracleSysUser,
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testDataStreamOracleToMySqlJson() throws IOException {
    // Run a simple IT
    simpleOracleToJdbcTest(JDBCType.MYSQL, Function.identity());
  }

  @Test
  public void testDataStreamOracleToPostgresJson() throws IOException {
    // Run a simple IT
    simpleOracleToJdbcTest(JDBCType.POSTGRES, Function.identity());
  }

  @Test
  public void testDataStreamOracleToMySqlJsonGCSNotifications() throws IOException {
    // Set up pubsub notifications
    SubscriptionName subscriptionName = createGcsNotifications();

    // Run a simple IT
    simpleOracleToJdbcTest(
        JDBCType.MYSQL,
        config -> config.addParameter("gcsPubSubSubscription", subscriptionName.toString()));
  }

  @Test
  public void testDataStreamOracleToPostgresJsonGCSNotifications() throws IOException {
    // Set up pubsub notifications
    SubscriptionName subscriptionName = createGcsNotifications();

    // Run a simple IT
    simpleOracleToJdbcTest(
        JDBCType.POSTGRES,
        config -> config.addParameter("gcsPubSubSubscription", subscriptionName.toString()));
  }

  private void simpleOracleToJdbcTest(
      JDBCType destJdbcType,
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    // Create destination JDBC Resource manager
    cloudSqlDestinationResourceManager =
        destJdbcType.equals(JDBCType.MYSQL)
            ? CloudMySQLResourceManager.builder(testName).build()
            : CloudPostgresResourceManager.builder(testName).build();

    // Since test uses Oracle XE, schemas are mapped to users. The following code
    // creates a new Oracle user and grants all necessary permissions needed by
    // Datastream.
    String oracleUser = cloudSqlDestinationResourceManager.getDatabaseName();
    String oraclePassword = System.getProperty("cloudProxyPassword");
    setUpOracleUser(oracleUser, oraclePassword);
    cloudSqlSourceResourceManager =
        (CloudSqlResourceManager)
            CloudOracleResourceManager.builder(testName)
                .setUsername(oracleUser)
                .setPassword(oraclePassword)
                .build();

    // Create source JDBC table
    String tableName = "oracletosql_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    cloudSqlSourceResourceManager.createTable(tableName, createJdbcSchema(JDBCType.ORACLE));

    // Create destination JDBC table
    cloudSqlDestinationResourceManager.createTable(tableName, createJdbcSchema(destJdbcType));

    // Create JDBC source
    JDBCSource jdbcSource =
        OracleSource.builder(
                cloudSqlSourceResourceManager.getHost(),
                cloudSqlSourceResourceManager.getUsername(),
                cloudSqlSourceResourceManager.getPassword(),
                cloudSqlSourceResourceManager.getPort(),
                cloudSqlSourceResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(
                    cloudSqlSourceResourceManager.getUsername().toUpperCase(),
                    List.of(tableName.toUpperCase())))
            .build();

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("oracle-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                    .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
                    .addParameter("streamName", stream.getName())
                    .addParameter("inputFileFormat", "json"))
            .addParameter(
                "databaseType", destJdbcType.equals(JDBCType.MYSQL) ? "mysql" : "postgres")
            .addParameter("databaseName", cloudSqlDestinationResourceManager.getDatabaseName())
            .addParameter("databaseHost", cloudSqlDestinationResourceManager.getHost())
            .addParameter(
                "databasePort", String.valueOf(cloudSqlDestinationResourceManager.getPort()))
            .addParameter("databaseUser", cloudSqlDestinationResourceManager.getUsername())
            .addParameter("databasePassword", cloudSqlDestinationResourceManager.getPassword());

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to JDBC
    // 2. Wait on BigQuery to merge events from staging to destination
    // 3. Send wave of mutations to JDBC
    // 4. Wait on BigQuery to merge second wave of events
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableName, cdcEvents),
                    JDBCRowsCheck.builder(cloudSqlDestinationResourceManager, tableName)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableName, cdcEvents),
                    checkDestinationRows(tableName, cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Assert
    checkJdbcTable(tableName, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the JDBC database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the JDBC write operation.
   */
  private ConditionCheck writeJdbcData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put(COLUMNS.get(0), i);
          values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
          values.put(COLUMNS.get(2), new Random().nextInt(100));
          values.put(COLUMNS.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
          values.put(COLUMNS.get(4), Instant.now().toString());
          rows.add(values);
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        boolean success = cloudSqlSourceResourceManager.write(tableName, rows);
        cloudSqlSourceResourceManager.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");

        cdcEvents.put(tableName, rows);
        return new CheckResult(
            success, String.format("Sent %d rows to %s.", rows.size(), tableName));
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method changes rows of data in
   * the JDBC database according to the common schema for the IT's in this class. Half the rows are
   * mutated and half are removed completely.
   *
   * @return A ConditionCheck containing the JDBC mutate operation.
   */
  private ConditionCheck changeJdbcData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Send JDBC changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = cdcEvents.get(tableName).get(i);
            values.put(COLUMNS.get(2), new Random().nextInt(100));
            values.put(
                COLUMNS.get(3),
                (Objects.equals(values.get(COLUMNS.get(3)).toString(), "Y") ? "N" : "Y"));

            String updateSql =
                "UPDATE "
                    + tableName
                    + " SET "
                    + COLUMNS.get(2)
                    + " = "
                    + values.get(COLUMNS.get(2))
                    + ","
                    + COLUMNS.get(3)
                    + " = '"
                    + values.get(COLUMNS.get(3))
                    + "'"
                    + " WHERE "
                    + COLUMNS.get(0)
                    + " = "
                    + i;
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            newCdcEvents.add(values);
          } else {
            cloudSqlSourceResourceManager.runSQLUpdate(
                "DELETE FROM " + tableName + " WHERE " + COLUMNS.get(0) + "=" + i);
          }
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        cloudSqlSourceResourceManager.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");

        cdcEvents.put(tableName, newCdcEvents);
        return new CheckResult(
            true, String.format("Sent %d changes to %s.", newCdcEvents.size(), tableName));
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method checks the rows in the
   * destination BigQuery database for specific rows.
   *
   * @return A ConditionCheck containing the check operation.
   */
  private ConditionCheck checkDestinationRows(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Check JDBC rows.";
      }

      @Override
      protected @NonNull CheckResult check() {
        // First, check that correct number of rows were deleted.
        long totalRows = cloudSqlDestinationResourceManager.getRowCount(tableName);
        long maxRows = cdcEvents.get(tableName).size();
        if (totalRows > maxRows) {
          return new CheckResult(
              false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
        }

        // Next, make sure in-place mutations were applied.
        try {
          checkJdbcTable(tableName, cdcEvents);
          return new CheckResult(true, "JDBC table contains expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "JDBC table does not contain expected rows.");
        }
      }
    };
  }

  /** Helper function for checking the rows of the destination JDBC tables. */
  private void checkJdbcTable(String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {

    assertThatRecords(cloudSqlDestinationResourceManager.readTable(tableName))
        .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema(JDBCType jdbcType) {
    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    String numericDataType = jdbcType.equals(JDBCType.ORACLE) ? "NUMBER" : "NUMERIC";
    columns.put(ROW_ID, numericDataType + " NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, numericDataType);
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  private SubscriptionName createGcsNotifications() throws IOException {
    // Instantiate pubsub resource manager for notifications
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    // Create pubsub notifications
    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "it-sub");

    gcsClient.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsClient.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));

    return subscription;
  }

  /**
   * Helper method for granting all the permissions to a user required by Datastream.
   *
   * @param user the user that will be given to Datastream
   * @param password the password for the given user.
   */
  private void setUpOracleUser(String user, String password) {
    cloudOracleSysUser.runSQLUpdate(
        String.format("CREATE USER %s IDENTIFIED BY \"%s\"", user, password));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE_CATALOG_ROLE TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CONNECT TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE SESSION TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$DATABASE TO %s", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO %s", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE ON DBMS_LOGMNR TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE ON DBMS_LOGMNR_D TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TRANSACTION TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TABLE TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON DBA_EXTENTS TO %s", user));

    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE ANY TABLE TO %s", user));
    cloudOracleSysUser.runSQLUpdate(String.format("ALTER USER %s QUOTA 50m ON SYSTEM", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT ALTER SYSTEM TO %s", user));
  }
}
