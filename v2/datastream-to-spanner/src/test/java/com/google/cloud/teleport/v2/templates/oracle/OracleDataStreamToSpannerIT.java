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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
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
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(Parameterized.class)
public class OracleDataStreamToSpannerIT extends SpannerTemplateITBase {

  private static final Integer NUM_EVENTS = 10;

  private static final String ROW_ID = "ROW_ID";
  private static final String NAME = "NAME";
  private static final String AGE = "AGE";
  private static final String MEMBER = "MEMBER";
  private static final String ENTRY_ADDED = "ENTRY_ADDED";

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private CloudOracleResourceManager cloudOracleSysUser;
  private CloudOracleResourceManager cloudSqlResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
            .build();

    gcsResourceManager = setUpSpannerITGcsResourceManager();
    gcsPrefix =
        getGcsPath(testName + "/cdc/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
    dlqGcsPrefix =
        getGcsPath(testName + "/dlq/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        cloudOracleSysUser,
        cloudSqlResourceManager,
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamOracleToSpanner() throws IOException {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToPostgresSpanner() throws IOException {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.POSTGRESQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToSpannerJson() throws IOException {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  private void setUpOracleUser(String user, String password) {
    cloudOracleSysUser.runSQLUpdate(
        String.format("CREATE USER %s IDENTIFIED BY %s CONTAINER=ALL", user, password));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE_CATALOG_ROLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CONNECT TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT CREATE SESSION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$DATABASE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PDBS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_SUPPLEMENTAL_LOGGING TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOG TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGFILE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$THREAD TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PARAMETER TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$NLS_PARAMETERS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TIMEZONE_NAMES TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TRANSACTION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.DBA_REGISTRY TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.OBJ$ TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.ENC$ TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT UNLIMITED TABLESPACE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY DICTIONARY TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SET CONTAINER TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT LOGMINING TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR_D TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY TRANSACTION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_EXTENTS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("GRANT CREATE ANY TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(
        String.format("ALTER USER %s QUOTA 50m ON SYSTEM CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT ALTER SYSTEM TO %s CONTAINER=ALL", user));
  }

  private void simpleOracleToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    cloudOracleSysUser =
        (CloudOracleResourceManager)
            CloudOracleResourceManager.builder(testName)
                .setUsername("sys as sysdba")
                .setPassword(System.getProperty("cloudProxyPassword"))
                .setDatabaseName("XE")
                .setHost(System.getProperty("hostIp"))
                .setPort(1521)
                .build();
    String oracleUser = System.getProperty("cloudProxyUsername");
    String oraclePassword = System.getProperty("cloudProxyPassword");
    // setUpOracleUser(oracleUser, oraclePassword);
    //     setUpOracleUser(oracleUser, oraclePassword);

    cloudSqlResourceManager =
        (CloudOracleResourceManager)
            CloudOracleResourceManager.builder(testName)
                .setUsername(oracleUser)
                .setPassword(oraclePassword)
                .setDatabaseName("/XEPDB1")
                .setHost(System.getProperty("hostIp"))
                .setPort(1521)
                .build();

    SpannerResourceManager.Builder spannerResourceManagerBuilder =
        SpannerResourceManager.builder(testName, PROJECT, REGION, spannerDialect)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .setCredentials(credentials);
    spannerResourceManager = spannerResourceManagerBuilder.build();

    List<String> tableNames =
        List.of(
            ("DatastreamToSpanner_1_" + RandomStringUtils.randomAlphanumeric(5)).toUpperCase(),
            ("DatastreamToSpanner_2_" + RandomStringUtils.randomAlphanumeric(5)).toUpperCase());

    tableNames.forEach(
        tableName -> {
          cloudSqlResourceManager.createTable(tableName, createJdbcSchema());
          cloudSqlResourceManager.runSQLUpdate(
              String.format("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableName));
        });

    OracleSource jdbcSource =
        OracleSource.builder(
                cloudSqlResourceManager.getHost(),
                cloudSqlResourceManager.getUsername(),
                cloudSqlResourceManager.getPassword(),
                cloudSqlResourceManager.getPort(),
                cloudSqlResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(
                    cloudSqlResourceManager.getUsername().toUpperCase(),
                    List.of(tableNames.get(0), tableNames.get(1))))
            .build();

    createSpannerTables(tableNames, spannerDialect);

    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", gcsResourceManager.getBucket(), gcsPrefix, fileFormat);

    Stream stream =
        datastreamResourceManager.createStream(
            "stream" + RandomStringUtils.randomAlphanumeric(5).toLowerCase(),
            sourceConfig,
            destinationConfig);
    datastreamResourceManager.startStream(stream);

    createPubSubNotifications();
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                    .addParameter("gcsPubSubSubscription", subscription.toString())
                    .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
                    .addParameter("streamName", stream.getName())
                    .addParameter("instanceId", spannerResourceManager.getInstanceId())
                    .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                    .addParameter("projectId", PROJECT)
                    .addParameter(
                        "deadLetterQueueDirectory",
                        getGcsPath(testName, gcsResourceManager) + "/dlq/")
                    .addParameter("spannerHost", spannerResourceManager.getSpannerHost())
                    .addParameter(
                        "inputFileFormat",
                        fileFormat.equals(
                                DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT)
                            ? "avro"
                            : "json"))
            .addParameter("workerMachineType", "n2-standard-4");

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableNames, cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, tableNames.get(0))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, tableNames.get(1))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableNames, cdcEvents),
                    checkDestinationRows(tableNames, cdcEvents)))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    checkSpannerTables(tableNames, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR2(200)");
    columns.put(AGE, "INTEGER");
    columns.put(MEMBER, "VARCHAR2(200)");
    columns.put(ENTRY_ADDED, "VARCHAR2(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  private void createPubSubNotifications() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    subscription = pubsubResourceManager.createSubscription(topic, "it-sub");
    dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsResourceManager.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));
  }

  private void createSpannerTables(List<String> tableNames, Dialect spannerDialect) {
    boolean usingPg = Dialect.POSTGRESQL.equals(spannerDialect);
    tableNames.forEach(
        tableName -> {
          String q = usingPg ? "\"" : "`";
          spannerResourceManager.executeDdlStatement(
              "CREATE TABLE "
                  + (usingPg ? "\"" + tableName + "\"" : "`" + tableName + "`")
                  + " ("
                  + q
                  + ROW_ID
                  + q
                  + (usingPg ? " bigint " : " INT64 ")
                  + "NOT NULL, "
                  + q
                  + NAME
                  + q
                  + (usingPg ? " character varying(200), " : " STRING(MAX), ")
                  + q
                  + AGE
                  + q
                  + (usingPg ? " bigint, " : " INT64, ")
                  + q
                  + MEMBER
                  + q
                  + (usingPg ? " character varying(200), " : " STRING(MAX), ")
                  + q
                  + ENTRY_ADDED
                  + q
                  + (usingPg ? " character varying(200)" : " STRING(MAX)")
                  + (usingPg ? ", " : ") ")
                  + "PRIMARY KEY ("
                  + q
                  + ROW_ID
                  + q
                  + ")"
                  + (usingPg ? ")" : ""));
        });
  }

  private ConditionCheck checkDestinationRows(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Check Spanner rows.";
      }

      @Override
      protected CheckResult check() {
        for (String tableName : tableNames) {
          long totalRows = spannerResourceManager.getRowCount(tableName);
          long maxRows = cdcEvents.get(tableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
          }
        }
        try {
          checkSpannerTables(tableNames, cdcEvents);
          return new CheckResult(true, "Spanner tables contain expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "Spanner tables do not contain expected rows.");
        }
      }
    };
  }

  private void checkSpannerTables(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    tableNames.forEach(
        tableName ->
            SpannerAsserts.assertThatStructs(
                    spannerResourceManager.readTableRecords(tableName, COLUMNS))
                .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName)));
  }

  private ConditionCheck writeJdbcData(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
        for (String tableName : tableNames) {

          List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            Map<String, Object> values = new HashMap<>();
            values.put(ROW_ID, i);
            values.put(NAME, RandomStringUtils.randomAlphabetic(10));
            values.put(AGE, new Random().nextInt(100));
            values.put(MEMBER, new Random().nextInt() % 2 == 0 ? "Y" : "N");
            values.put(ENTRY_ADDED, Instant.now().toString());
            rows.add(values);
          }

          List<Map<String, Object>> cdcRows = new ArrayList<>();
          for (Map<String, Object> row : rows) {
            Map<String, Object> cdcRow = new HashMap<>();
            cdcRow.put(ROW_ID, row.get(ROW_ID));
            cdcRow.put(NAME, row.get(NAME));
            cdcRow.put(AGE, row.get(AGE));
            cdcRow.put(MEMBER, row.get(MEMBER));
            cdcRow.put(ENTRY_ADDED, row.get(ENTRY_ADDED));
            cdcRows.add(cdcRow);
          }
          cdcEvents.put(tableName, cdcRows);

          success &= cloudSqlResourceManager.write(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }

        cloudOracleSysUser.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  private ConditionCheck changeJdbcData(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send JDBC changes.";
      }

      @Override
      protected CheckResult check() {
        List<String> messages = new ArrayList<>();
        for (String tableName : tableNames) {

          List<Map<String, Object>> newCdcEvents = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            if (i % 2 == 0) {
              Map<String, Object> values = cdcEvents.get(tableName).get(i);

              String newName = values.get(NAME).toString().toUpperCase();
              int newAge = new Random().nextInt(100);
              String newMember = Objects.equals(values.get(MEMBER).toString(), "Y") ? "N" : "Y";

              values.put(NAME, newName);
              values.put(AGE, newAge);
              values.put(MEMBER, newMember);

              String updateSql =
                  "UPDATE "
                      + tableName
                      + " SET "
                      + NAME
                      + " = '"
                      + newName
                      + "', "
                      + AGE
                      + " = "
                      + newAge
                      + ", "
                      + MEMBER
                      + " = '"
                      + newMember
                      + "' WHERE "
                      + ROW_ID
                      + " = "
                      + i;
              cloudSqlResourceManager.runSQLUpdate(updateSql);
              newCdcEvents.add(values);
            } else {
              cloudSqlResourceManager.runSQLUpdate(
                  "DELETE FROM " + tableName + " WHERE " + ROW_ID + "=" + i);
            }
          }
          cdcEvents.put(tableName, newCdcEvents);
          messages.add(String.format("%d changes to %s", newCdcEvents.size(), tableName));
        }

        cloudSqlResourceManager.runSQLUpdate("COMMIT");
        cloudOracleSysUser.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
        return new CheckResult(true, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
