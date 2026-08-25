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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for sharded data migration using a single Dataflow job for Oracle Source
 * targeting Spanner.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDatastreamToSpannerSingleDFShardedMigrationIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerSingleDFShardedMigrationIT.class);

  private static final String TABLE = "Users";

  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-session.json";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-google_standard_sql-spanner-schema.sql";

  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-schema.sql";

  private static HashSet<OracleDatastreamToSpannerSingleDFShardedMigrationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static CloudOracleResourceManager jdbcResourceManagerShardA;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String streamNameA;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerSingleDFShardedMigrationIT.class) {
      testInstances.add(this);
      if (spannerResourceManager == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
      }
      if (pubsubResourceManager == null) {
        pubsubResourceManager = setUpPubSubResourceManager();
      }
      if (gcsResourceManager == null) {
        gcsResourceManager = setUpSpannerITGcsResourceManager();
      }

      if (jobInfo == null) {

        String oracleUser = System.getProperty("cloudProxyUsername", "system");
        String oraclePassword = System.getProperty("cloudProxyPassword", "TestPassword123");

        jdbcResourceManagerShardA =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(
                    (CloudOracleResourceManager.Builder)
                        CloudOracleResourceManager.builder(testName)
                            .setUsername(oracleUser)
                            .setPassword(oraclePassword)
                            .setDatabaseName("/XEPDB1")
                            .setHost(System.getProperty("hostIp"))
                            .setPort(1521));
        try {
          jdbcResourceManagerShardA.runSQLUpdate("DROP TABLE \"Users\"");
        } catch (Exception e) {
        }

        executeSqlScript(jdbcResourceManagerShardA, ORACLE_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardA.runSQLUpdate(
            "ALTER TABLE \"Users\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        datastreamResourceManager =
            org.apache.beam.it.gcp.datastream.DatastreamResourceManager.builder(
                    testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        org.apache.beam.it.gcp.datastream.OracleSource jdbcSource =
            org.apache.beam.it.gcp.datastream.OracleSource.builder(
                    System.getProperty("hostIp"), oracleUser, oraclePassword, 1521, "XEPDB1")
                .setAllowedTables(java.util.Map.of(oracleUser, java.util.List.of("Users")))
                .build();

        com.google.cloud.datastream.v1.SourceConfig sourceConfig =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

        com.google.cloud.datastream.v1.DestinationConfig destinationConfig =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile",
                gcsResourceManager.getBucket(),
                "oracle-shard-cdc/cdc/",
                org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat
                    .AVRO_FILE_FORMAT);

        com.google.cloud.datastream.v1.Stream stream =
            datastreamResourceManager.createStream(
                "test_stream_"
                    + org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric(5)
                        .toLowerCase(),
                sourceConfig,
                destinationConfig);

        datastreamResourceManager.startStream(stream);
        streamNameA = stream.getName().substring(stream.getName().lastIndexOf('/') + 1);

        String shardConfig = generateSourceConfig(streamNameA, oracleUser, "L1");

        gcsResourceManager.createArtifact(
            "input/shardingConfig.conf",
            shardConfig.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("inputFileFormat", "avro");
        jobParams.put(
            "inputFilePattern",
            "gs://" + gcsResourceManager.getBucket() + "/oracle-shard-cdc/cdc/");
        jobParams.put("datastreamSourceType", "oracle");
        jobParams.put(
            "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));

        if (System.getProperty("jdbcDriverJars") != null) {
          String driverPath = System.getProperty("jdbcDriverJars");
          jobParams.put("jdbcDriverJars", driverPath);
        }

        String sessionFileContent =
            com.google.common.io.Resources.toString(
                com.google.common.io.Resources.getResource(SESSION_FILE_RESOURCE),
                java.nio.charset.StandardCharsets.UTF_8);
        sessionFileContent =
            sessionFileContent.replace("it_test", oracleUser).replace("shard_1", "L1");
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                null,
                null,
                "shard1",
                spannerResourceManager,
                null,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                null);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleDatastreamToSpannerSingleDFShardedMigrationIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        jdbcResourceManagerShardA,
        datastreamResourceManager);
  }

  @Test
  public void multiShardMigration() throws Exception {

    // Check if pipeline is running
    assertThatPipeline(jobInfo).isRunning();

    insertDataInOracle();

    // Setup condition
    ConditionCheck rowsConditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, TABLE)
            .setMinRows(12)
            .setMaxRows(12)
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), rowsConditionCheck);

    assertThatResult(result).meetsConditions();

    // Sleep for cutover time to wait till all CDCs propagate.
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }

    // Assert specific rows
    assertUsersTableContents();
  }

  private void insertDataInOracle() {
    LOG.info("Inserting rows into Users table in Oracle");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (1, 'Tester1', 20)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (3, 'Tester3', 103)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (13, 'Tester13', 113)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (4, 'Tester4', 104)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (5, 'Tester5', 105)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (6, 'Tester6', 106)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (7, 'Tester7', 107)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (8, 'Tester8', 108)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (9, 'Tester9', 109)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (10, 'Tester10', 110)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (11, 'Tester11', 111)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (12, 'Tester12', 112)");

    LOG.info("Wait, executing Hard-boot raw JDBC switch logfile...");
    // Force log file archive - needed so Datastream can see changes which are read from archived
    // log files.
    // Explicit constraint: Hard-boot raw JDBC strictly mapping to FREE at " +
    // System.getProperty("hostIp") + ":1521 (User: system, Pass: TestPassword123) calling ALTER
    // SYSTEM SWITCH LOGFILE.
    try (java.sql.Connection conn =
            java.sql.DriverManager.getConnection(
                "jdbc:oracle:thin:@" + System.getProperty("hostIp") + ":1521/XEPDB1",
                "system",
                "TestPassword123");
        java.sql.Statement stmt = conn.createStatement()) {
      flushOracleRedoLogs(null);
    } catch (Exception e) {
      LOG.warn("Error while executing ALTER SYSTEM SWITCH LOGFILE. Using framework fallback...", e);
      flushOracleRedoLogs(jdbcResourceManagerShardA);
    }
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "Tester1");
    row.put("age", 20);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("name", "Tester3");
    row.put("age", 103);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 13);
    row.put("name", "Tester13");
    row.put("age", 113);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 4);
    row.put("name", "Tester4");
    row.put("age", 104);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 5);
    row.put("name", "Tester5");
    row.put("age", 105);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 6);
    row.put("name", "Tester6");
    row.put("age", 106);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 7);
    row.put("name", "Tester7");
    row.put("age", 107);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 8);
    row.put("name", "Tester8");
    row.put("age", 108);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 9);
    row.put("name", "Tester9");
    row.put("age", 109);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 10);
    row.put("name", "Tester10");
    row.put("age", 110);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 11);
    row.put("name", "Tester11");
    row.put("age", 111);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 12);
    row.put("name", "Tester12");
    row.put("age", 112);
    row.put("migration_shard_id", "L1");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private String generateSourceConfig(String streamA, String dbA, String shardA) {
    return "{\n"
        + "  \"shardConfigs\": [\n"
        + "    {\n"
        + "      \"logicalShardId\": \""
        + shardA
        + "\",\n"
        + "      \"dbName\": \""
        + dbA
        + "\",\n"
        + "      \"streamId\": \""
        + streamA
        + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
