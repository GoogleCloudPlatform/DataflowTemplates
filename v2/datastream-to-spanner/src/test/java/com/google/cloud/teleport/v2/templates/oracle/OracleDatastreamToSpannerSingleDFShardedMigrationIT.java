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
  public static SpannerOracleResourceManager oracleResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String streamNameA;
  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
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
        oracleResourceManager = SharedOracleLiveITInstance.getInstance();
        oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();

        executeOracleSqlFileScript(oracleResourceManager, ORACLE_SCHEMA_FILE_RESOURCE, oracleUser);

        datastreamResourceManager =
            org.apache.beam.it.gcp.datastream.DatastreamResourceManager.builder(
                    testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        org.apache.beam.it.gcp.datastream.OracleSource oracleSource =
            org.apache.beam.it.gcp.datastream.OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    java.util.Map.of(oracleUser.toUpperCase(), java.util.List.of("Users")))
                .build();

        com.google.cloud.datastream.v1.SourceConfig sourceConfig =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", oracleSource);

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
        jobParams.put("workerMachineType", "n1-standard-4");
        jobParams.put(
            "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));

        if (System.getProperty("jdbcDriverJars") != null) {
          String driverPath = System.getProperty("jdbcDriverJars");
          jobParams.put("jdbcDriverJars", driverPath);
        }

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                SESSION_FILE_RESOURCE,
                null,
                "shard1",
                spannerResourceManager,
                null,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                oracleSource);
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
        datastreamResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
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

  private void insertDataInOracle() throws Exception {
    LOG.info("Inserting rows into Users table in Oracle");
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (1, 'Tester1', 20)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (3, 'Tester3', 103)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (13, 'Tester13', 113)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (4, 'Tester4', 104)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (5, 'Tester5', 105)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (6, 'Tester6', 106)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (7, 'Tester7', 107)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (8, 'Tester8', 108)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (9, 'Tester9', 109)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (10, 'Tester10', 110)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (11, 'Tester11', 111)",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (12, 'Tester12', 112)",
        oracleUser);
    SharedOracleLiveITInstance.flushRedoLogs();
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
        + "    },\n"
        + "    {\n"
        + "      \"logicalShardId\": \""
        + shardA
        + "\",\n"
        + "      \"dbName\": \""
        + dbA.toLowerCase()
        + "\",\n"
        + "      \"streamId\": \""
        + streamA
        + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
