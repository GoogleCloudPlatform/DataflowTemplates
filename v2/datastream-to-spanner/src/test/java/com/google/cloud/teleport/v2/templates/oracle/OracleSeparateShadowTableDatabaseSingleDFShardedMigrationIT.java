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
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT
    extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT.class);

  private static final String TABLE = "Users";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT/oracle-session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT>
      testInstances = new HashSet<>();

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static SpannerOracleResourceManager oracleResourceManager;
  private static String streamNameA;
  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(
                    System.getProperty("privateConnectivity", "datastream-connect-2"))
                .build();

        spannerResourceManager = setUpSpannerResourceManager();
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();

        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        oracleResourceManager = SharedOracleLiveITInstance.getInstance();
        oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();
        executeOracleSqlFileScript(
            oracleResourceManager,
            "oracle/OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT/oracle-schema.sql",
            oracleUser);

        OracleSource jdbcSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(Map.of(oracleUser.toUpperCase(), List.of("Users")))
                .build();

        com.google.cloud.datastream.v1.SourceConfig sourceConfig =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);
        com.google.cloud.datastream.v1.DestinationConfig destinationConfig =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile",
                gcsResourceManager.getBucket(),
                "oracle-shard-cdc/cdc/",
                DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);
        com.google.cloud.datastream.v1.Stream stream =
            datastreamResourceManager.createStream(
                "test-stream-" + RandomStringUtils.randomAlphanumeric(5).toLowerCase(),
                sourceConfig,
                destinationConfig);
        datastreamResourceManager.startStream(stream);
        streamNameA = stream.getName().substring(stream.getName().lastIndexOf('/') + 1);

        gcsResourceManager.createArtifact(
            "input/shardingConfig.conf", generateSourceConfig(streamNameA, "system", "L1"));

        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("inputFileFormat", "avro");
        jobParams.put(
            "inputFilePattern",
            "gs://" + gcsResourceManager.getBucket() + "/oracle-shard-cdc/cdc/");
        jobParams.put("datastreamSourceType", "oracle");
        jobParams.put(
            "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));
        jobParams.put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        jobParams.put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        if (System.getProperty("jdbcDriverJars") != null) {
          String driverPath = System.getProperty("jdbcDriverJars");
          jobParams.put("jdbcDriverJars", driverPath);
        }

        String sessionFileContent =
            com.google.common.io.Resources.toString(
                com.google.common.io.Resources.getResource(SESSION_FILE_RESOURCE),
                java.nio.charset.StandardCharsets.UTF_8);
        sessionFileContent =
            sessionFileContent.replace("it_test", "system").replace("shard_1", "L1");

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                null,
                null,
                "OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT_shard1",
                spannerResourceManager,
                pubsubResourceManager,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                jdbcSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleSeparateShadowTableDatabaseSingleDFShardedMigrationIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        shadowSpannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        datastreamResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void multiShardMigration() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck rowsConditionCheck =
        org.apache.beam.it.conditions.ChainedConditionCheck.builder(
                List.of(
                    new ConditionCheck() {
                      boolean executed = false;

                      @Override
                      protected String getDescription() {
                        return "Insert Data into Oracle";
                      }

                      @Override
                      protected CheckResult check() {
                        if (!executed) {
                          try {
                            insertDataInOracle();
                            executed = true;
                          } catch (Exception e) {
                            return new CheckResult(false, e.getMessage());
                          }
                        }
                        return new CheckResult(true, "Inserted successfully");
                      }
                    },
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(12)
                        .setMaxRows(12)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), rowsConditionCheck);

    assertThatResult(result).meetsConditions();

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
    executeOracleSql(oracleResourceManager, "COMMIT", oracleUser);
    SharedOracleLiveITInstance.flushRedoLogs();
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("id", 1, "name", "Tester1", "age", 20, "migration_shard_id", "L1"));
    events.add(Map.of("id", 3, "name", "Tester3", "age", 103, "migration_shard_id", "L1"));
    events.add(Map.of("id", 13, "name", "Tester13", "age", 113, "migration_shard_id", "L1"));
    events.add(Map.of("id", 4, "name", "Tester4", "age", 104, "migration_shard_id", "L1"));
    events.add(Map.of("id", 5, "name", "Tester5", "age", 105, "migration_shard_id", "L1"));
    events.add(Map.of("id", 6, "name", "Tester6", "age", 106, "migration_shard_id", "L1"));
    events.add(Map.of("id", 7, "name", "Tester7", "age", 107, "migration_shard_id", "L1"));
    events.add(Map.of("id", 8, "name", "Tester8", "age", 108, "migration_shard_id", "L1"));
    events.add(Map.of("id", 9, "name", "Tester9", "age", 109, "migration_shard_id", "L1"));
    events.add(Map.of("id", 10, "name", "Tester10", "age", 110, "migration_shard_id", "L1"));
    events.add(Map.of("id", 11, "name", "Tester11", "age", 111, "migration_shard_id", "L1"));
    events.add(Map.of("id", 12, "name", "Tester12", "age", 112, "migration_shard_id", "L1"));

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
