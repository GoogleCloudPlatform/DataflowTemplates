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
import static org.junit.Assert.assertTrue;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
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
public class DataStreamToSpannerOracleRetryDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerOracleRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryDLQIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryDLQIT/oracle-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryDLQIT/session.json";
  private static final String GCS_PATH_PREFIX = "oracle-datastream-to-spanner-sharded-retrydlq";

  private static final HashSet<DataStreamToSpannerOracleRetryDLQIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerOracleResourceManager oracleResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static PubsubResourceManager pubsubResourceManager;
  public static String oracleUser;

  private static String streamNameA;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerOracleRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        oracleResourceManager = SharedOracleLiveITInstance.getInstance();
        oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();

        executeOracleSqlFileScript(oracleResourceManager, ORACLE_SCHEMA_FILE_RESOURCE, oracleUser);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(
                    System.getProperty("privateConnectivity", "datastream-connect-2"))
                .build();

        pubsubResourceManager = setUpPubSubResourceManager();

        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        String gcsPrefix =
            getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), "");

        OracleSource oracleSourceA =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(
                        oracleUser.toUpperCase(),
                        List.of(
                            "Customers",
                            "Orders",
                            "AllDataTypes"))) // Assuming schemas are username
                .build();

        SourceConfig sourceConfigA =
            datastreamResourceManager.buildJDBCSourceConfig("oracle-profile-shardA", oracleSourceA);
        DestinationConfig destinationConfigA =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile-shardA",
                gcsResourceManager.getBucket(),
                gcsPrefix,
                DestinationOutputFormat.AVRO_FILE_FORMAT);
        Stream streamA =
            datastreamResourceManager.createStream(
                "stream-shardA", sourceConfigA, destinationConfigA);
        datastreamResourceManager.startStream(streamA);
        streamNameA = streamA.getName().substring(streamA.getName().lastIndexOf('/') + 1);

        String shardConfig =
            generateSourceConfig(
                streamNameA,
                oracleUser.toUpperCase(),
                "shard1",
                streamNameA,
                "DUMMY_DB_B",
                "shard2");

        gcsResourceManager.createArtifact(
            "input/shardingConfig.conf", shardConfig.getBytes(StandardCharsets.UTF_8));

        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put("dlqMaxRetryCount", "1000");
        jobParameters.put(
            "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));
        jobParameters.put(
            "inputFilePattern", getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager));
        jobParameters.put(
            "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
        jobParameters.put("datastreamSourceType", "oracle");
        jobParameters.put("dlqRetryMinutes", "1");

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                null);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerOracleRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        gcsResourceManager,
        datastreamResourceManager,
        pubsubResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void testDataStreamToSpannerShardedRetryDLQ() throws Exception {
    LOG.info("Starting testDataStreamToSpannerShardedRetryDLQ");
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Inserting parent rows directly into Spanner");
    spannerResourceManager.write(
        List.of(
            com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
                .set("CustomerId")
                .to(2)
                .set("CustomerName")
                .to("Customer 2")
                .set("CreditLimit")
                .to(1500)
                .set("LegacyRegion")
                .to("Silver")
                .set("migration_shard_id")
                .to("shard1")
                .build()));

    insertDataInOracle();
    LOG.info("Data inserted into Oracle successfully");

    LOG.info("Waiting for DLQ events to appear in severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "Orders")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "Customers")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();

    assertTrue(
        "id=1 should exist in AllDataTypes on shard1",
        rowExistsInSpanner("AllDataTypes", "id", 1, "shard1"));
    assertTrue(
        "id=999 should NOT exist yet in AllDataTypes on shard1",
        !rowExistsInSpanner("AllDataTypes", "id", 999, "shard1"));

    assertTrue(
        "id=2 should exist in Customers on shard1",
        rowExistsInSpanner("Customers", "CustomerId", 2, "shard1"));
    assertTrue(
        "id=1 should NOT exist yet in Customers on shard1",
        !rowExistsInSpanner("Customers", "CustomerId", 1, "shard1"));

    assertTrue(
        "id=102 should exist in Orders on shard1",
        rowExistsInSpanner("Orders", "OrderId", 102, "shard1"));
    assertTrue(
        "id=101 should NOT exist yet in Orders on shard1",
        !rowExistsInSpanner("Orders", "OrderId", 101, "shard1"));
    assertTrue(
        "id=103 should NOT exist yet in Orders on shard1",
        !rowExistsInSpanner("Orders", "OrderId", 103, "shard1"));

    LOG.info("Launching retryDLQ job with session file to process DLQ");
    Map<String, String> retryJobParameters = new HashMap<>();
    retryJobParameters.put("runMode", "retryDLQ");
    retryJobParameters.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryJobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
    retryJobParameters.put("transformationCustomParameters", "mode=good");
    retryJobParameters.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
    retryJobParameters.put(
        "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));
    retryJobParameters.put("datastreamSourceType", "oracle");

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            SESSION_FILE_RESOURCE,
            null,
            GCS_PATH_PREFIX + "-retry",
            spannerResourceManager,
            null,
            retryJobParameters,
            null,
            null,
            gcsResourceManager,
            null,
            null,
            null);
    LOG.info("RetryDLQ job launched: {}", retryJobInfo.jobId());

    assertThatPipeline(retryJobInfo).isRunning();

    LOG.info("Applying partial fixes in Spanner");
    spannerResourceManager.write(
        List.of(
            com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
                .set("CustomerId")
                .to(3)
                .set("CustomerName")
                .to("Parent Customer")
                .set("CreditLimit")
                .to(2000)
                .set("LegacyRegion")
                .to("Gold")
                .set("migration_shard_id")
                .to("shard1")
                .build()));

    LOG.info("Waiting for the retryDLQ job to complete automatically");
    PipelineOperator.Result retryJobResult =
        pipelineOperator().waitUntilDone(createConfig(retryJobInfo, Duration.ofMinutes(15)));
    assertThatResult(retryJobResult).isLaunchFinished();

    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
            .setMinEvents(1)
            .build()
            .get());

    LOG.info("Waiting for fixed rows to appear in Spanner");
    PipelineOperator.Result finalWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, "Orders")
                    .setMinRows(2)
                    .build()
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                            .setMinRows(2)
                            .build()));
    assertThatResult(finalWaitResult).meetsConditions();

    assertTrue("id=999 should exist", rowExistsInSpanner("AllDataTypes", "id", 999, "shard1"));
    assertTrue("id=3 should exist", rowExistsInSpanner("Customers", "CustomerId", 3, "shard1"));
    assertTrue(
        "id=1 should NOT exist", !rowExistsInSpanner("Customers", "CustomerId", 1, "shard1"));
    assertTrue("id=101 should exist", rowExistsInSpanner("Orders", "OrderId", 101, "shard1"));
    assertTrue("id=103 should NOT exist", !rowExistsInSpanner("Orders", "OrderId", 103, "shard1"));

    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());
  }

  private boolean rowExistsInSpanner(
      String tableName, String idColumnName, long id, String shardId) {
    List<com.google.cloud.spanner.Struct> rows =
        spannerResourceManager.readTableRecords(
            tableName, List.of(idColumnName, "migration_shard_id"));
    for (com.google.cloud.spanner.Struct row : rows) {
      if (row.getLong(idColumnName) == id && row.getString("migration_shard_id").equals(shardId)) {
        return true;
      }
    }
    return false;
  }

  private void insertDataInOracle() throws Exception {
    LOG.info("Inserting data in Oracle Shard A");
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\","
            + " \"LoyaltyTier\") VALUES (1, 'Customer 1', 500, 'Bronze')",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (3, 101, 1000, 'Website')",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (2, 102, 1000, 'AppStore')",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (4, 103, 1000, 'AppStore')",
        oracleUser);

    LOG.info("Inserting data in Oracle Shard B");
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar_col\") VALUES (1, 'test1')",
        oracleUser);
    executeOracleSql(
        oracleResourceManager,
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar_col\") VALUES (999, 'test999')",
        oracleUser);
    SharedOracleLiveITInstance.flushRedoLogs();
  }

  private String getCustomShardJarPath() {
    return "/home/dhwanilpatel_google_com/MyStorage/OracleSupport/DataflowTemplates/v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }

  private String generateSourceConfig(
      String streamA, String dbA, String shardA, String streamB, String dbB, String shardB) {
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
        + shardB
        + "\",\n"
        + "      \"dbName\": \""
        + dbB
        + "\",\n"
        + "      \"streamId\": \""
        + streamB
        + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
