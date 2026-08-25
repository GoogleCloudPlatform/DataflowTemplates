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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.ORACLE_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link SpannerToSourceDb} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleCustomShardIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToOracleCustomShardIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleCustomShardIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/SpannerToOracleCustomShardIT/session.json";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleCustomShardIT/oracle-schema.sql";

  private static final String TABLE = "Singers";
  private static final HashSet<SpannerToOracleCustomShardIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static OracleResourceManager jdbcResourceManagerShardA;
  private static OracleResourceManager jdbcResourceManagerShardB;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleCustomShardIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToOracleCustomShardIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        jdbcResourceManagerShardA = SharedOracleReverseITContainer.getInstance();
        testUsernameShardA = setupOracleIsolatedUser(jdbcResourceManagerShardA);

        createOracleSchema(
            jdbcResourceManagerShardA,
            SpannerToOracleCustomShardIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsernameShardA);

        jdbcResourceManagerShardB = SharedOracleReverseITContainer.getInstance();
        testUsernameShardB = setupOracleIsolatedUser(jdbcResourceManagerShardB);

        createOracleSchema(
            jdbcResourceManagerShardB,
            SpannerToOracleCustomShardIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsernameShardB);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadJarToGcs(gcsResourceManager);

        createAndUploadShardConfigToGcs(
            gcsResourceManager,
            Map.of(
                "testShardA", jdbcResourceManagerShardA, "testShardB", jdbcResourceManagerShardB));
        gcsResourceManager.uploadArtifact(
            "input/session.json",
            Resources.getResource(SpannerToOracleCustomShardIT.SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                getClass().getSimpleName(),
                "input/customShard.jar",
                "com.custom.CustomShardIdFetcherForIT",
                null,
                null,
                ORACLE_SOURCE_TYPE,
                jobParameters);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToOracleCustomShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToSourceDbCustomShard() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    // Perform writes to Spanner
    writeSpannerDataForSingers(1, "one", "");
    writeSpannerDataForSingers(2, "two", "");
    writeSpannerDataForSingers(3, "three", "");
    writeSpannerDataForSingers(4, "four", "");
    // Assert events on Oracle
    assertRowsInOracle();
  }

  private void assertRowsInOracle() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"Singers\"")
                        == 2);
    assertThatResult(result).meetsConditions();
    PipelineOperator.Result shardBResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"Singers\"")
                        == 2);
    assertThatResult(shardBResult).meetsConditions();

    List<Map<String, Object>> rows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA,
            testUsernameShardA,
            "SELECT \"SingerId\",\"FirstName\" FROM \"Singers\" ORDER BY \"SingerId\"");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("SingerId").toString()).isEqualTo("1");
    assertThat(rows.get(1).get("SingerId").toString()).isEqualTo("3");

    List<Map<String, Object>> shardBRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardB,
            testUsernameShardB,
            "SELECT \"SingerId\",\"FirstName\" FROM \"Singers\" ORDER BY \"SingerId\"");
    assertThat(shardBRows).hasSize(2);
    assertThat(shardBRows.get(0).get("SingerId").toString()).isEqualTo("2");
    assertThat(shardBRows.get(1).get("SingerId").toString()).isEqualTo("4");
  }

  private void writeSpannerDataForSingers(int singerId, String firstName, String shardId) {
    // Write a single record to Spanner
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(singerId)
            .set("FirstName")
            .to(firstName)
            .build();
    spannerResourceManager.write(m);
  }
}
