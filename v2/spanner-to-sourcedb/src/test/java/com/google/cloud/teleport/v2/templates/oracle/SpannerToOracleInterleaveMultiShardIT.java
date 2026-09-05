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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.ORACLE_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
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

/** Integration test for {@link SpannerToSourceDb} Flex template for multiple shards. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleInterleaveMultiShardIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleInterleaveMultiShardIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleInterleaveMultiShardIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE =
      "oracle/SpannerToOracleInterleaveMultiShardIT/session.json";
  private static final String ORACLE_DDL_RESOURCE =
      "oracle/SpannerToOracleInterleaveMultiShardIT/oracle-schema.sql";

  private static HashSet<SpannerToOracleInterleaveMultiShardIT> testInstances = new HashSet<>();
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
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleInterleaveMultiShardIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToOracleInterleaveMultiShardIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = SharedOracleReverseITContainer.getInstance();
        testUsernameShardA = setupOracleIsolatedUser(jdbcResourceManagerShardA);
        createOracleSchema(
            jdbcResourceManagerShardA,
            SpannerToOracleInterleaveMultiShardIT.ORACLE_DDL_RESOURCE,
            testUsernameShardA);

        jdbcResourceManagerShardB = SharedOracleReverseITContainer.getInstance();
        testUsernameShardB = setupOracleIsolatedUser(jdbcResourceManagerShardB);
        createOracleSchema(
            jdbcResourceManagerShardB,
            SpannerToOracleInterleaveMultiShardIT.ORACLE_DDL_RESOURCE,
            testUsernameShardB);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(
            gcsResourceManager,
            Map.of("shardA", jdbcResourceManagerShardA, "shardB", jdbcResourceManagerShardB));
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());
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
                put("dlqRetryMinutes", "1");
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
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
    for (SpannerToOracleInterleaveMultiShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToSourceFKTest() throws IOException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    doInsertsInSpanner();
    assertInsertedRowsInOracle();

    doUpdatesInSpanner();
    assertUpdatedRowsInOracle();

    doDeletesInSpanner();
    assertDeletedRowsInOracle();
  }

  private void doInsertsInSpanner() {
    // Insert records
    List<Mutation> mutations = new ArrayList<>();
    Mutation p1 =
        Mutation.newInsertOrUpdateBuilder("parent1")
            .set("id")
            .to(1)
            .set("migration_shard_id")
            .to("shardA")
            .build();
    spannerResourceManager.write(p1);

    Mutation p2 =
        Mutation.newInsertOrUpdateBuilder("parent2")
            .set("id")
            .to(2)
            .set("migration_shard_id")
            .to("shardB")
            .build();
    spannerResourceManager.write(p2);

    Mutation c1 =
        Mutation.newInsertOrUpdateBuilder("child11")
            .set("child_id")
            .to(11)
            .set("parent_id")
            .to(1)
            .set("migration_shard_id")
            .to("shardA")
            .build();
    Mutation c2 =
        Mutation.newInsertOrUpdateBuilder("child21")
            .set("child_id")
            .to(22)
            .set("id")
            .to(2)
            .set("migration_shard_id")
            .to("shardB")
            .build();
    Mutation c3 =
        Mutation.newInsertOrUpdateBuilder("child31")
            .set("child_id")
            .to(33)
            .set("id")
            .to(2)
            .set("migration_shard_id")
            .to("shardB")
            .build();
    mutations.add(c1);
    mutations.add(c2);
    mutations.add(c3);
    spannerResourceManager.write(mutations);
  }

  private void assertInsertedRowsInOracle() throws InterruptedException {
    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"parent1\"")
                        == 1);
    assertThatResult(parent1Result).meetsConditions();

    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"child11\"")
                        == 1);
    assertThatResult(child1Result).meetsConditions();

    PipelineOperator.Result parent2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"parent2\"")
                        == 1);
    assertThatResult(parent2Result).meetsConditions();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"child21\"")
                        == 1);
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result2 =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"child31\"")
                        == 1);
    assertThatResult(result2).meetsConditions();

    List<Map<String, Object>> rows =
        runIsolatedReadTable(jdbcResourceManagerShardA, testUsernameShardA, "\"parent1\"");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(new java.math.BigDecimal("1"));

    List<Map<String, Object>> rows1 =
        runIsolatedReadTable(jdbcResourceManagerShardB, testUsernameShardB, "\"parent2\"");
    assertThat(rows1).hasSize(1);
    assertThat(rows1.get(0).get("id")).isEqualTo(new java.math.BigDecimal("2"));

    List<Map<String, Object>> rows2 =
        runIsolatedReadTable(jdbcResourceManagerShardA, testUsernameShardA, "\"child11\"");
    assertThat(rows2).hasSize(1);
    assertThat(rows2.get(0).get("child_id")).isEqualTo(new java.math.BigDecimal("11"));

    List<Map<String, Object>> rows3 =
        runIsolatedReadTable(jdbcResourceManagerShardB, testUsernameShardB, "\"child21\"");
    assertThat(rows3).hasSize(1);
    assertThat(rows3.get(0).get("child_id")).isEqualTo(new java.math.BigDecimal("22"));

    List<Map<String, Object>> rows4 =
        runIsolatedReadTable(jdbcResourceManagerShardB, testUsernameShardB, "\"child31\"");
    assertThat(rows4).hasSize(1);
    assertThat(rows4.get(0).get("child_id")).isEqualTo(new java.math.BigDecimal("33"));
  }

  private void doUpdatesInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation p1 =
        Mutation.newUpdateBuilder("parent1")
            .set("id")
            .to(1)
            .set("migration_shard_id")
            .to("shardA")
            .set("update_ts")
            .to(Timestamp.parseTimestamp("1980-01-01T00:00:00Z"))
            .build();
    Mutation c1 =
        Mutation.newUpdateBuilder("child11")
            .set("child_id")
            .to(11)
            .set("parent_id")
            .to(1)
            .set("migration_shard_id")
            .to("shardA")
            .set("update_ts")
            .to(Timestamp.parseTimestamp("1980-01-01T00:00:00Z"))
            .build();
    // This extra insert will help us in validation
    Mutation c2 =
        Mutation.newInsertOrUpdateBuilder("child11")
            .set("child_id")
            .to(12)
            .set("parent_id")
            .to(1)
            .set("migration_shard_id")
            .to("shardA")
            .build();
    mutations.add(p1);
    mutations.add(c1);
    mutations.add(c2);
    spannerResourceManager.write(mutations);
  }

  private void assertUpdatedRowsInOracle() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"child11\"")
                        == 2);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows =
        runIsolatedReadTable(jdbcResourceManagerShardA, testUsernameShardA, "\"parent1\"");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("update_ts").toString()).isEqualTo("1980-01-01 00:00:00.0");

    List<Map<String, Object>> rows2 =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA,
            testUsernameShardA,
            "SELECT \"child_id\",\"update_ts\" FROM \"child11\" ORDER BY \"child_id\"");
    assertThat(rows2).hasSize(2);
    assertThat(rows2.get(0).get("child_id")).isEqualTo(new java.math.BigDecimal("11"));
    assertThat(rows.get(0).get("update_ts").toString()).isEqualTo("1980-01-01 00:00:00.0");
  }

  private void doDeletesInSpanner() {
    // Delete records
    List<Mutation> mutations = new ArrayList<>();
    Mutation c1 = Mutation.delete("child11", Key.of(11));
    Mutation c2 = Mutation.delete("child11", Key.of(12));
    Mutation p1 = Mutation.delete("parent1", Key.of(1));
    Mutation p2 = Mutation.delete("parent2", Key.of(2));
    mutations.add(c1);
    mutations.add(c2);
    mutations.add(p1);
    mutations.add(p2); // this should cause child22 delete as well
    spannerResourceManager.write(mutations);
  }

  private void assertDeletedRowsInOracle() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"parent2\"")
                        == 0);
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"parent1\"")
                        == 0);
    assertThatResult(parent1Result).meetsConditions();
    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofSeconds(1)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardA, testUsernameShardA, "\"child11\"")
                        == 0);
    assertThatResult(child1Result).meetsConditions();
    PipelineOperator.Result child2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofSeconds(1)),
                () ->
                    runIsolatedGetRowCount(
                            jdbcResourceManagerShardB, testUsernameShardB, "\"child22\"")
                        == 0);
    assertThatResult(child2Result).meetsConditions();
  }
}
