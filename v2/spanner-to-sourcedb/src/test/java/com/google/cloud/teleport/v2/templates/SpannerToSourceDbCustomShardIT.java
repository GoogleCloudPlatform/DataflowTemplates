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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
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
public class SpannerToSourceDbCustomShardIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbCustomShardIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbCustomShardIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToSourceDbCustomShardIT/session.json";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbCustomShardIT/mysql-schema.sql";

  private static final String TABLE = "Users";
  private static final HashSet<SpannerToSourceDbCustomShardIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManagerShardA;
  private static MySQLResourceManager jdbcResourceManagerShardB;
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
    synchronized (SpannerToSourceDbCustomShardIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDbCustomShardIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        jdbcResourceManagerShardA = MySQLResourceManager.builder(testName + "shardA").build();

        createMySQLSchema(
            jdbcResourceManagerShardA, SpannerToSourceDbCustomShardIT.MYSQL_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = MySQLResourceManager.builder(testName + "shardB").build();

        createMySQLSchema(
            jdbcResourceManagerShardB, SpannerToSourceDbCustomShardIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadJarToGcs(gcsResourceManager);

        createAndUploadShardConfigToGcs();
        gcsResourceManager.uploadArtifact(
            "input/session.json",
            Resources.getResource(SpannerToSourceDbCustomShardIT.SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
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
                null);
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
    for (SpannerToSourceDbCustomShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
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
    // Assert events on Mysql
    assertRowsInMySQL();
  }

  private void assertRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    jdbcResourceManagerShardA.getRowCount("Singers")
                        == 2); // only one row is inserted
    assertThatResult(result).meetsConditions();
    PipelineOperator.Result shardBResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManagerShardB.getRowCount("Singers") == 2);
    assertThatResult(shardBResult).meetsConditions();

    List<Map<String, Object>> rows =
        jdbcResourceManagerShardA.runSQLQuery(
            "SELECT SingerId,FirstName FROM Singers ORDER BY SingerId");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("SingerId")).isEqualTo(1);
    assertThat(rows.get(1).get("SingerId")).isEqualTo(3);

    List<Map<String, Object>> shardBRows =
        jdbcResourceManagerShardB.runSQLQuery(
            "SELECT SingerId,FirstName FROM Singers ORDER BY SingerId");
    assertThat(shardBRows).hasSize(2);
    assertThat(shardBRows.get(0).get("SingerId")).isEqualTo(2);
    assertThat(shardBRows.get(1).get("SingerId")).isEqualTo(4);
  }

  private void writeSpannerDataForSingers(int singerId, String firstName, String shardId) {
    // Write a single record to Spanner
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(singerId)
            .set("FirstName")
            .to(firstName)
            .set("migration_shard_id")
            .to(shardId)
            .build();
    spannerResourceManager.write(m);
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("testShardA");
    shard.setUser(jdbcResourceManagerShardA.getUsername());
    shard.setHost(jdbcResourceManagerShardA.getHost());
    shard.setPassword(jdbcResourceManagerShardA.getPassword());
    shard.setPort(String.valueOf(jdbcResourceManagerShardA.getPort()));
    shard.setDbName(jdbcResourceManagerShardA.getDatabaseName());
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri

    Shard shardB = new Shard();
    shardB.setLogicalShardId("testShardB");
    shardB.setUser(jdbcResourceManagerShardB.getUsername());
    shardB.setHost(jdbcResourceManagerShardB.getHost());
    shardB.setPassword(jdbcResourceManagerShardB.getPassword());
    shardB.setPort(String.valueOf(jdbcResourceManagerShardB.getPort()));
    shardB.setDbName(jdbcResourceManagerShardB.getDatabaseName());
    JsonObject jsObjB = (JsonObject) new Gson().toJsonTree(shardB).getAsJsonObject();
    jsObjB.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    ja.add(jsObjB);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }
}
