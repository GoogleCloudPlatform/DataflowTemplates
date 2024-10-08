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

import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.gcp.storage.conditions.GCSArtifactsCheck;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for simple test of single shard,single table. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerChangeStreamsToShardedFileSink.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamToGcsMultiShardIT extends SpannerChangeStreamToGcsITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamToGcsMultiShardIT.class);
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static HashSet<SpannerChangeStreamToGcsMultiShardIT> testInstances = new HashSet<>();
  private static final String spannerDdl =
      "SpannerChangeStreamToGcsMultiShardIT/spanner-schema.sql";
  private static final String sessionFileResourceName =
      "SpannerChangeStreamToGcsMultiShardIT/session.json";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static String spannerDatabaseName = "";
  private static String spannerMetadataDatabaseName = "";
  private static GcsResourceManager gcsResourceManager;

  /**
   * Does the following setup:
   *
   * <p>1. Creates a Spanner database with a given table 2. Creates a shard file with the connection
   * details 3. The session file for the same is taken from the resources and uploaded to GCS 4.
   * Places the session file and shard file in GCS 5. Creates the change stream in Spanner database
   * 6. Creates the metadata database 7. Launches the job to read from Spanner and write to GCS
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerChangeStreamToGcsMultiShardIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        gcsResourceManager = createGcsResourceManager(getClass().getSimpleName());
        spannerResourceManager = createSpannerResourceManager();
        spannerMetadataResourceManager = createSpannerMetadataResourceManager();
        prepareLaunchParameters(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            spannerDdl,
            sessionFileResourceName);
        createAndUploadShardConfigToGcs();
        jobInfo =
            launchReaderDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                getClass().getSimpleName(),
                null,
                null,
                true);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerChangeStreamToGcsMultiShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, spannerMetadataResourceManager, gcsResourceManager);
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    List<String> shardNames = new ArrayList<>();
    shardNames.add("testShardB");
    shardNames.add("testShardC");
    JsonArray ja = new JsonArray();

    for (String shardName : shardNames) {
      Shard shard = new Shard();
      shard.setLogicalShardId(shardName);
      shard.setUser("dummy");
      shard.setHost("dummy");
      shard.setPassword("dummy");
      shard.setPort("3306");
      JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
      ja.add(jsObj);
    }

    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    // -DartifactBucket has the bucket name
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  private void writeSpannerDataForSingers(int singerId, String firstName, String shardId) {
    // Write a single record to Spanner for the given logical shard
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

  @Test
  public void testMultiShardsRecordWrittenToGcs()
      throws IOException, java.lang.InterruptedException {
    // Construct a ChainedConditionCheck with below stages.
    // 1. Wait for the metadata table to have the start time of reader job
    // 2. Write 2 records per shard to Spanner
    // 3. Wait on GCS to have the files
    // 4. Match the PK in GCS with the PK written to Spanner
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(
                            spannerMetadataResourceManager, "spanner_to_gcs_metadata")
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();
    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);
    // Assert Conditions
    assertThatResult(result).meetsConditions();
    // Perform writes to Spanner
    writeSpannerDataForSingers(2, "two", "testShardB");
    writeSpannerDataForSingers(3, "three", "testShardB");
    writeSpannerDataForSingers(4, "four", "testShardC");
    writeSpannerDataForSingers(5, "five", "testShardC");

    // Assert file present in GCS with the needed data
    assertFileContentsInGCSForMultipleShards();
  }

  private void assertFileContentsInGCSForMultipleShards() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    GCSArtifactsCheck.builder(
                            gcsResourceManager, "output/testShardB/", Pattern.compile(".*\\.txt$"))
                        .setMinSize(1)
                        .setMaxSize(2)
                        .build(),
                    GCSArtifactsCheck.builder(
                            gcsResourceManager, "output/testShardC/", Pattern.compile(".*\\.txt$"))
                        .setMinSize(1)
                        .setMaxSize(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(6)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    List<Artifact> artifactsShardB =
        gcsResourceManager.listArtifacts("output/testShardB/", Pattern.compile(".*\\.txt$"));
    List<Artifact> artifactsShardC =
        gcsResourceManager.listArtifacts("output/testShardC/", Pattern.compile(".*\\.txt$"));
    // checks that any of the artifact has the given content
    assertThatArtifacts(artifactsShardB).hasContent("SingerId\\\":\\\"2");
    assertThatArtifacts(artifactsShardB).hasContent("SingerId\\\":\\\"3");
    assertThatArtifacts(artifactsShardC).hasContent("SingerId\\\":\\\"4");
    assertThatArtifacts(artifactsShardC).hasContent("SingerId\\\":\\\"5");
  }

  @Test
  public void testForwardMigrationFiltered() throws IOException, java.lang.InterruptedException {
    // Construct a ChainedConditionCheck with below stages.
    // 1. Wait for the metadata table to have the start time of reader job
    // 2. Write 1 records per shard to Spanner with the transaction tag as txBy=
    // 3. Wait and check there are no files in GCS for that shard
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(
                            spannerMetadataResourceManager, "spanner_to_gcs_metadata")
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();
    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);
    // Assert Conditions
    assertThatResult(result).meetsConditions();
    // Perform writes to Spanner
    writeSpannerDataForForwardMigration(7, "seven", "testShardD");
    // Assert file present in GCS with the needed data
    assertFileContentsInGCSForFilteredRecords();
  }

  private void assertFileContentsInGCSForFilteredRecords() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    GCSArtifactsCheck.builder(
                            gcsResourceManager, "output/testShardD/", Pattern.compile(".*\\.txt$"))
                        .setMinSize(1)
                        .setMaxSize(1)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(6)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).hasTimedOut();
  }

  private void writeSpannerDataForForwardMigration(int singerId, String firstName, String shardId) {
    // Write a single record to Spanner for the given logical shard
    // Add the record with the transaction tag as txBy=
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(PROJECT)
            .withInstanceId(spannerResourceManager.getInstanceId())
            .withDatabaseId(spannerResourceManager.getDatabaseId());
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag("txBy=forwardMigration"),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .run(
            (TransactionCallable<Void>)
                transaction -> {
                  Mutation m =
                      Mutation.newInsertOrUpdateBuilder("Singers")
                          .set("SingerId")
                          .to(singerId)
                          .set("FirstName")
                          .to(firstName)
                          .set("migration_shard_id")
                          .to(shardId)
                          .build();
                  transaction.buffer(m);
                  return null;
                });
  }
}
