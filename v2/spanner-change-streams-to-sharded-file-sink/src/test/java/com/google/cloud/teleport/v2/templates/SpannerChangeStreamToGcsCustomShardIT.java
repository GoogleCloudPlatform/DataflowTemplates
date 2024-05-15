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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for multiple shard database with custom shard id. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerChangeStreamsToShardedFileSink.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamToGcsCustomShardIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToGcsSimpleIT.class);
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static HashSet<SpannerChangeStreamToGcsCustomShardIT> testInstances = new HashSet<>();
  private static final String spannerDdl =
      "SpannerChangeStreamToGcsCustomShardIT/spanner-schema.sql";
  private static final String sessionFileResourceName =
      "SpannerChangeStreamToGcsCustomShardIT/session.json";
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
   * 6. Creates the metadata database 7. Creates a JAR of v2/spanner-custom-shard and uploads it to
   * GCS 8. Launches the job to read from Spanner and write to GCS
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerChangeStreamToGcsSimpleIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        createGcsResourceManager();
        createSpannerDatabase();
        createAndUploadShardConfigToGcs();
        uploadSessionFileToGcs();
        createSpannerMetadataDatabase();
        createAndUploadJarToGcs();
        launchReaderDataflowJob();
      }
    }
  }

  @Test
  public void testMultiShardsRecordWrittenToGcsWithCustomShardId()
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
    writeSpannerDataForSingers(1, "one", "");
    writeSpannerDataForSingers(2, "two", "");
    writeSpannerDataForSingers(3, "three", "");
    writeSpannerDataForSingers(4, "four", "");

    // Assert file present in GCS with the needed data
    assertFileContentsInGCSForMultipleShards();
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

  private void assertFileContentsInGCSForMultipleShards() throws java.lang.InterruptedException {
    List<Artifact> artifactsShardA = null;
    List<Artifact> artifactsShardB = null;
    Thread.sleep(
        180000); // wait sufficiently for the file to be generated. It takes about 3 minutes
    // at-least. If not present wait additional 3 minutes before failing
    for (int i = 0; i < 10; i++) {
      Thread.sleep(18000); // wait for total 3 minutes over an interval of 18 seconds
      artifactsShardA =
          gcsResourceManager.listArtifacts("output/testShardA/", Pattern.compile(".*\\.txt$"));
      artifactsShardB =
          gcsResourceManager.listArtifacts("output/testShardB/", Pattern.compile(".*\\.txt$"));

      // Ideally both the mutations written to spanner per shard will commit within 10 seconds.
      // But that does not guarantee that they will be in the same file, since they can commit
      // within 1 second interval boundary
      if (artifactsShardB.size() >= 1 && artifactsShardA.size() >= 1) {
        break;
      }
    }
    assertThatArtifacts(artifactsShardB).hasFiles();
    assertThatArtifacts(artifactsShardA).hasFiles();
    // checks that any of the artifact has the given content
    assertThatArtifacts(artifactsShardB).hasContent("SingerId\\\":\\\"2");
    assertThatArtifacts(artifactsShardB).hasContent("SingerId\\\":\\\"4");
    assertThatArtifacts(artifactsShardA).hasContent("SingerId\\\":\\\"1");
    assertThatArtifacts(artifactsShardA).hasContent("SingerId\\\":\\\"3");
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    List<String> shardNames = new ArrayList<>();
    shardNames.add("testShardA");
    shardNames.add("testShardB");
    JsonArray ja = new JsonArray();

    for (String shardName : shardNames) {
      Shard shard = new Shard();
      shard.setLogicalShardId(shardName);
      shard.setUser("dummy");
      shard.setHost("dummy");
      shard.setPassword("dummy");
      shard.setPort("3306");
      JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
      jsObj.remove("secretManagerUri"); // remove field secretManagerUri
      ja.add(jsObj);
    }

    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    // -DartifactBucket has the bucket name
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  private void uploadSessionFileToGcs() throws IOException {
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResourceName).getPath());
  }

  private void createSpannerMetadataDatabase() throws IOException {
    spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build(); // DB name is appended with prefix to avoid clashes
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    // needed to create separate metadata database
    spannerMetadataDatabaseName = spannerMetadataResourceManager.getDatabaseId();
  }

  private void launchReaderDataflowJob() throws IOException {
    // default parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsFullPath("input/session.json"));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put("sourceShardsFilePath", getGcsFullPath("input/shard.json"));
            put("changeStreamName", "allstream");
            put("runIdentifier", "run1");
            put("gcsOutputDirectory", getGcsFullPath("output"));
            put("shardingCustomJarPath", getGcsFullPath("input/customShard.jar"));
            put("shardingCustomClassName", "com.custom.CustomShardIdFetcherForIT");
          }
        };

    // Construct template
    String jobName = PipelineUtils.createJobName("rr-it");
    // /-DunifiedWorker=true when using runner v2
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    // Run
    jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();
  }

  private void createSpannerDatabase() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build(); // DB name is appended with prefix to avoid clashes
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(spannerDdl), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    spannerDatabaseName = spannerResourceManager.getDatabaseId();
  }

  private void createAndUploadJarToGcs() throws IOException, InterruptedException {
    String[] commands = {"cd ../spanner-custom-shard", "mvn install"};

    // Join the commands with && to execute them sequentially
    String[] shellCommand = {"/bin/bash", "-c", String.join(" && ", commands)};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        "input/customShard.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  private void createGcsResourceManager() {
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build(); // DB name is appended with prefix to avoid clashes
  }

  private String getGcsFullPath(String artifactId) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), gcsResourceManager.runId(), artifactId);
  }
}
