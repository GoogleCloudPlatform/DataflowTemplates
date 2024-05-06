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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for simple test of single shard,single table. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerChangeStreamsToShardedFileSink.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamToGcsSimpleIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToGcsSimpleIT.class);
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static HashSet<SpannerChangeStreamToGcsSimpleIT> testInstances = new HashSet<>();
  private static final String spannerDdl =
      "SpannerChangeStreamToGcsSimpleIT/spanner-schema-simple.sql";
  private static final String sessionFileResourceName =
      "SpannerChangeStreamToGcsSimpleIT/session.json";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static String spannerDatabaseName = "";
  private static String spannerMetadataDatabaseName = "";

  /**
   * Does the following setup:
   *
   * <p>1. Creates a Spanner database with a given table 2. Creates a shard file with the connection
   * details 3. The session file for the same is taken from the resources and uploaded to GCS 4.
   * Places the session file and shard file in GCS 5. Creates the change stream in Spanner database
   * 6. Creates the metadata database 8. Launches the job to read from Spanner and write to GCS
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    synchronized (SpannerChangeStreamToGcsSimpleIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        createSpannerDatabase();
        createAndUploadShardConfigToGcs();
        uploadSessionFileToGcs();
        createSpannerMetadataDatabase();
        launchReaderDataflowJob();
      }
    }
  }

  @After
  public void cleanUp() throws IOException {
    for (SpannerChangeStreamToGcsSimpleIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, spannerMetadataResourceManager);
  }

  @Test
  public void testSingleRecordWrittenToGcs() throws IOException, java.lang.InterruptedException {
    // Construct a ChainedConditionCheck with below stages.
    // 1. Wait for the metadata table to have the start time of reader job
    // 2. Write a single record to Spanner
    // 3. Wait on GCS to have the file
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
    writeSpannerData();
    // Assert file present in GCS with the needed data
    assertFileContentsInGCS();
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

  private void createAndUploadShardConfigToGcs() throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("testA");
    shard.setUser("dummy");
    shard.setHost("dummy");
    shard.setPassword("dummy");
    shard.setPort("3306");
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    // -DartifactBucket has the bucket name
    gcsClient.createArtifact("input/shard.json", shardFileContents);
  }

  private void uploadSessionFileToGcs() throws IOException {
    gcsClient.uploadArtifact(
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
            put("sessionFilePath", getGcsPath("input/session.json"));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put("sourceShardsFilePath", getGcsPath("input/shard.json"));
            put("changeStreamName", "allstream");
            put("runIdentifier", "run1");
            put("gcsOutputDirectory", getGcsPath("output"));
          }
        };

    // Construct template
    String jobName = PipelineUtils.createJobName(testName);
    // /-DunifiedWorker=true when using runner v2
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    // Run
    jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();
  }

  private void writeSpannerData() {
    // Write a single record to Spanner for logical shard : testA
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(1)
            .set("FirstName")
            .to("FF")
            .set("shardId")
            .to("testA")
            .build();
    spannerResourceManager.write(m);
  }

  private void assertFileContentsInGCS() throws IOException, java.lang.InterruptedException {
    List<Artifact> artifacts = null;
    Thread.sleep(300000); // wait sufficiently for the file to be generated
    artifacts = gcsClient.listArtifacts("output/testA/", Pattern.compile(".*\\.txt$"));

    LOG.info("The number of items in GCS: {}", artifacts.size());
    for (Artifact a : artifacts) {
      LOG.info("The file name is : {}", a.name());
    }
    assertThat(artifacts).hasSize(1);
    assertThatArtifacts(artifacts).hasContent("SingerId\\\":\\\"1");
  }
}
