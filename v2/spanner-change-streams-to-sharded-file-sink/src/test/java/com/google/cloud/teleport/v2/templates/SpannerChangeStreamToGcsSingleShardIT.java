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
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for test of multiple shards. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerChangeStreamsToShardedFileSink.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamToGcsSingleShardIT extends SpannerChangeStreamToGcsITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamToGcsSingleShardIT.class);
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static HashSet<SpannerChangeStreamToGcsSingleShardIT> testInstances = new HashSet<>();
  private static final String spannerDdl =
      "SpannerChangeStreamToGcsSingleShardIT/spanner-schema.sql";
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
    synchronized (SpannerChangeStreamToGcsSingleShardIT.class) {
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
            null);
        createAndUploadShardConfigToGcs();
        jobInfo =
            launchReaderDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                getClass().getSimpleName(),
                null,
                null,
                false);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerChangeStreamToGcsSingleShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, spannerMetadataResourceManager, gcsResourceManager);
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    JsonArray ja = new JsonArray();
    Shard shard = new Shard();
    shard.setLogicalShardId("testShardA");
    shard.setUser("dummy");
    shard.setHost("dummy");
    shard.setPassword("dummy");
    shard.setPort("3306");
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    ja.add(jsObj);

    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    // -DartifactBucket has the bucket name
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
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
    writeSpannerDataForSingers(1, "FF");
    // Assert file present in GCS with the needed data
    assertFileContentsInGCS();
  }

  private void writeSpannerDataForSingers(int singerId, String firstName) {
    // Write a single record to Spanner for the given logical shard
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(singerId)
            .set("FirstName")
            .to(firstName)
            .build();
    spannerResourceManager.write(m);
  }

  private void assertFileContentsInGCS() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    GCSArtifactsCheck.builder(
                            gcsResourceManager, "output/testShardA/", Pattern.compile(".*\\.txt$"))
                        .setMinSize(1)
                        .setMaxSize(1)
                        .setArtifactContentMatcher("\"tableName\":\"Singers")
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    List<Artifact> artifacts =
        gcsResourceManager.listArtifacts("output/testShardA/", Pattern.compile(".*\\.txt$"));
    assertThatArtifacts(artifacts).hasContent("SingerId\\\":\\\"1");
  }

  @Test
  public void testAllDatatypes() throws IOException, java.lang.InterruptedException {
    // Construct a ChainedConditionCheck with below stages.
    // 1. Wait for the metadata table to have the start time of reader job
    // 2. Write a record with
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
    writeSpannerDataForAllDatatypes();
    // Assert file present in GCS with the needed data
    assertFileContentsInGCSForAllDatatypes();
  }

  private void writeSpannerDataForAllDatatypes() {
    // Write a single record to Spanner for logical shard : testD
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("sample_table")
            .set("id")
            .to(1)
            .set("varchar_column")
            .to("abc")
            .set("tinyint_column")
            .to(1)
            .set("text_column")
            .to("aaaaaddd")
            .set("year_column")
            .to("2023")
            .set("smallint_column")
            .to(22)
            .set("bigint_column")
            .to(12345678910L)
            .set("float_column")
            .to(4.2f)
            .set("double_column")
            .to(42.42d)
            .set("blob_column")
            .to("abc")
            .set("bool_column")
            .to(false)
            .set("binary_column")
            .to(Base64.getEncoder().encodeToString("Hello".getBytes()))
            .set("enum_column")
            .to("1")
            .set("timestamp_column")
            .to("2024-05-09T05:40:08.005683553Z")
            .set("date_column")
            .to("2024-05-09")
            .build();
    spannerResourceManager.write(m);
  }

  private void assertFileContentsInGCSForAllDatatypes() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    GCSArtifactsCheck.builder(
                            gcsResourceManager, "output/testShardA/", Pattern.compile(".*\\.txt$"))
                        .setMinSize(1)
                        .setArtifactContentMatcher("\"tableName\":\"sample_table")
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    List<Artifact> artifacts =
        gcsResourceManager.listArtifacts("output/testShardA/", Pattern.compile(".*\\.txt$"));

    assertThatArtifacts(artifacts).hasContent("id\\\":\\\"1");
    assertThatArtifacts(artifacts).hasContent("year_column\\\":\\\"2023");
    assertThatArtifacts(artifacts).hasContent("bigint_column\\\":\\\"12345678910");
    assertThatArtifacts(artifacts).hasContent("binary_column\\\":\\\"SGVsbG8");
    assertThatArtifacts(artifacts).hasContent("blob_column\\\":\\\"abc");
    assertThatArtifacts(artifacts).hasContent("bool_column\\\":false");
    assertThatArtifacts(artifacts).hasContent("char_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("date_column\\\":\\\"2024-05-09");
    assertThatArtifacts(artifacts).hasContent("datetime_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("decimal_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("double_column\\\":42.42");
    assertThatArtifacts(artifacts).hasContent("enum_column\\\":\\\"1");
    assertThatArtifacts(artifacts).hasContent("float_column\\\":4.199999809265137");
    assertThatArtifacts(artifacts).hasContent("longblob_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("longtext_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("mediumblob_column\\\":null");

    assertThatArtifacts(artifacts).hasContent("mediumint_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("mediumtext_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("smallint_column\\\":\\\"22");
    assertThatArtifacts(artifacts).hasContent("text_column\\\":\\\"aaaaaddd");
    assertThatArtifacts(artifacts).hasContent("time_column\\\":null");
    assertThatArtifacts(artifacts)
        .hasContent("timestamp_column\\\":\\\"2024-05-09T05:40:08.005683553Z");

    assertThatArtifacts(artifacts).hasContent("tinyblob_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("tinyint_column\\\":\\\"1");
    assertThatArtifacts(artifacts).hasContent("tinytext_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("update_ts\\\":null");
    assertThatArtifacts(artifacts).hasContent("varbinary_column\\\":null");
    assertThatArtifacts(artifacts).hasContent("varchar_column\\\":\\\"abc");
  }
}
