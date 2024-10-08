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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for checking the timezone conversion. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class TimezoneIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TimezoneIT.class);

  private static final String SPANNER_DDL_RESOURCE = "TimezoneIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "TimezoneIT/session.json";

  private static final String TABLE = "Users";
  private static HashSet<TimezoneIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo writerJobInfo;
  private static PipelineLauncher.LaunchInfo readerJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (TimezoneIT.class) {
      testInstances.add(this);
      if (writerJobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();
        createMySQLSchema(jdbcResourceManager);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

        launchReaderDataflowJob();
        launchWriterDataflowJob();
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
    for (TimezoneIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        flexTemplateDataflowJobResourceManager);
  }

  @Test
  public void testGCSToSource() throws IOException, InterruptedException {
    assertThatPipeline(readerJobInfo).isRunning();
    assertThatPipeline(writerJobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();

    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void writeRowInSpanner() {
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Users")
            .set("id")
            .to(1)
            .set("time_colm")
            .to(Timestamp.parseTimestamp("2024-02-02T00:00:00Z"))
            .build();
    spannerResourceManager.write(m);
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder("Users")
            .set("id")
            .to(2)
            .set("time_colm")
            .to(Timestamp.parseTimestamp("2024-02-02T10:00:00Z"))
            .build();
    spannerResourceManager.write(m2);
    Mutation m3 =
        Mutation.newInsertOrUpdateBuilder("Users")
            .set("id")
            .to(3)
            .set("time_colm")
            .to(Timestamp.parseTimestamp("2024-02-02T20:00:00Z"))
            .build();
    spannerResourceManager.write(m3);
  }

  private void assertRowInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE) == 3);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows =
        jdbcResourceManager.runSQLQuery("SELECT id,time_colm FROM Users ORDER BY id");
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("time_colm"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-02-02 10:00:00.0"));
    assertThat(rows.get(1).get("id")).isEqualTo(2);
    assertThat(rows.get(1).get("time_colm"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-02-02 20:00:00.0"));
    assertThat(rows.get(2).get("id")).isEqualTo(3);
    assertThat(rows.get(2).get("time_colm"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-02-03 06:00:00.0"));
  }

  private SpannerResourceManager createSpannerDatabase(String spannerDdlResourceFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(spannerDdlResourceFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    return spannerResourceManager;
  }

  private SpannerResourceManager createSpannerMetadataDatabase() throws IOException {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  private void createMySQLSchema(MySQLResourceManager jdbcResourceManager) {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT NOT NULL");
    columns.put("time_colm", "TIMESTAMP");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    jdbcResourceManager.createTable(TABLE, schema);
  }

  private void launchWriterDataflowJob() throws IOException {
    Map<String, String> params =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put("sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager));
            put("runIdentifier", "run1");
            put("sourceDbTimezoneOffset", "+10:00");
            put("GCSInputDirectoryPath", getGcsPath("output", gcsResourceManager));
          }
        };
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    // Run
    writerJobInfo = launchTemplate(options, false);
  }

  private void launchReaderDataflowJob() throws IOException {
    // default parameters
    flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(getClass().getSimpleName())
            .withTemplateName("Spanner_Change_Streams_to_Sharded_File_Sink")
            .withTemplateModulePath("v2/spanner-change-streams-to-sharded-file-sink")
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("metadataDatabase", spannerMetadataResourceManager.getDatabaseId())
            .addParameter("metadataInstance", spannerMetadataResourceManager.getInstanceId())
            .addParameter(
                "sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager))
            .addParameter("changeStreamName", "allstream")
            .addParameter("runIdentifier", "run1")
            .addParameter("gcsOutputDirectory", getGcsPath("output", gcsResourceManager))
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"))
            .build();
    // Run
    readerJobInfo = flexTemplateDataflowJobResourceManager.launchJob();
  }

  private void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, MySQLResourceManager jdbcResourceManager)
      throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(jdbcResourceManager.getUsername());
    shard.setHost(jdbcResourceManager.getHost());
    shard.setPassword(jdbcResourceManager.getPassword());
    shard.setPort(String.valueOf(jdbcResourceManager.getPort()));
    shard.setDbName(jdbcResourceManager.getDatabaseName());
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }
}
