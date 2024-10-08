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
import com.google.cloud.spanner.Key;
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
import java.util.ArrayList;
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

/** Integration test for {@link GCSToSourceDb} Flex template without launching reader job. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class GCSToSourceDbInterleaveIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbInterleaveIT.class);

  private static final String SPANNER_DDL_RESOURCE = "GCSToSourceDbInterleaveIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "GCSToSourceDbInterleaveIT/session.json";
  private static final String MYSQL_DDL_RESOURCE = "GCSToSourceDbInterleaveIT/mysql-schema.sql";

  private static HashSet<GCSToSourceDbInterleaveIT> testInstances = new HashSet<>();
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
    synchronized (GCSToSourceDbInterleaveIT.class) {
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
    for (GCSToSourceDbInterleaveIT instance : testInstances) {
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
  public void testOrderedWrites() throws IOException, InterruptedException {
    assertThatPipeline(readerJobInfo).isRunning();
    assertThatPipeline(writerJobInfo).isRunning();

    doInsertsInSpanner();
    assertInsertedRowsInMySQL();

    doUpdatesInSpanner();
    assertUpdatedRowsInMySQL();

    doDeletesInSpanner();
    assertUpdatedRowsInMySQL();
  }

  private void doInsertsInSpanner() {
    // Insert records
    List<Mutation> mutations = new ArrayList<>();
    Mutation p1 = Mutation.newInsertOrUpdateBuilder("parent1").set("id").to(1).build();
    spannerResourceManager.write(p1);

    Mutation p2 = Mutation.newInsertOrUpdateBuilder("parent2").set("id").to(2).build();
    spannerResourceManager.write(p2);

    Mutation c1 =
        Mutation.newInsertOrUpdateBuilder("child11")
            .set("child_id")
            .to(11)
            .set("parent_id")
            .to(1)
            .build();
    Mutation c2 =
        Mutation.newInsertOrUpdateBuilder("child21").set("child_id").to(22).set("id").to(2).build();
    mutations.add(c1);
    mutations.add(c2);
    spannerResourceManager.write(mutations);
  }

  private void assertInsertedRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount("child21") == 1);
    assertThatResult(result).meetsConditions();
    // child21 is the last row to be inserted. Hence by this time all other records must be updated
    // too.

    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("parent1") == 1);
    assertThatResult(parent1Result).meetsConditions();
    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("child11") == 1);
    assertThatResult(child1Result).meetsConditions();
    PipelineOperator.Result parent2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("parent2") == 1);
    assertThatResult(parent2Result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManager.readTable("parent1");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);

    List<Map<String, Object>> rows1 = jdbcResourceManager.readTable("parent2");
    assertThat(rows1).hasSize(1);
    assertThat(rows1.get(0).get("id")).isEqualTo(2);

    List<Map<String, Object>> rows2 = jdbcResourceManager.readTable("child11");
    assertThat(rows2).hasSize(1);
    assertThat(rows2.get(0).get("child_id")).isEqualTo(11);

    List<Map<String, Object>> rows3 = jdbcResourceManager.readTable("child21");
    assertThat(rows3).hasSize(1);
    assertThat(rows3.get(0).get("child_id")).isEqualTo(22);
  }

  private void doUpdatesInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation p1 =
        Mutation.newUpdateBuilder("parent1")
            .set("id")
            .to(1)
            .set("update_ts")
            .to(Timestamp.parseTimestamp("1980-01-01T00:00:00Z"))
            .build();
    Mutation c1 =
        Mutation.newUpdateBuilder("child11")
            .set("child_id")
            .to(11)
            .set("parent_id")
            .to(1)
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
            .build();
    mutations.add(p1);
    mutations.add(c1);
    mutations.add(c2);
    spannerResourceManager.write(mutations);
  }

  private void assertUpdatedRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount("child11") == 2);
    assertThatResult(result).meetsConditions();
    // we added one extra child row in spanner. Hence by this time all other records must be updated
    // too.

    List<Map<String, Object>> rows = jdbcResourceManager.readTable("parent1");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("update_ts"))
        .isEqualTo(java.sql.Timestamp.valueOf("1980-01-01 00:00:00.0"));

    List<Map<String, Object>> rows2 =
        jdbcResourceManager.runSQLQuery("SELECT child_id,update_ts FROM child11 ORDER BY child_id");
    assertThat(rows2).hasSize(2);
    assertThat(rows2.get(0).get("child_id")).isEqualTo(11);
    assertThat(rows.get(0).get("update_ts"))
        .isEqualTo(java.sql.Timestamp.valueOf("1980-01-01 00:00:00.0"));
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

  private void assertDeletedRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount("parent2") == 0);
    assertThatResult(result).meetsConditions();
    // parent2 is the last row to be deleted. Hence by this time all other records must be deleted
    // too.

    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("parent1") == 0);
    assertThatResult(parent1Result).meetsConditions();
    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("child11") == 0);
    assertThatResult(child1Result).meetsConditions();
    PipelineOperator.Result child2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(writerJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManager.getRowCount("child22") == 0);
    assertThatResult(child2Result).meetsConditions();
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

  private void createMySQLSchema(MySQLResourceManager jdbcResourceManager) throws IOException {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT NOT NULL");
    columns.put("name", "VARCHAR(25)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    jdbcResourceManager.createTable("test", schema);

    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(MYSQL_DDL_RESOURCE), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        jdbcResourceManager.runSQLUpdate(d);
      }
    }
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
