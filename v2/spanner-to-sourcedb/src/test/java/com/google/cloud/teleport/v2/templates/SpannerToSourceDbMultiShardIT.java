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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
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

/** Integration test for {@link SpannerToSourceDb} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbMultiShardIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbMultiShardIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbMultiShardIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerToSourceDbMultiShardIT/session.json";
  private static final String MYSQL_DDL_RESOURCE = "SpannerToSourceDbMultiShardIT/mysql-schema.sql";

  private static HashSet<SpannerToSourceDbMultiShardIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo dataflowJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManagerShardA;
  private static MySQLResourceManager jdbcResourceManagerShardB;
  private static GcsResourceManager gcsResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbMultiShardIT.class) {
      testInstances.add(this);
      if (dataflowJobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = MySQLResourceManager.builder(testName + "shardA").build();
        createMySQLSchema(jdbcResourceManagerShardA);

        jdbcResourceManagerShardB = MySQLResourceManager.builder(testName + "shardB").build();
        createMySQLSchema(jdbcResourceManagerShardB);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs();
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

        launchDataflowJob();
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
    for (SpannerToSourceDbMultiShardIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
        spannerMetadataResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testOrderedWrites() throws IOException, InterruptedException {
    assertThatPipeline(dataflowJobInfo).isRunning();

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
    mutations.add(c1);
    mutations.add(c2);
    spannerResourceManager.write(mutations);
  }

  private void assertInsertedRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManagerShardB.getRowCount("child21") == 1);
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardA.getRowCount("parent1") == 1);
    assertThatResult(parent1Result).meetsConditions();
    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardA.getRowCount("child11") == 1);
    assertThatResult(child1Result).meetsConditions();
    PipelineOperator.Result parent2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardB.getRowCount("parent2") == 1);
    assertThatResult(parent2Result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManagerShardA.readTable("parent1");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);

    List<Map<String, Object>> rows1 = jdbcResourceManagerShardB.readTable("parent2");
    assertThat(rows1).hasSize(1);
    assertThat(rows1.get(0).get("id")).isEqualTo(2);

    List<Map<String, Object>> rows2 = jdbcResourceManagerShardA.readTable("child11");
    assertThat(rows2).hasSize(1);
    assertThat(rows2.get(0).get("child_id")).isEqualTo(11);

    List<Map<String, Object>> rows3 = jdbcResourceManagerShardB.readTable("child21");
    assertThat(rows3).hasSize(1);
    assertThat(rows3.get(0).get("child_id")).isEqualTo(22);
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

  private void assertUpdatedRowsInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManagerShardA.getRowCount("child11") == 2);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManagerShardA.readTable("parent1");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("update_ts"))
        .isEqualTo(java.sql.Timestamp.valueOf("1980-01-01 00:00:00.0"));

    List<Map<String, Object>> rows2 =
        jdbcResourceManagerShardA.runSQLQuery(
            "SELECT child_id,update_ts FROM child11 ORDER BY child_id");
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
                createConfig(dataflowJobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManagerShardB.getRowCount("parent2") == 0);
    assertThatResult(result).meetsConditions();
    // parent2 is the last row to be deleted. Hence by this time all other records must be deleted
    // too.

    PipelineOperator.Result parent1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardA.getRowCount("parent1") == 0);
    assertThatResult(parent1Result).meetsConditions();
    PipelineOperator.Result child1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardA.getRowCount("child11") == 0);
    assertThatResult(child1Result).meetsConditions();
    PipelineOperator.Result child2Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(dataflowJobInfo, Duration.ofSeconds(1)),
                () -> jdbcResourceManagerShardB.getRowCount("child22") == 0);
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

  private void launchDataflowJob() throws IOException {
    Map<String, String> params =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put("sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", PROJECT);
            put("changeStreamName", "allstream");
          }
        };
    String jobName = PipelineUtils.createJobName("rr-it" + testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));

    // Run
    dataflowJobInfo = launchTemplate(options, false);
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("shardA");
    shard.setUser(jdbcResourceManagerShardA.getUsername());
    shard.setHost(jdbcResourceManagerShardA.getHost());
    shard.setPassword(jdbcResourceManagerShardA.getPassword());
    shard.setPort(String.valueOf(jdbcResourceManagerShardA.getPort()));
    shard.setDbName(jdbcResourceManagerShardA.getDatabaseName());
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri

    Shard shardB = new Shard();
    shardB.setLogicalShardId("shardB");
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
