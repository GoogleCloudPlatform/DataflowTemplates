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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link GCSToSourceDb} Flex template without launching reader job. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class GCSToSourceDbWithoutReaderIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbWithoutReaderIT.class);

  private static final String SESSION_FILE_RESOURSE = "GCSToSourceDbWithoutReaderIT/session.json";

  private static final String TABLE = "Users";
  private static final String TABLE2 = "AllDatatypeTransformation";
  private static HashSet<GCSToSourceDbWithoutReaderIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static CustomMySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (GCSToSourceDbWithoutReaderIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = CustomMySQLResourceManager.builder(testName).build();
        createMySQLSchema(jdbcResourceManager);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

        createAndUploadJarToGcs(gcsResourceManager);
        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "customTransformation.jar", "com.custom.CustomTransformationWithShardForIT")
                .build();
        launchWriterDataflowJob(customTransformation);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  /*@AfterClass
  public static void cleanUp() throws IOException {
    for (GCSToSourceDbWithoutReaderIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        jdbcResourceManager, spannerMetadataResourceManager, gcsResourceManager);
  }*/

  @Test
  public void testGCSToSource() throws IOException, InterruptedException {
    // Write events to GCS
    gcsResourceManager.uploadArtifact(
        "output/Shard1/2024-05-13T08:43:10.000Z-2024-05-13T08:43:20.000Z-pane-0-last-0-of-1.txt",
        Resources.getResource("GCSToSourceDbWithoutReaderIT/events.txt").getPath());
    assertThatPipeline(jobInfo).isRunning();

    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void assertRowInMySQL() throws InterruptedException {
    long rowCount = 0;
    for (int i = 0; rowCount != 1 && i < 60; ++i) {
      rowCount = jdbcResourceManager.getRowCount(TABLE);
      LOG.info("Row count = {}, Waiting for 30s if row count not = 1", rowCount);
      Thread.sleep(10000);
    }
    assertThat(rowCount).isEqualTo(1);
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("name")).isEqualTo("FF");

    for (int i = 0; rowCount != 2 && i < 60; ++i) {
      rowCount = jdbcResourceManager.getRowCount(TABLE2);
      LOG.info("Row count = {}, Waiting for 30s if row count not = 2", rowCount);
      Thread.sleep(10000);
    }
    assertThat(rowCount).isEqualTo(2);
    rows = jdbcResourceManager.readTable(TABLE2);
    assertThat(rows).hasSize(2);
    String sortColumn = "varchar_column";
    List<Map<String, Object>> sortedRows =
        rows.stream()
            .sorted(Comparator.comparing(row -> (String) row.get(sortColumn)))
            .collect(Collectors.toList());
    assertThat(sortedRows.get(1).get("varchar_column")).isEqualTo("example2");
    assertThat(sortedRows.get(1).get("bigint_column")).isEqualTo(1000);
    assertThat(sortedRows.get(1).get("binary_column"))
        .isEqualTo("bin_column".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(1).get("bit_column")).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(1).get("blob_column"))
        .isEqualTo("blob_column".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(1).get("bool_column")).isEqualTo(true);
    assertThat(sortedRows.get(1).get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-01-01"));
    assertThat(sortedRows.get(1).get("datetime_column"))
        .isEqualTo(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 56));
    assertThat(sortedRows.get(1).get("decimal_column")).isEqualTo(new BigDecimal("99999.99"));
    assertThat(sortedRows.get(1).get("double_column")).isEqualTo(123456.123);
    assertThat(sortedRows.get(1).get("enum_column")).isEqualTo("1");
    assertThat(sortedRows.get(1).get("float_column")).isEqualTo(12345.67f);
    assertThat(sortedRows.get(1).get("int_column")).isEqualTo(100);
    assertThat(sortedRows.get(1).get("text_column")).isEqualTo("Sample text for entry 2");
    assertThat(sortedRows.get(1).get("time_column")).isEqualTo(java.sql.Time.valueOf("14:30:00"));
    assertThat(sortedRows.get(1).get("timestamp_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:56.0"));
    assertThat(sortedRows.get(1).get("tinyint_column")).isEqualTo(2);
    assertThat(sortedRows.get(1).get("year_column")).isEqualTo(java.sql.Date.valueOf("2024-01-01"));

    assertThat(sortedRows.get(0).get("varchar_column")).isEqualTo("example");
    assertThat(sortedRows.get(0).get("bigint_column")).isEqualTo(12346);
    assertThat(sortedRows.get(0).get("binary_column"))
        .isEqualTo("binary_column_appended".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(0).get("bit_column")).isEqualTo("5".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(0).get("blob_column"))
        .isEqualTo("blob_column_appended".getBytes(StandardCharsets.UTF_8));
    assertThat(sortedRows.get(0).get("bool_column")).isEqualTo(false);
    assertThat(sortedRows.get(0).get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-01-02"));
    assertThat(sortedRows.get(0).get("datetime_column"))
        .isEqualTo(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 55));
    assertThat(sortedRows.get(0).get("decimal_column")).isEqualTo(new BigDecimal("12344.67"));
    assertThat(sortedRows.get(0).get("double_column")).isEqualTo(124.456);
    assertThat(sortedRows.get(0).get("enum_column")).isEqualTo("3");
    assertThat(sortedRows.get(0).get("float_column")).isEqualTo(124.45f);
    assertThat(sortedRows.get(0).get("int_column")).isEqualTo(124);
    assertThat(sortedRows.get(0).get("text_column")).isEqualTo("Sample text append");
    assertThat(sortedRows.get(0).get("time_column")).isEqualTo(java.sql.Time.valueOf("14:40:00"));
    assertThat(sortedRows.get(0).get("timestamp_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:55.0"));
    assertThat(sortedRows.get(0).get("tinyint_column")).isEqualTo(2);
    assertThat(sortedRows.get(0).get("year_column")).isEqualTo(java.sql.Date.valueOf("2025-01-01"));
  }

  private SpannerResourceManager createSpannerMetadataDatabase() throws IOException {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build(); // DB name is appended with prefix to avoid clashes
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  private void createMySQLSchema(CustomMySQLResourceManager jdbcResourceManager) {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT NOT NULL");
    columns.put("name", "VARCHAR(25)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    jdbcResourceManager.createTable(TABLE, schema);

    columns.clear();
    columns.put("varchar_column", "VARCHAR(20) NOT NULL");
    columns.put("tinyint_column", "TINYINT");
    columns.put("text_column", "TEXT");
    columns.put("date_column", "DATE");
    columns.put("int_column", "INT");
    columns.put("bigint_column", "BIGINT");
    columns.put("float_column", "FLOAT(10,2)");
    columns.put("double_column", "DOUBLE");
    columns.put("decimal_column", "DECIMAL(10,2)");
    columns.put("datetime_column", "DATETIME");
    columns.put("timestamp_column", "TIMESTAMP");
    columns.put("time_column", "TIME");
    columns.put("year_column", "YEAR");
    columns.put("blob_column", "BLOB");
    columns.put("enum_column", "ENUM('1','2','3')");
    columns.put("bool_column", "TINYINT(1)");
    columns.put("binary_column", "VARBINARY(150)");
    columns.put("bit_column", "BIT(8)");
    schema = new JDBCResourceManager.JDBCSchema(columns, "varchar_column");
    jdbcResourceManager.createTable(TABLE2, schema);
  }

  private void launchWriterDataflowJob(CustomTransformation customTransformation)
      throws IOException {
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
            put("startTimestamp", "2024-05-13T08:43:10.000Z");
            put("windowDuration", "10s");
          }
        };

    if (customTransformation != null) {
      params.put(
          "transformationJarPath",
          getGcsPath("input/" + customTransformation.jarPath(), gcsResourceManager));
      params.put("transformationClassName", customTransformation.classPath());
    }
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    // Run
    jobInfo = launchTemplate(options, false);
  }

  private void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, CustomMySQLResourceManager jdbcResourceManager)
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

  public void createAndUploadJarToGcs(GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        "input/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }
}
