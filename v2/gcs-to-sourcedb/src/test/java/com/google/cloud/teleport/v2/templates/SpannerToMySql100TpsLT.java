/*
 * Copyright (C) 2023 Google LLC
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

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToMySql100TpsLT extends SpannerToJdbcLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySql100TpsLT.class);

  private static String generatorSchemaPath;
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String SPANNER_DDL_RESOURCE = "SpannerToMySql100TpsLT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToMySql100TpsLT/session.json";
  private static final String DATA_GENERATOR_SCHEMA_RESOURCE =
      "SpannerToMySql100TpsLT/datagenerator-schema.json";
  private static final String TABLE = "Person";
  private static final int MAX_WORKERS = 100;
  private static final int NUM_WORKERS = 50;
  private static PipelineLauncher.LaunchInfo writerJobInfo;
  private static PipelineLauncher.LaunchInfo readerJobInfo;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(SPANNER_DDL_RESOURCE, SESSION_FILE_RESOURCE, ARTIFACT_BUCKET);
    setupMySQLResourceManager(1);
    generatorSchemaPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            gcsResourceManager
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource(DATA_GENERATOR_SCHEMA_RESOURCE).getPath())
                .name());

    createMySQLSchema(jdbcResourceManagers);
    readerJobInfo =
        launchReaderDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            ARTIFACT_BUCKET,
            NUM_WORKERS,
            MAX_WORKERS);
    writerJobInfo =
        launchWriterDataflowJob(
            gcsResourceManager,
            spannerMetadataResourceManager,
            ARTIFACT_BUCKET,
            NUM_WORKERS,
            MAX_WORKERS);
  }

  @After
  public void tearDown() {
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplication100TpsLoadTest()
      throws IOException, ParseException, InterruptedException {
    // Start data generator
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("100")
            .setMessagesLimit(String.valueOf(300000))
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(TABLE)
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setBatchSizeBytes("0")
            .build();

    dataGenerator.execute(Duration.ofMinutes(90));
    assertThatPipeline(readerJobInfo).isRunning();
    assertThatPipeline(writerJobInfo).isRunning();

    JDBCRowsCheck check =
        JDBCRowsCheck.builder(jdbcResourceManagers.get(0), TABLE)
            .setMinRows(300000)
            .setMaxRows(300000)
            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(writerJobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(readerJobInfo, Duration.ofMinutes(5)));
    PipelineOperator.Result result2 =
        pipelineOperator.cancelJobAndFinish(createConfig(writerJobInfo, Duration.ofMinutes(5)));
    assertThatResult(result1).isLaunchFinished();
    assertThatResult(result2).isLaunchFinished();

    // export results
    exportMetricsToBigQuery(readerJobInfo, getMetrics(readerJobInfo));
    exportMetricsToBigQuery(writerJobInfo, getMetrics(writerJobInfo));
  }

  private void createMySQLSchema(List<JDBCResourceManager> jdbcResourceManagers) {
    if (!(jdbcResourceManagers.get(0) instanceof CustomMySQLResourceManager)) {
      throw new IllegalArgumentException(jdbcResourceManagers.get(0).getClass().getSimpleName());
    }
    CustomMySQLResourceManager jdbcResourceManager =
        (CustomMySQLResourceManager) jdbcResourceManagers.get(0);
    HashMap<String, String> columns = new HashMap<>();
    columns.put("first_name1", "varchar(500)");
    columns.put("last_name1", "varchar(500)");
    columns.put("first_name2", "varchar(500)");
    columns.put("last_name2", "varchar(500)");
    columns.put("first_name3", "varchar(500)");
    columns.put("last_name3", "varchar(500)");
    columns.put("ID", "varchar(100) NOT NULL");

    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "ID");

    jdbcResourceManager.createTable(TABLE, schema);
  }
}
