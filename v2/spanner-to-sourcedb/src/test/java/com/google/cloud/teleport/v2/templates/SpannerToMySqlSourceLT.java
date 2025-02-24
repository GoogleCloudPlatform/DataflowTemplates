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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
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
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
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
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToMySqlSourceLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySqlSourceLT.class);

  private String generatorSchemaPath;
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String spannerDdlResource = "SpannerToMySqlSourceLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToMySqlSourceLT/session.json";
  private final String dataGeneratorSchemaResource =
      "SpannerToMySqlSourceLT/datagenerator-schema.json";
  private final String table = "Person";
  private final int maxWorkers = 50;
  private final int numWorkers = 20;
  private PipelineLauncher.LaunchInfo jobInfo;
  private PipelineLauncher.LaunchInfo readerJobInfo;
  private final int numShards = 1;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(spannerDdlResource, sessionFileResource, artifactBucket);
    setupMySQLResourceManager(numShards);
    generatorSchemaPath =
        getFullGcsPath(
            artifactBucket,
            gcsResourceManager
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource(dataGeneratorSchemaResource).getPath())
                .name());

    createMySQLSchema(jdbcResourceManagers);
    jobInfo =
        launchDataflowJob(
            artifactBucket,
            numWorkers,
            maxWorkers,
            null,
            MYSQL_SOURCE_TYPE,
            SOURCE_SHARDS_FILE_NAME);
  }

  @After
  public void tearDown() {
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplication1KTpsLoadTest()
      throws IOException, ParseException, InterruptedException {
    // Start data generator
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("1000")
            .setMessagesLimit(String.valueOf(300000))
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(table)
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setBatchSizeBytes("0")
            .build();

    dataGenerator.execute(Duration.ofMinutes(90));
    assertThatPipeline(jobInfo).isRunning();

    JDBCRowsCheck check =
        JDBCRowsCheck.builder(jdbcResourceManagers.get(0), table)
            .setMinRows(300000)
            .setMaxRows(300000)
            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));

    assertThatResult(result1).isLaunchFinished();

    exportMetrics(jobInfo, numShards);
  }

  private void createMySQLSchema(List<JDBCResourceManager> jdbcResourceManagers) {
    if (!(jdbcResourceManagers.get(0) instanceof MySQLResourceManager)) {
      throw new IllegalArgumentException(jdbcResourceManagers.get(0).getClass().getSimpleName());
    }
    MySQLResourceManager jdbcResourceManager = (MySQLResourceManager) jdbcResourceManagers.get(0);
    HashMap<String, String> columns = new HashMap<>();
    columns.put("first_name1", "varchar(500)");
    columns.put("last_name1", "varchar(500)");
    columns.put("first_name2", "varchar(500)");
    columns.put("last_name2", "varchar(500)");
    columns.put("first_name3", "varchar(500)");
    columns.put("last_name3", "varchar(500)");
    columns.put("ID", "varchar(100) NOT NULL");

    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "ID");

    jdbcResourceManager.createTable(table, schema);
  }
}
