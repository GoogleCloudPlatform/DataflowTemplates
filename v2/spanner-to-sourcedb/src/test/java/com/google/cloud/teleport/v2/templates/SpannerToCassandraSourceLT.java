/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.CASSANDRA_SOURCE_TYPE;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import org.apache.beam.it.cassandra.conditions.CassandraRowsCheck;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
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
public class SpannerToCassandraSourceLT extends SpannerToCassandraLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToCassandraSourceLT.class);
  private String generatorSchemaPath;
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String spannerDdlResource = "SpannerToCassandraSourceLT/spanner-schema.sql";
  private static final String cassandraDdlResource =
      "SpannerToCassandraSourceLT/cassandra-schema.sql";
  private final String dataGeneratorSchemaResource =
      "SpannerToCassandraSourceLT/datagenerator-schema.json";
  private final String table = "person";
  private final int maxWorkers = 50;
  private final int numWorkers = 20;
  private PipelineLauncher.LaunchInfo jobInfo;
  private final int numShards = 1;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(spannerDdlResource, cassandraDdlResource, artifactBucket);
    generatorSchemaPath =
        getFullGcsPath(
            artifactBucket,
            gcsResourceManager
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource(dataGeneratorSchemaResource).getPath())
                .name());
    jobInfo =
        launchDataflowJob(
            artifactBucket,
            numWorkers,
            maxWorkers,
            null,
            CASSANDRA_SOURCE_TYPE,
            SOURCE_SHARDS_FILE_NAME);
  }

  @After
  public void teardown() {
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplicationCassandra1KTpsLoadTest()
      throws IOException, ParseException, InterruptedException {

    Integer numRecords = 300000;
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("1000")
            .setMessagesLimit(String.valueOf(numRecords))
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

    CassandraRowsCheck check =
        CassandraRowsCheck.builder(table)
            .setResourceManager(cassandraResourceManager)
            .setMinRows(numRecords)
            .setMaxRows(numRecords)
            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));

    assertThatResult(result1).isLaunchFinished();

    exportMetrics(jobInfo, numShards);
  }
}
