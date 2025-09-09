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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.SpannerDataProvider.AUTHORS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.conditions.CloudSQLRowsCheck;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure injection test for the Spanner to SourceDb template. This test writes data which can
 * lead to severe errors in reverse migration and check the template behaviour. This is the
 * non-sharded version of the test.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSrcDBMySQLSevereErrorNonShardedFT extends SpannerToSourceDbFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSrcDBMySQLSevereErrorNonShardedFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerFailureInjectionTesting/session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager cloudSqlResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    // Create MySql Resource
    cloudSqlResourceManager =
        MySQLSrcDataProvider.createSourceResourceManagerWithSchema(getClass().getSimpleName());

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // create and upload reverse migration config
    createAndUploadReverseShardConfigToGcs(
        gcsResourceManager, cloudSqlResourceManager, cloudSqlResourceManager.getHost());

    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

    // create pubsub manager
    pubsubResourceManager = setUpPubSubResourceManager();

    // launch reverse migration template
    jobInfo =
        launchRRDataflowJob(
            PipelineUtils.createJobName("rr" + getClass().getSimpleName()),
            spannerResourceManager,
            gcsResourceManager,
            spannerMetadataResourceManager,
            pubsubResourceManager,
            MYSQL_SOURCE_TYPE);
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cloudSqlResourceManager);
  }

  @Test
  public void severeErrorTest() throws IOException {
    assertThatPipeline(jobInfo).isRunning();

    // Write some data in Spanner
    writeAuthorRowsInSpanner(1, 100, "author_name_", spannerResourceManager);
    // Write large data which violates string length constraint in MySql
    writeAuthorRowsInSpanner(101, 105, "a".repeat(202), spannerResourceManager);

    ConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    CloudSQLRowsCheck.builder(cloudSqlResourceManager, AUTHORS_TABLE)
                        .setMinRows(100)
                        .build(),
                    DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                        .setMinEvents(5)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  private void writeAuthorRowsInSpanner(
      int startId, int endId, String authorName, SpannerResourceManager spannerResourceManager) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = startId; i <= endId; i++) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder(AUTHORS_TABLE)
              .set("author_id")
              .to(i)
              .set("name")
              .to(authorName)
              .build());
    }
    spannerResourceManager.write(mutations);
    LOG.info("Wrote {} rows to table {}", mutations.size(), AUTHORS_TABLE);
  }
}
