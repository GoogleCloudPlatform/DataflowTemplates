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

import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.BOOKS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure injection test for the SourceDB to Spanner template. This test writes data which can
 * lead to severe errors in bulk migration and check the template behaviour.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SrcDbToSpannerMySQLSevereErrorNonShardedFT extends SourceDbToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SrcDbToSpannerMySQLSevereErrorNonShardedFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;

  private static CloudSqlResourceManager sourceDBResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create Source Resources
    sourceDBResourceManager = MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // Insert data before launching the job
    MySQLSrcDataProvider.writeRowsInSourceDB(1, 90, sourceDBResourceManager);
    MySQLSrcDataProvider.writeBookRowsInSourceDB(91, 100, 2000, sourceDBResourceManager);

    // launch forward migration template
    jobInfo =
        launchBulkDataflowJob(
            getClass().getSimpleName(),
            spannerResourceManager,
            gcsResourceManager,
            sourceDBResourceManager);
  }

  /**
   * Cleanup all the resources and resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, gcsResourceManager, sourceDBResourceManager);
  }

  @Test
  public void severeErrorFailureTest() {

    // Wait for Bulk migration job to be in running state
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(90)
                        .setMaxRows(90)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(90)
                        .setMaxRows(90)
                        .build(),
                    DlqEventsCountCheck.builder(gcsResourceManager, "output/dlq/severe/")
                        .setMinEvents(10)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }
}
