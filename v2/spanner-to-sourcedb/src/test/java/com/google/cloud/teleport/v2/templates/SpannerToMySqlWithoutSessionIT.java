/*
 * Copyright (C) 2026 Google LLC
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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.utils.SpannerGeneratedColumnUtils;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for basic run including new spanner
 * tables with generated column without session file.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToMySqlWithoutSessionIT extends SpannerToSourceDbITBase {
  @Rule public Timeout timeout = new Timeout(25, TimeUnit.MINUTES);

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySqlWithoutSessionIT.class);

  // Test timeout configuration - can be adjusted if tests need more time
  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(10);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToMySqlWithoutSessionIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToMySqlWithoutSessionIT/mysql-schema.sql";

  private static final HashSet<SpannerToMySqlWithoutSessionIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToMySqlWithoutSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToMySqlWithoutSessionIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager, SpannerToMySqlWithoutSessionIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        Map<String, String> jobParameters = new HashMap<>();
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                MYSQL_SOURCE_TYPE,
                jobParameters);
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
    for (SpannerToMySqlWithoutSessionIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToMySqlDataTypes() {
    LOG.info("Starting Spanner to MySQL Data Types IT");
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    SpannerGeneratedColumnUtils.addInitialMultiColSpannerData(spannerTableData);

    SpannerGeneratedColumnUtils.writeRowsInSpanner(spannerTableData, spannerResourceManager);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                SpannerGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    assertThatResult(result).meetsConditions();

    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    SpannerGeneratedColumnUtils.addInitialGeneratedColumnData(expectedData);
    // Assert events on Mysql
    SpannerGeneratedColumnUtils.assertRowInMySQL(expectedData, jdbcResourceManager);

    // Validating update and delete events.
    Map<String, List<Map<String, Value>>> updateSpannerTableData =
        SpannerGeneratedColumnUtils.updateGeneratedColRowsInSpanner(spannerResourceManager);
    spannerTableData.putAll(updateSpannerTableData);
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                SpannerGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    assertThatResult(result).meetsConditions();

    expectedData = new HashMap<>();
    SpannerGeneratedColumnUtils.addUpdatedGeneratedColumnData(expectedData);
    SpannerGeneratedColumnUtils.assertRowInMySQL(expectedData, jdbcResourceManager);
  }
}
