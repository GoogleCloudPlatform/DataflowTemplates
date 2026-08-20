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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
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
import org.apache.beam.it.jdbc.OracleResourceManager;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleWithoutSessionIT extends SpannerToSourceDbITBase {
  @Rule public Timeout timeout = new Timeout(25, TimeUnit.MINUTES);

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToOracleWithoutSessionIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(10);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleWithoutSessionIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleWithoutSessionIT/oracle-schema.sql";

  private static final HashSet<SpannerToOracleWithoutSessionIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static OracleResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleWithoutSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToOracleWithoutSessionIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = OracleResourceManager.builder(testName).build();

        try {
          createOracleSchema(
              jdbcResourceManager, SpannerToOracleWithoutSessionIT.ORACLE_SCHEMA_FILE_RESOURCE);
        } catch (Exception e) {
          throw new IOException("Failed to create Oracle Schema", e);
        }

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

        // If your target source database relies on a proprietary JDBC driver that is excluded from
        // the main template deployment
        // Not specifically called out if oracle driver is staged dynamically... wait I should check
        // if we need `--jdbcDriverJars`
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
                "oracle",
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToOracleWithoutSessionIT instance : testInstances) {
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
  public void spannerToOracleGeneratedColumns() {
    LOG.info("Starting Spanner to Oracle Generated Columns IT");
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    OracleGeneratedColumnUtils.addInitialMultiColSpannerData(spannerTableData);

    OracleGeneratedColumnUtils.writeRowsInSpanner(spannerTableData, spannerResourceManager);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                OracleGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    assertThatResult(result).meetsConditions();

    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    OracleGeneratedColumnUtils.addInitialGeneratedColumnData(expectedData);
    OracleGeneratedColumnUtils.assertRowInOracle(expectedData, jdbcResourceManager);

    Map<String, List<Map<String, Value>>> updateSpannerTableData =
        OracleGeneratedColumnUtils.updateGeneratedColRowsInSpanner(spannerResourceManager);
    spannerTableData.putAll(updateSpannerTableData);
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                OracleGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    assertThatResult(result).meetsConditions();

    expectedData = new HashMap<>();
    OracleGeneratedColumnUtils.addUpdatedGeneratedColumnData(expectedData);
    OracleGeneratedColumnUtils.assertRowInOracle(expectedData, jdbcResourceManager);
  }
}
