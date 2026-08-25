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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for max number of columns in Oracle.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleSourceDbWideRowMaxColumnsIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleSourceDbWideRowMaxColumnsIT.class);
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToSourceDbWideRowIT/max-col-session.json";
  private static final String TABLE1 = "testtable";
  private static final int NUM_NON_KEY_COLS = 100;
  private static final String COLUMN_SIZE = "100";
  private static final String ORACLE_SOURCE_TYPE = "oracle";

  private static HashSet<SpannerToOracleSourceDbWideRowMaxColumnsIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleSourceDbWideRowMaxColumnsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDBAndTableWithNColumns(TABLE1, NUM_NON_KEY_COLS, COLUMN_SIZE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
        testUsername = setupOracleIsolatedUser(jdbcResourceManager);

        createOracleTableWithNColumns(jdbcResourceManager, TABLE1, NUM_NON_KEY_COLS, COLUMN_SIZE);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
                gcsResourceManager);
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
              }
            };
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
                ORACLE_SOURCE_TYPE,
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
    for (SpannerToOracleSourceDbWideRowMaxColumnsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToOracleSourceDbMaxColTest()
      throws IOException, InterruptedException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowsInSpanner();
    // Assert events on Oracle
    assertRowInOracle();
  }

  private void writeRowsInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(TABLE1).set("id").to("SampleTest");

    for (int i = 1; i <= 100; i++) {
      mutationBuilder.set("col_" + i).to("TestValue_" + i);
    }

    mutations.add(mutationBuilder.build());
    spannerResourceManager.write(mutations);
    LOG.info("Inserted row with 100 columns into Spanner using Mutations");
  }

  private final List<Throwable> assertionErrors = new ArrayList<>();

  private void assertRowInOracle() throws MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    runIsolatedGetRowCount(jdbcResourceManager, testUsername, "\"" + TABLE1 + "\"")
                        == 1);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows =
        runIsolatedReadTable(jdbcResourceManager, testUsername, "\"" + TABLE1 + "\"");
    assertThat(rows).hasSize(1);
    Map<String, Object> row = rows.get(0);

    Map<String, Object> lowerCaseRow = new HashMap<>();
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      lowerCaseRow.put(entry.getKey().toLowerCase(), entry.getValue());
    }

    for (int i = 1; i <= 100; i++) {
      String columnName = "col_" + i;
      String expectedValue = "TestValue_" + i;

      try {
        assertThat(lowerCaseRow.get(columnName)).isEqualTo(expectedValue);
      } catch (Throwable e) {
        assertionErrors.add(e);
      }
    }
    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }

  @org.junit.AfterClass
  public static void flushRedo() {
    SpannerToSourceDbITBase.flushOracleRedoLogs(SharedOracleReverseITContainer.getInstance());
    SpannerToSourceDbITBase.clearIsolatedUser();
  }
}
