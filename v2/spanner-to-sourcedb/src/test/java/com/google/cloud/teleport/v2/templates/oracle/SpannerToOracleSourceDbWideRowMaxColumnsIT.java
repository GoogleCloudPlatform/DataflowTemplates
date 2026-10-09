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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.ORACLE_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
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
 * Integration test for {@link SpannerToSourceDb} Flex template for max number of columns, with an
 * Oracle source (reverse replication target). Oracle port of {@code
 * SpannerToMySqlSourceDbWideRowMaxColumnsIT}.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleSourceDbWideRowMaxColumnsIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleSourceDbWideRowMaxColumnsIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleSourceDbWideRowMaxColumnsIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE =
      "oracle/SpannerToOracleSourceDbWideRowMaxColumnsIT/oracle-schema.sql";
  private static final String TABLE1 = "testtable";

  /**
   * Oracle folds unquoted identifiers to upper case, while the schema preserves the lowercase names
   * of the Spanner table, so the table name must be quoted in Oracle SQL queries.
   */
  private static final String ORACLE_TABLE1 = "\"" + TABLE1 + "\"";

  private static final int NUM_NON_KEY_COLS = 100;

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
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = OracleResourceManager.builder(testName).build();

        loadSQLFileResource(jdbcResourceManager, ORACLE_DDL_RESOURCE);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
                gcsResourceManager);
        // No session file: the template falls back to the IdentityMapper, which requires the
        // Spanner and Oracle table/column names to match exactly (case-sensitive). This holds
        // because the Oracle DDL quotes the lowercase identifiers.
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
        jdbcResourceManager,
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

    for (int i = 1; i <= NUM_NON_KEY_COLS; i++) {
      mutationBuilder.set("col_" + i).to("TestValue_" + i);
    }

    mutations.add(mutationBuilder.build());
    spannerResourceManager.write(mutations);
    LOG.info("Inserted row with {} columns into Spanner using Mutations", NUM_NON_KEY_COLS);
  }

  private final List<Throwable> assertionErrors = new ArrayList<>();

  private void assertRowInOracle() throws MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(ORACLE_TABLE1) == 1); // only one row
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManager.readTable(ORACLE_TABLE1);
    assertThat(rows).hasSize(1);
    Map<String, Object> row = rows.get(0);
    for (int i = 1; i <= NUM_NON_KEY_COLS; i++) {
      String columnName = "col_" + i;
      String expectedValue = "TestValue_" + i;

      try {
        assertThat(row.get(columnName)).isEqualTo(expectedValue);
      } catch (Throwable e) {
        assertionErrors.add(e);
      }
    }
    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }
}
