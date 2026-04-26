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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template with 5000 tables using parallel DDL
 * execution and data verification.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDb5kIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb5kIT.class);
  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);

  private SpannerResourceManager spannerResourceManager;
  private SpannerResourceManager spannerMetadataResourceManager;
  private MySQLResourceManager jdbcResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();
    pubsubResourceManager = setUpPubSubResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLargeSchemaLaunch() throws Exception {
    gcsResourceManager.uploadArtifact(
        "input/large_session.json",
        Resources.getResource("SpannerToSourceDb5kIT/large_session.json").getPath());

    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

    SubscriptionName subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);

    LOG.info("Collecting 5000 Spanner DDL statements...");
    List<String> spannerDdls = new ArrayList<>();
    for (int i = 1; i <= 5000; i++) {
      spannerDdls.add(
          String.format("CREATE TABLE table_%d (id STRING(100) NOT NULL) PRIMARY KEY(id)", i));
    }

    LOG.info("Executing Spanner DDLs in parallel batches...");
    int batchSize = 100;
    ExecutorService ddlExecutor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < spannerDdls.size(); i += batchSize) {
      int end = Math.min(spannerDdls.size(), i + batchSize);
      List<String> batch = spannerDdls.subList(i, end);
      ddlExecutor.submit(
          () -> {
            try {
              spannerResourceManager.executeDdlStatements(batch);
            } catch (Exception e) {
              LOG.error("Failed to execute Spanner DDL batch", e);
            }
          });
    }
    ddlExecutor.shutdown();
    if (!ddlExecutor.awaitTermination(60, TimeUnit.MINUTES)) {
      throw new RuntimeException("Spanner DDL timed out after 60 minutes");
    }

    LOG.info("Creating change stream in Spanner...");
    spannerResourceManager.executeDdlStatement(
        "CREATE CHANGE STREAM allstream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d', allow_txn_exclusion = true)");

    LOG.info("Creating 5000 tables in MySQL...");
    try (java.sql.Connection connection =
            java.sql.DriverManager.getConnection(
                jdbcResourceManager.getUri(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword());
        java.sql.Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 5000; i++) {
        String mySqlDdl =
            String.format("CREATE TABLE table_%d (id VARCHAR(20) NOT NULL PRIMARY KEY)", i);
        statement.executeUpdate(mySqlDdl);
      }
    }

    LOG.info("Launching Dataflow job...");
    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put(
        "sessionFilePath", getGcsPath("input/large_session.json", gcsResourceManager));

    PipelineLauncher.LaunchInfo jobInfo =
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

    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Writing rows to Spanner...");
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 1; i <= 5000; i++) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder("table_" + i).set("id").to(String.valueOf(i)).build());
    }
    spannerResourceManager.write(mutations);

    LOG.info("Waiting for changes to propagate to MySQL...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> {
                  AtomicInteger matchedTables = new AtomicInteger(0);
                  ExecutorService checkExecutor = Executors.newFixedThreadPool(50);
                  for (int i = 1; i <= 5000; i++) {
                    String tableName = "table_" + i;
                    checkExecutor.submit(
                        () -> {
                          try {
                            if (jdbcResourceManager.getRowCount(tableName) == 1) {
                              matchedTables.incrementAndGet();
                            }
                          } catch (Exception ignored) {
                          }
                        });
                  }
                  checkExecutor.shutdown();
                  try {
                    checkExecutor.awaitTermination(10, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted while waiting for table checks", e);
                    return false;
                  }
                  LOG.info("Matched tables: {}/5000", matchedTables.get());
                  return matchedTables.get() == 5000;
                });
    assertThatResult(result).meetsConditions();
  }
}
