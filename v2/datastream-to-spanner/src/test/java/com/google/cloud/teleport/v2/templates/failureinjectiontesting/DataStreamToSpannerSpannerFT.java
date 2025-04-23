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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager.JDBCSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataStreamToSpannerSpannerFailureInjectionTest.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerSpannerFT
    extends DataStreamToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerSpannerFT.class);
  private static final String SPANNER_DDL_RESOURCE = "FailureInjectionTesting/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "FailureInjectionTesting/session.json";

  private static final String AUTHORS_TABLE = "Authors";
  private static final String BOOKS_TABLE = "Books";
  private static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static final HashMap<String, String> BOOK_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("author_id", "INT NOT NULL");
          put("book_id", "INT NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager cloudSqlResourceManager;
  private JDBCSource sourceConnectionProfile;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create MySql Resources
    cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();
    cloudSqlResourceManager.createTable(AUTHORS_TABLE, new JDBCSchema(AUTHOR_TABLE_COLUMNS, "id"));
    cloudSqlResourceManager.createTable(BOOKS_TABLE, new JDBCSchema(BOOK_TABLE_COLUMNS, "book_id"));
    sourceConnectionProfile =
        createMySQLSourceConnectionProfile(
            cloudSqlResourceManager, Arrays.asList(AUTHORS_TABLE, BOOKS_TABLE));

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();
    gcsResourceManager.createArtifact(
        "input/session.json",
        generateSessionFile(
            cloudSqlResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            SESSION_FILE_RESOURCE));

    // create pubsub manager
    pubsubResourceManager = setUpPubSubResourceManager();

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName)
            .withAdditionalMavenProfile("failureInjectionTest")
            .addParameter(
                "failureInjectionParameter",
                "{\"policyType\":\"InitialLimitedDurationErrorInjectionPolicy\"}");

    // launch forward migration template
    jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsResourceManager,
            pubsubResourceManager,
            flexTemplateBuilder,
            sourceConnectionProfile);
  }

  /**
   * Cleanup all the resources and resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, gcsResourceManager, pubsubResourceManager, cloudSqlResourceManager);
  }

  @Test
  public void spannerRetryableErrorFailureInjectionTest() {
    // Wait for Forward migration job to be in running state
    assertThatPipeline(jobInfo).isRunning();

    // Wave of inserts
    writeRowsInMySql(1, 20000, cloudSqlResourceManager);

    // Failure injection phase: check that retryable errors are there

    // Recovery phase: Wait for all events to appear in Spanner
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(20000)
                        .setMaxRows(20000)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(20000)
                        .setMaxRows(20000)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  protected boolean writeRowsInMySql(
      Integer startId, Integer endId, CloudSqlResourceManager cloudSqlResourceManager) {

    boolean success = true;
    List<Map<String, Object>> rows = new ArrayList<>();
    // Insert Authors
    for (int i = startId; i <= endId; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put("id", i);
      values.put("name", "author_name_" + i);
      rows.add(values);
    }
    success &= cloudSqlResourceManager.write(AUTHORS_TABLE, rows);
    LOG.info(String.format("Wrote %d rows to table %s", rows.size(), AUTHORS_TABLE));

    if (success) {
      // Insert Books
      for (int i = startId; i <= endId; i++) {
        Map<String, Object> values = new HashMap<>();
        values.put("author_id", i);
        values.put("book_id", i);
        values.put("name", "book_name_" + i);
        rows.add(values);
      }
      success &= cloudSqlResourceManager.write(BOOKS_TABLE, rows);
      LOG.info(String.format("Wrote %d rows to table %s", rows.size(), BOOKS_TABLE));
    }

    return success;
  }
}
