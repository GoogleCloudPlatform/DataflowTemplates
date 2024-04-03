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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests use-cases where a
 * session file is required.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerSessionIT extends DataStreamToSpannerITBase {

  private static final String TABLE = "Category";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<DataStreamToSpannerSessionIT> testInstances = new HashSet<>();
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerSessionIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerSessionIT/mysql-session.json";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                "SessionIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "json");
                  }
                });
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
    for (DataStreamToSpannerSessionIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void migrationTestWithRenameAndDropColumn() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "backfill.jsonl",
                        "DataStreamToSpannerSessionIT/mysql-backfill-Category.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertCategoryTableBackfillContents();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSessionIT/mysql-cdc-Category.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertCategoryTableCdcContents();
  }

  private void assertCategoryTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 1);
    row1.put("full_name", "xyz");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 2);
    row2.put("full_name", "abc");

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertCategoryTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 2);
    row1.put("full_name", "abc1");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 3);
    row2.put("full_name", "def");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("category_id", 4);
    row3.put("full_name", "ghi");

    events.add(row1);
    events.add(row2);
    events.add(row3);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
