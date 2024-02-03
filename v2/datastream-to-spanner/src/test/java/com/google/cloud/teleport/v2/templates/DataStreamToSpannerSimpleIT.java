/*
 * Copyright (C) 2023 Google LLC
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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Simple Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerSimpleIT extends DataStreamToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerSimpleIT.class);

  private static final String TABLE1 = "Users";
  private static final String TABLE2 = "Movie";

  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerSimpleIT/mysql-session.json";

  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerSimpleIT/spanner-schema.sql";

  private static HashSet<DataStreamToSpannerSimpleIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerSimpleIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                SPANNER_DDL_RESOURCE,
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
    for (DataStreamToSpannerSimpleIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void migrationTestWithUpdatesAndDeletes() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    // 3. Send second wave of events
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Users.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-cdc-Users.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertUsersTableContents();
  }

  @Test
  public void migrationTestWithInsertsOnly() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE2,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Movie.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
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

    // Assert specific rows
    assertMovieTableContents();
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("name", "Tester Kumar");
    row1.put("age_spanner", 30);
    row1.put("subscribed", false);
    row1.put("plan", "A");
    row1.put("startDate", Date.parseDate("2023-01-01"));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 3);
    row2.put("name", "Tester Gupta");
    row2.put("age_spanner", 50);
    row2.put("subscribed", false);
    row2.put("plan", "Z");
    row2.put("startDate", Date.parseDate("2023-06-07"));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 4);
    row3.put("name", "Tester");
    row3.put("age_spanner", 38);
    row3.put("subscribed", true);
    row3.put("plan", "D");
    row3.put("startDate", Date.parseDate("2023-09-10"));
    events.add(row1);
    events.add(row2);
    events.add(row3);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Users where id in (1, 3, 4)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertMovieTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("name", "movie1");
    row1.put("startTime", Timestamp.parseTimestamp("2023-01-01T12:12:12.000"));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2);
    row2.put("name", "movie2");
    row2.put("startTime", Timestamp.parseTimestamp("2023-11-25T17:10:12.000"));

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, name, startTime from Movie where id in (1, 2)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);

    ImmutableList<Struct> numericVals =
        spannerResourceManager.runQuery("select actor from Movie order by id");
    Assert.assertEquals(123.098, numericVals.get(0).getBigDecimal(0).doubleValue(), 0.001);
    Assert.assertEquals(931.512, numericVals.get(1).getBigDecimal(0).doubleValue(), 0.001);
  }
}
