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
import java.util.concurrent.TimeUnit;
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
 * An integration test for {@link DataStreamToSpanner} Flex template which multiple use-cases
 * tested: 1. Foreign Keys. 2. Table Dropped. 3. Column Rename. 4. DLQ Retry. 5. Missing PK 6.
 * Indexes
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerMixedIT extends DataStreamToSpannerITBase {

  private static final String TABLE1 = "Authors";
  private static final String TABLE2 = "Books";
  private static final String TABLE3 = "Genre";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<DataStreamToSpannerMixedIT> testInstances = new HashSet<>();
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerMixedIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerMixedIT/mysql-session.json";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerMixedIT.class) {
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
                "MixedIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "avro");
                  }
                },
                null,
                null);
      }
    }
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerMixedIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void mixedMigrationTest() throws InterruptedException {
    // Construct a ChainedConditionCheck with 2 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "authors_1.avro",
                        "DataStreamToSpannerMixedIT/Authors_1.avro"),
                    uploadDataStreamFile(
                        jobInfo, TABLE2, "books.avro", "DataStreamToSpannerMixedIT/Books.avro"),
                    uploadDataStreamFile(
                        jobInfo, TABLE3, "genre.avro", "DataStreamToSpannerMixedIT/Genre.avro"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    TimeUnit.MINUTES.sleep(1);
    // Assert Conditions
    assertThatResult(result).meetsConditions();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "authors_2.avro",
                        "DataStreamToSpannerMixedIT/Authors_2.avro"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertAuthorsTableContents();

    assertBooksTableContents();
  }

  private void assertAuthorsTableContents() {
    List<Map<String, Object>> authorEvents = new ArrayList<>();

    Map<String, Object> authorRow1 = new HashMap<>();
    authorRow1.put("author_id", 4);
    authorRow1.put("full_name", "Stephen King");

    Map<String, Object> authorRow2 = new HashMap<>();
    authorRow2.put("author_id", 1);
    authorRow2.put("full_name", "Jane Austen");

    Map<String, Object> authorRow3 = new HashMap<>();
    authorRow3.put("author_id", 2);
    authorRow3.put("full_name", "Charles Dickens");

    Map<String, Object> authorRow4 = new HashMap<>();
    authorRow4.put("author_id", 3);
    authorRow4.put("full_name", "Leo Tolstoy");

    authorEvents.add(authorRow1);
    authorEvents.add(authorRow2);
    authorEvents.add(authorRow3);
    authorEvents.add(authorRow4);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(authorEvents);
  }

  private void assertBooksTableContents() {
    List<Map<String, Object>> bookEvents = new ArrayList<>();

    Map<String, Object> bookRow1 = new HashMap<>();
    bookRow1.put("id", 1);
    bookRow1.put("title", "Pride and Prejudice");

    Map<String, Object> bookRow2 = new HashMap<>();
    bookRow2.put("id", 2);
    bookRow2.put("title", "Oliver Twist");

    Map<String, Object> bookRow3 = new HashMap<>();
    bookRow3.put("id", 3);
    bookRow3.put("title", "War and Peace");

    bookEvents.add(bookRow1);
    bookEvents.add(bookRow2);
    bookEvents.add(bookRow3);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select id, title from Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(bookEvents);
  }
}
