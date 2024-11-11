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
public class DataStreamToSpannerEndToEndIT extends DataStreamToSpannerITBase {

  private static final String TABLE1 = "Authors";
  private static final String TABLE2 = "Books";
  private static final String TABLE3 = "Genre";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<DataStreamToSpannerEndToEndIT> testInstances = new HashSet<>();
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerEndToEndIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerEndToEndIT/mysql-session.json";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerEndToEndIT.class) {
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
                "EndToEndIT",
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
    for (DataStreamToSpannerEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void endToEndMigrationTest() {
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
                        "DataStreamToSpannerEndToEndIT/Authors_1.avro"),
                    uploadDataStreamFile(
                        jobInfo, TABLE2, "books.avro", "DataStreamToSpannerEndToEndIT/Books.avro"),
                    uploadDataStreamFile(
                        jobInfo, TABLE3, "genre.avro", "DataStreamToSpannerEndToEndIT/Genre.avro"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "authors_2.avro",
                        "DataStreamToSpannerEndToEndIT/Authors_2.avro"),
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
  }

  private void assertAuthorsTableContents() {
    List<Map<String, Object>> author_events = new ArrayList<>();

    Map<String, Object> author_row1 = new HashMap<>();
    author_row1.put("author_id", 4);
    author_row1.put("full_name", "Stephen King");

    Map<String, Object> author_row2 = new HashMap<>();
    author_row2.put("author_id", 1);
    author_row2.put("full_name", "Jane Austen");

    Map<String, Object> author_row3 = new HashMap<>();
    author_row3.put("author_id", 2);
    author_row3.put("full_name", "Charles Dickens");

    Map<String, Object> author_row4 = new HashMap<>();
    author_row4.put("author_id", 3);
    author_row4.put("full_name", "Leo Tolstoy");

    author_events.add(author_row1);
    author_events.add(author_row2);
    author_events.add(author_row3);
    author_events.add(author_row4);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(author_events);
  }
}
