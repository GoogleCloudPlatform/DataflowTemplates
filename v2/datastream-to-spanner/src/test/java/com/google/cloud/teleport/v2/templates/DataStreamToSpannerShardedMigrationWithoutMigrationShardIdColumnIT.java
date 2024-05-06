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
import org.apache.beam.it.conditions.ConditionCheck;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sharded data migration Integration test without any migration_shard_id column transformation for
 * {@link DataStreamToSpanner} Flex template.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT
    extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(
          DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT.class);

  private static final String TABLE = "Users";
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/spanner-schema.sql";

  private static HashSet<DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT>
      testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo1;
  private static PipelineLauncher.LaunchInfo jobInfo2;

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
    synchronized (DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT.class) {
      testInstances.add(this);
      if (spannerResourceManager == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
      }
      if (pubsubResourceManager == null) {
        pubsubResourceManager = setUpPubSubResourceManager();
      }
      if (jobInfo1 == null) {
        jobInfo1 =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                null,
                null,
                "shard1",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "json");
                  }
                });
      }
      if (jobInfo2 == null) {
        jobInfo2 =
            launchDataflowJob(
                getClass().getSimpleName() + "shard2",
                null,
                null,
                "shard2",
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
    for (DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT instance :
        testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void multiShardMigration() {
    // Two dataflow jobs are running corresponding to two physical shards containing two logical
    // shards each. Migrates Users table from 4 logical shards. Asserts data from all the shards are
    // going to Spanner. Checks whether migration shard id column is populated properly based on the
    // transformation context.
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo1,
                        TABLE,
                        "Users-backfill-logical-shard1.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-backfill-logical-shard1.jsonl"),
                    uploadDataStreamFile(
                        jobInfo1,
                        TABLE,
                        "Users-backfill-logical-shard2.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-backfill-logical-shard2.jsonl"),
                    uploadDataStreamFile(
                        jobInfo1,
                        TABLE,
                        "Users-cdc-shard1.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-cdc-shard1.jsonl")))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo1, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo2,
                        TABLE,
                        "Users-backfill-logical-shard3.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-backfill-logical-shard3.jsonl"),
                    uploadDataStreamFile(
                        jobInfo2,
                        TABLE,
                        "Users-backfill-logical-shard4.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-backfill-logical-shard4.jsonl"),
                    uploadDataStreamFile(
                        jobInfo2,
                        TABLE,
                        "Users-cdc-shard2.jsonl",
                        "DataStreamToSpannerShardedMigrationWithoutMigrationShardIdColumnIT/Users-cdc-shard2.jsonl")))
            .build();

    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo2, Duration.ofMinutes(8)), conditionCheck);
    assertThatResult(result).meetsConditions();

    ConditionCheck rowsConditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, TABLE)
            .setMinRows(12)
            .setMaxRows(12)
            .build();
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo1, Duration.ofMinutes(10)), rowsConditionCheck);
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertUsersTableContents();
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "Tester1");
    row.put("age", 21);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 5);
    row.put("name", "Tester5");
    row.put("age", 23);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 6);
    row.put("name", "Tester6");
    row.put("age", 22);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 7);
    row.put("name", "Tester7");
    row.put("age", 7);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 8);
    row.put("name", "Tester8");
    row.put("age", 8);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 9);
    row.put("name", "Tester9");
    row.put("age", 9);
    events.add(row);

    row.put("id", 10);
    row.put("name", "Tester10");
    row.put("age", 10);
    events.add(row);

    row.put("id", 11);
    row.put("name", "Tester11");
    row.put("age", 11);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 12);
    row.put("name", "Tester12");
    row.put("age", 12);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 13);
    row.put("name", "Tester13");
    row.put("age", 13);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 14);
    row.put("name", "Tester14");
    row.put("age", 14);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 15);
    row.put("name", "Tester15");
    row.put("age", 15);
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
