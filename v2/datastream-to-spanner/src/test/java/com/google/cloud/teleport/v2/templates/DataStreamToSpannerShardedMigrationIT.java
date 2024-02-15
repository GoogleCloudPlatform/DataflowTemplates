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

/** Sharded data migration Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerShardedMigrationIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerShardedMigrationIT.class);

  private static final String TABLE = "Users";
  private static final String MOVIE_TABLE = "Movie";

  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerShardedMigrationIT/mysql-session.json";

  private static final String TRANSFORMATION_CONTEXT_RESOURCE_SHARD1 =
      "DataStreamToSpannerShardedMigrationIT/transformation-context-shard1.json";
  private static final String TRANSFORMATION_CONTEXT_RESOURCE_SHARD2 =
      "DataStreamToSpannerShardedMigrationIT/transformation-context-shard2.json";

  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerShardedMigrationIT/spanner-schema.sql";

  private static HashSet<DataStreamToSpannerShardedMigrationIT> testInstances = new HashSet<>();
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
    synchronized (DataStreamToSpannerShardedMigrationIT.class) {
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
                SESSION_FILE_RESOURCE,
                TRANSFORMATION_CONTEXT_RESOURCE_SHARD1,
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
                SESSION_FILE_RESOURCE,
                TRANSFORMATION_CONTEXT_RESOURCE_SHARD2,
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
    for (DataStreamToSpannerShardedMigrationIT instance : testInstances) {
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
                        "DataStreamToSpannerShardedMigrationIT/Users-backfill-logical-shard1.jsonl"),
                    uploadDataStreamFile(
                        jobInfo1,
                        TABLE,
                        "Users-backfill-logical-shard2.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Users-backfill-logical-shard2.jsonl"),
                    uploadDataStreamFile(
                        jobInfo1,
                        TABLE,
                        "Users-cdc-shard1.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Users-cdc-shard1.jsonl")))
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
                        "DataStreamToSpannerShardedMigrationIT/Users-backfill-logical-shard3.jsonl"),
                    uploadDataStreamFile(
                        jobInfo2,
                        TABLE,
                        "Users-backfill-logical-shard4.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Users-backfill-logical-shard4.jsonl"),
                    uploadDataStreamFile(
                        jobInfo2,
                        TABLE,
                        "Users-cdc-shard2.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Users-cdc-shard2.jsonl")))
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

  @Test
  public void pkReorderedMultiShardMigration() {
    // Migrates Movie table from 4 logical shards. Asserts data from all the shards are going to
    // Spanner. This test case changes the Primary key order from default to (id,
    // migration_shard_id). Checks whether migration shard id column is populated properly based on
    // the transformation context when PK is reordered.
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo1,
                        MOVIE_TABLE,
                        "Movie-shard1.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Movie-shard1.jsonl"),
                    uploadDataStreamFile(
                        jobInfo1,
                        MOVIE_TABLE,
                        "Movie-shard2.jsonl",
                        "DataStreamToSpannerShardedMigrationIT/Movie-shard2.jsonl")))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo1, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    ConditionCheck rowsConditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, MOVIE_TABLE)
            .setMinRows(6)
            .setMaxRows(6)
            .build();
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo1, Duration.ofMinutes(8)), rowsConditionCheck);
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertMovieTableContents();
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "Tester1");
    row.put("age_spanner", 20);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("name", "Tester2");
    row.put("age_spanner", 33);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 10);
    row.put("name", "Tester10");
    row.put("age_spanner", 40);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "Tester1");
    row.put("age_spanner", 21);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 4);
    row.put("name", "Tester4");
    row.put("age_spanner", 33);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 20);
    row.put("name", "Tester20");
    row.put("age_spanner", 50);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 6);
    row.put("name", "Tester6");
    row.put("age_spanner", 22);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("name", "Tester3");
    row.put("age_spanner", 36);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 30);
    row.put("name", "Tester30");
    row.put("age_spanner", 60);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 7);
    row.put("name", "Tester7");
    row.put("age_spanner", 23);
    row.put("migration_shard_id", "L4");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 9);
    row.put("name", "Tester9");
    row.put("age_spanner", 36);
    row.put("migration_shard_id", "L4");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 40);
    row.put("name", "Tester40");
    row.put("age_spanner", 70);
    row.put("migration_shard_id", "L4");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertMovieTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id1", 1);
    row.put("id2", 1);
    row.put("name", "Mov123");
    row.put("actor", 27);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id1", 26);
    row.put("id2", 3);
    row.put("name", "Mov637");
    row.put("actor", 3);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id1", 18);
    row.put("id2", 12);
    row.put("name", "Tester363");
    row.put("actor", 40);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id1", 18);
    row.put("id2", 23);
    row.put("name", "Mov945");
    row.put("actor", 18);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id1", 26);
    row.put("id2", 12);
    row.put("name", "Mov764");
    row.put("actor", 8);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id1", 13);
    row.put("id2", 8);
    row.put("name", "Tester828");
    row.put("actor", 15);
    row.put("migration_shard_id", "L2");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Movie"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
