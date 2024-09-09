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
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sharded data migration Integration test with addition of migration_shard_id column in the schema
 * for each table in the {@link DataStreamToSpanner} Flex template and with single dataflow job per
 * migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DatastreamToSpannerSingleDFShardedMigrationIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerSingleDFShardedMigrationIT.class);

  private static final String TABLE = "Users";

  private static final String SESSION_FILE_RESOURCE =
      "DatastreamToSpannerSingleDFShardedMigrationIT/mysql-session.json";

  private static final String SHARDING_CONTEXT_RESOURCE =
      "DatastreamToSpannerSingleDFShardedMigrationIT/sharding-context.json";

  private static final String SPANNER_DDL_RESOURCE =
      "DatastreamToSpannerSingleDFShardedMigrationIT/spanner-schema.sql";

  private static HashSet<DatastreamToSpannerSingleDFShardedMigrationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerShardedMigrationWithMigrationShardIdColumnIT.class) {
      testInstances.add(this);
      if (spannerResourceManager == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
      }
      if (pubsubResourceManager == null) {
        pubsubResourceManager = setUpPubSubResourceManager();
      }
      createAndUploadJarToGcs("shard1");
      if (jobInfo == null) {
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                SESSION_FILE_RESOURCE,
                null,
                "shard1",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "avro");
                  }
                },
                null,
                SHARDING_CONTEXT_RESOURCE);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  //  @AfterClass
  //  public static void cleanUp() throws IOException {
  //    for (DatastreamToSpannerSingleDFShardedMigrationIT instance : testInstances) {
  //      instance.tearDownBase();
  //    }
  //    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  //  }

  @Test
  public void multiShardMigration() {
    // Two dataflow jobs are running corresponding to two physical shards containing two logical
    // shards each. Migrates Users table from 4 logical shards. Asserts data from all the shards are
    // going to Spanner. Checks whether migration shard id column is populated properly based on the
    // transformation context.

    // Currently, we have a conditional check on spanner row count to validate if
    // desired number of rows are present in spanner, if yes, we proceed with
    // assertions.
    // In test cases with cdc events where cdc file might have equal number
    // of inserts and deletes resulting in spanner count after cdc same as spanner
    // count before cdc can result in a situation where condition check passes
    // because spanner counts match but the test cases later fail during assertion which can be a
    // possible source of flakiness.
    // In order to ensure that such situation doesn't occur we need to validate
    // actual row data rather than comparing counts and enhance implement a
    // SpannerRowMatcher.
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "Users-backfill-logical-shard1.avro",
                        "DatastreamToSpannerSingleDFShardedMigrationIT/Users-backfill-logical-shard1.avro"),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "Users-backfill-logical-shard2.avro",
                        "DatastreamToSpannerSingleDFShardedMigrationIT/Users-backfill-logical-shard2.avro"),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "Users-cdc-logical-shard1.avro",
                        "DatastreamToSpannerSingleDFShardedMigrationIT/Users-cdc-logical-shard1.avro"),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "Users-backfill-logical-shard3.avro",
                        "DatastreamToSpannerSingleDFShardedMigrationIT/Users-backfill-logical-shard3.avro"),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE,
                        "Users-backfill-logical-shard4.avro",
                        "DatastreamToSpannerSingleDFShardedMigrationIT/Users-backfill-logical-shard4.avro")))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    ConditionCheck rowsConditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, TABLE)
            .setMinRows(12)
            .setMaxRows(12)
            .build();
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), rowsConditionCheck);
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertUsersTableContents();
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
    row.put("id", 3);
    row.put("name", "Tester3");
    row.put("age_spanner", 103);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 13);
    row.put("name", "Tester13");
    row.put("age_spanner", 113);
    row.put("migration_shard_id", "L1");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 4);
    row.put("name", "Tester4");
    row.put("age_spanner", 104);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 5);
    row.put("name", "Tester5");
    row.put("age_spanner", 105);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 6);
    row.put("name", "Tester6");
    row.put("age_spanner", 106);
    row.put("migration_shard_id", "L2");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 7);
    row.put("name", "Tester7");
    row.put("age_spanner", 107);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 8);
    row.put("name", "Tester8");
    row.put("age_spanner", 108);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 9);
    row.put("name", "Tester9");
    row.put("age_spanner", 109);
    row.put("migration_shard_id", "L3");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 10);
    row.put("name", "Tester10");
    row.put("age_spanner", 110);
    row.put("migration_shard_id", "L4");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 11);
    row.put("name", "Tester11");
    row.put("age_spanner", 111);
    row.put("migration_shard_id", "L4");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 12);
    row.put("name", "Tester12");
    row.put("age_spanner", 112);
    row.put("migration_shard_id", "L4");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
