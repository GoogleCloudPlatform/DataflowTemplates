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
package com.google.cloud.teleport.v2.templates.endtoend;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/**
 * Integration test for end-to-end Testing of all Spanner migration Flex templates for basic run
 * including new spanner tables and column rename use-case.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("This test is disabled currently")
public class BulkForwardAndReverseMigrationShardedEndToEndIT extends EndToEndTestingITBase {
  private static final String SPANNER_DDL_RESOURCE =
      "EndToEndTesting/ShardedMigration/spanner-schema.sql";

  private static final String TABLE = "Authors";
  private static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT" + " NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static final HashSet<BulkForwardAndReverseMigrationShardedEndToEndIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  private static PipelineLauncher.LaunchInfo rrJobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager cloudSqlResourceManagerShardA;
  private static CloudSqlResourceManager cloudSqlResourceManagerShardB;

  private static final Map<String, Object> COLUMNS =
      new HashMap<>() {
        {
          put("name", RandomStringUtils.randomAlphabetic(10));
        }
      };

  private static final Integer NUM_EVENTS = 2;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (BulkForwardAndReverseMigrationShardedEndToEndIT.class) {
      testInstances.add(this);
      if (rrJobInfo == null || fwdJobInfo == null) {
        // create Spanner Resources
        spannerResourceManager =
            createSpannerDatabase(
                BulkForwardAndReverseMigrationShardedEndToEndIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        // Create MySql Resource
        cloudSqlResourceManagerShardA =
            CloudMySQLResourceManager.builder(testName + "ShardA").build();
        cloudSqlResourceManagerShardB =
            CloudMySQLResourceManager.builder(testName + "ShardB").build();
        jdbcSource =
            createMySqlDatabase(
                new ArrayList<>() {
                  {
                    add(cloudSqlResourceManagerShardA);
                    add(cloudSqlResourceManagerShardB);
                  }
                },
                new HashMap<>() {
                  {
                    put(TABLE, AUTHOR_TABLE_COLUMNS);
                  }
                });

        // create and upload GCS Resources
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();

        generateAndUploadSessionFileUsingSMT(
            jdbcSource, cloudSqlResourceManagerShardA, spannerResourceManager, gcsResourceManager);

        Database databaseA =
            new Database(
                cloudSqlResourceManagerShardA.getDatabaseName(),
                cloudSqlResourceManagerShardA.getDatabaseName(),
                "ref1");
        Database databaseB =
            new Database(
                cloudSqlResourceManagerShardB.getDatabaseName(),
                cloudSqlResourceManagerShardB.getDatabaseName(),
                "ref2");

        ArrayList<Database> databases = new ArrayList<>(List.of(databaseA, databaseB));
        DataShard dataShard =
            new DataShard(
                "1",
                jdbcSource.hostname(),
                jdbcSource.username(),
                jdbcSource.password(),
                String.valueOf(jdbcSource.port()),
                "",
                "",
                "",
                databases);
        createAndUploadBulkShardConfigToGcs(
            new ArrayList<>(List.of(dataShard)), gcsResourceManager);

        // create pubsub manager
        pubsubResourceManager = setUpPubSubResourceManager();

        // Write backfill data
        writeRows(TABLE, NUM_EVENTS, COLUMNS, new HashMap<>(), 0, cloudSqlResourceManagerShardA);
        writeRows(TABLE, NUM_EVENTS, COLUMNS, new HashMap<>(), 2, cloudSqlResourceManagerShardB);

        // launch bulk migration template
        bulkJobInfo =
            launchBulkDataflowJob(
                PipelineUtils.createJobName("bulk" + getClass().getSimpleName()),
                spannerResourceManager,
                gcsResourceManager,
                cloudSqlResourceManagerShardA,
                true);

        // launch forward migration template
        fwdJobInfo =
            launchFwdDataflowJob(
                PipelineUtils.createJobName("fwd" + getClass().getSimpleName()),
                spannerResourceManager,
                gcsResourceManager,
                pubsubResourceManager,
                true,
                new HashMap<>() {
                  {
                    put(
                        cloudSqlResourceManagerShardA.getDatabaseName(),
                        cloudSqlResourceManagerShardA.getDatabaseName());
                    put(
                        cloudSqlResourceManagerShardB.getDatabaseName(),
                        cloudSqlResourceManagerShardB.getDatabaseName());
                  }
                },
                false);

        // launch reverse migration template
        createAndUploadReverseMultiShardConfigToGcs(
            gcsResourceManager,
            new HashMap<>() {
              {
                put(cloudSqlResourceManagerShardA.getDatabaseName(), cloudSqlResourceManagerShardA);
                put(cloudSqlResourceManagerShardB.getDatabaseName(), cloudSqlResourceManagerShardB);
              }
            });
        rrJobInfo =
            launchRRDataflowJob(
                PipelineUtils.createJobName("rr" + getClass().getSimpleName()),
                spannerResourceManager,
                gcsResourceManager,
                spannerMetadataResourceManager,
                pubsubResourceManager,
                MYSQL_SOURCE_TYPE);
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
    for (BulkForwardAndReverseMigrationShardedEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cloudSqlResourceManagerShardA,
        cloudSqlResourceManagerShardB,
        datastreamResourceManager);
  }

  @Test
  public void spannerToSourceDbBasic() throws IOException {
    // Bulk Migration pipeline check
    assertThatPipeline(bulkJobInfo).isRunning();

    // Forward Migration pipeline check
    assertThatPipeline(fwdJobInfo).isRunning();

    // Reverse Migration pipeline check
    assertThatPipeline(rrJobInfo).isRunning();

    // Bulk Migration check condition
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));
    assertThatResult(result).isLaunchFinished();
    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(TABLE, "id"))
        .hasRows(4);

    // Forward Migration check condition
    writeRowInMySqlAndAssertRows();

    // Reverse Migration check condition
    writeRowInSpanner();
    assertRowInMySQL();
  }

  private void writeRowInMySqlAndAssertRows() throws IOException {
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(TABLE, 1, COLUMNS, cdcEvents, 8, cloudSqlResourceManagerShardA),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(5)
                        .setMaxRows(5)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(fwdJobInfo, Duration.ofMinutes(8)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  private void writeRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(2)
            .set("name")
            .to("FF")
            .set("migration_shard_id")
            .to(cloudSqlResourceManagerShardA.getDatabaseName())
            .build();
    spannerResourceManager.write(m1);
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(3)
            .set("name")
            .to("B")
            .set("migration_shard_id")
            .to(cloudSqlResourceManagerShardA.getDatabaseName())
            .build();
    spannerResourceManager.write(m2);
  }

  private void assertRowInMySQL() throws IOException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(rrJobInfo, Duration.ofMinutes(10)),
                () -> cloudSqlResourceManagerShardA.getRowCount(TABLE) == 5);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = cloudSqlResourceManagerShardA.readTable(TABLE);
    assertThat(rows).hasSize(5);
    assertThat(rows.get(2).get("id")).isEqualTo(2);
    assertThat(rows.get(2).get("name")).isEqualTo("FF");
  }
}
