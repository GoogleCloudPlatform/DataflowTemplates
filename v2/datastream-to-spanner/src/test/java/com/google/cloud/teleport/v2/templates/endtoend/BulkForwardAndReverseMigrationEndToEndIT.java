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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
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
public class BulkForwardAndReverseMigrationEndToEndIT extends EndToEndTestingITBase {
  private static final String SPANNER_DDL_RESOURCE =
      "EndToEndTesting/BulkForwardAndReverseSharded/spanner-schema.sql";
  private static String sessionFileResource;
  private static final String SESSION_FILE_RESOURCE =
      "EndToEndTesting/BulkForwardAndReverseSharded/session.json";

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static final String TABLE = "Authors";
  private static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT" + " NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static final HashSet<BulkForwardAndReverseMigrationEndToEndIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo rrJobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
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
    synchronized (BulkForwardAndReverseMigrationEndToEndIT.class) {
      testInstances.add(this);
      // spannerResourceManager = createEmptySpannerDatabase();
      // spannerMetadataResourceManager = createSpannerMetadataDatabase();
      // create Spanner Resources
      spannerResourceManager =
          createSpannerDatabase(BulkForwardAndReverseMigrationEndToEndIT.SPANNER_DDL_RESOURCE);
      spannerMetadataResourceManager = createSpannerMetadataDatabase();

      // create pubsub manager
      pubsubResourceManager = setUpPubSubResourceManager();

      // Create MySql Resource
      cloudSqlResourceManagerShardA =
          CloudMySQLResourceManager.builder(testName + "ShardA").build();
      cloudSqlResourceManagerShardB =
          CloudMySQLResourceManager.builder(testName + "ShardB").build();
      JDBCSource jdbcSourceShardA =
          createMySqlDatabase(
              cloudSqlResourceManagerShardA,
              new HashMap<>() {
                {
                  put(TABLE, AUTHOR_TABLE_COLUMNS);
                }
              });
      JDBCSource jdbcSourceShardB =
          createMySqlDatabase(
              cloudSqlResourceManagerShardB,
              new HashMap<>() {
                {
                  put(TABLE, AUTHOR_TABLE_COLUMNS);
                }
              });

      gcsResourceManager =
          GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
              .build();
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
              "10.94.208.4",
              jdbcSourceShardA.username(),
              jdbcSourceShardA.password(),
              String.valueOf(jdbcSourceShardA.port()),
              "",
              "",
              "",
              databases);
      createAndUploadBulkShardConfigToGcs(new ArrayList<>(List.of(dataShard)), gcsResourceManager);

      // launch datastream
      datastreamResourceManager =
          DatastreamResourceManager.builder(testName, PROJECT, REGION)
              .setCredentialsProvider(credentialsProvider)
              .setPrivateConnectivity("datastream-private-connect-us-central1")
              .build();
      gcsResourceManager.uploadArtifact(
          "input/session.json",
          Resources.getResource(BulkForwardAndReverseMigrationEndToEndIT.SESSION_FILE_RESOURCE)
              .getPath());
      writeRows(TABLE, NUM_EVENTS, COLUMNS, new HashMap<>(), 0, cloudSqlResourceManagerShardA);
      writeRows(TABLE, NUM_EVENTS, COLUMNS, new HashMap<>(), 2, cloudSqlResourceManagerShardB);
      // launch bulk migration template
      bulkJobInfo = launchBulkDataflowJob(spannerResourceManager, gcsResourceManager);
      // launch forward migration template
      fwdJobInfo =
          launchFwdDataflowJob(
              spannerResourceManager,
              gcsResourceManager,
              pubsubResourceManager,
              true,
              new HashMap<>() {
                {
                  put(cloudSqlResourceManagerShardA.getDatabaseName(), "ref1");
                  put(cloudSqlResourceManagerShardB.getDatabaseName(), "ref2");
                }
              },
              false);
      // launch reverse migration template
      createAndUploadReverseMultiShardConfigToGcs(
          gcsResourceManager,
          new HashMap<>() {
            {
              put("ShardA", cloudSqlResourceManagerShardA);
              put("ShardB", cloudSqlResourceManagerShardB);
            }
          });
      rrJobInfo =
          launchRRDataflowJob(
              spannerResourceManager,
              gcsResourceManager,
              spannerMetadataResourceManager,
              pubsubResourceManager,
              MYSQL_SOURCE_TYPE);
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (BulkForwardAndReverseMigrationEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
  }

  @Test
  public void spannerToSourceDbBasic() {
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));
    assertThatResult(result).isLaunchFinished();
    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(TABLE, "id"))
        .hasRows(4);
    // Reverse Migration check condition
    writeRowInSpanner();
    assertRowInMySQL();
  }

  private void writeRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(2)
            .set("name")
            .to("FF")
            .set("migration_shard_id")
            .to("ref1")
            .build();
    spannerResourceManager.write(m1);
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(3)
            .set("name")
            .to("B")
            .set("migration_shard_id")
            .to("ref1")
            .build();
    spannerResourceManager.write(m2);
  }

  private void assertRowInMySQL() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(rrJobInfo, Duration.ofMinutes(10)),
                () -> cloudSqlResourceManagerShardA.getRowCount(TABLE) == 4);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = cloudSqlResourceManagerShardA.readTable(TABLE);
    assertThat(rows).hasSize(4);
    assertThat(rows.get(2).get("id")).isEqualTo(2);
    assertThat(rows.get(2).get("name")).isEqualTo("FF");
  }
}
