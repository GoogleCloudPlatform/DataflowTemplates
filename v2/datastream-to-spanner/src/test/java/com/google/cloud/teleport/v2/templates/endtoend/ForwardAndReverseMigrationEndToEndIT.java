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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
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
public class ForwardAndReverseMigrationEndToEndIT extends EndToEndTestingITBase {
  private static final String SPANNER_DDL_RESOURCE = "EndToEndTesting/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "EndToEndTesting/session.json";

  private static final String TABLE = "Authors";
  private static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT" + " NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static final HashSet<ForwardAndReverseMigrationEndToEndIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo rrJobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  protected SecretManagerResourceManager secretClient;

  private static CloudSqlResourceManager cloudSqlResourceManager;

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
    synchronized (ForwardAndReverseMigrationEndToEndIT.class) {
      testInstances.add(this);
      if (rrJobInfo == null || fwdJobInfo == null) {
        // create Spanner Resources
        spannerResourceManager =
            createSpannerDatabase(ForwardAndReverseMigrationEndToEndIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        // fetch secrets
        secretClient = SecretManagerResourceManager.builder(PROJECT, credentialsProvider).build();
        String privateHost = "10.33.0.4";
        // secretClient.accessSecret(
        //     "projects/269744978479/secrets/end-to-end-private-ip/versions/1");
        String publicHost = "34.170.126.170";
        // secretClient.accessSecret(
        //     "projects/269744978479/secrets/end-to-end-public-ip/versions/1");
        String username = "user2";
        // secretClient.accessSecret("projects/269744978479/secrets/end-to-end-user/versions/1");
        String password = "password";
        // secretClient.accessSecret(
        //     "projects/269744978479/secrets/end-to-end-password/versions/1");
        String network = "end-to-end";
        // secretClient.accessSecret("projects/269744978479/secrets/network-mysql/versions/1");
        String subnetwork = "regions/us-central1/subnetworks/e2e";
        // secretClient.accessSecret("projects/269744978479/secrets/subnetwork-mysql/versions/1");
        // create MySql Resources
        cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();
        jdbcSource =
            createMySqlDatabase(
                cloudSqlResourceManager,
                new HashMap<>() {
                  {
                    put(TABLE, AUTHOR_TABLE_COLUMNS);
                  }
                });

        // create and upload GCS Resources
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(
            gcsResourceManager, cloudSqlResourceManager, cloudSqlResourceManager.getHost());
        createAndUploadJarToGcs(gcsResourceManager);
        gcsResourceManager.createArtifact(
            "input/session.json",
            generateSessionFile(
                cloudSqlResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId(),
                SESSION_FILE_RESOURCE));

        // create pubsub manager
        pubsubResourceManager = setUpPubSubResourceManager();

        // launch forward migration template
        fwdJobInfo =
            launchFwdDataflowJob(spannerResourceManager, gcsResourceManager, pubsubResourceManager);

        // launch reverse migration template
        rrJobInfo =
            launchRRDataflowJob(
                spannerResourceManager,
                gcsResourceManager,
                spannerMetadataResourceManager,
                pubsubResourceManager,
                MYSQL_SOURCE_TYPE,
                network,
                subnetwork);
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
    for (ForwardAndReverseMigrationEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cloudSqlResourceManager);
  }

  @Test
  public void spannerToSourceDbBasic() {
    // Forward Migration check condition
    assertThatPipeline(fwdJobInfo).isRunning();
    writeRowInMySqlAndAssertRows();

    // Reverse Migration check condition
    assertThatPipeline(rrJobInfo).isRunning();
    writeRowInSpanner();
    assertRowInMySQL();
  }

  private void writeRowInMySqlAndAssertRows() {
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(TABLE, NUM_EVENTS, COLUMNS, cdcEvents, cloudSqlResourceManager),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(NUM_EVENTS)
                        .setMaxRows(NUM_EVENTS)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(fwdJobInfo, Duration.ofMinutes(8)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  private void writeRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(2).set("name").to("FF").build();
    spannerResourceManager.write(m1);
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(3).set("name").to("B").build();
    spannerResourceManager.write(m2);
  }

  private void assertRowInMySQL() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(rrJobInfo, Duration.ofMinutes(10)),
                () -> cloudSqlResourceManager.getRowCount(TABLE) == 4);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = cloudSqlResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(4);
    assertThat(rows.get(2).get("id")).isEqualTo(2);
    assertThat(rows.get(2).get("name")).isEqualTo("FF");
  }
}
