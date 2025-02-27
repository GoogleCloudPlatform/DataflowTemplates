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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for basic run including new spanner
 * tables and column rename use-case.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbEndToEndIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbEndToEndIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbEndToEndIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToSourceDbEndToEndIT/session.json";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbEndToEndIT/mysql-schema.sql";

  private static final String TABLE = "Authors";
  private static final HashSet<SpannerToSourceDbEndToEndIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName rrSubscriptionName;
  private SubscriptionName fwdSubscriptionName;
  protected SecretManagerResourceManager secretClient;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbEndToEndIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDbEndToEndIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        secretClient = SecretManagerResourceManager.builder(PROJECT, credentialsProvider).build();
        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager, SpannerToSourceDbEndToEndIT.MYSQL_SCHEMA_FILE_RESOURCE);
        String password =
            secretClient.accessSecret("projects/940149800767/secrets/testing-password/versions/1");
        //JDBCSource mySQLSource = getMySQLSource("35.232.15.141", "root", password);


        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        createAndUploadJarToGcs(gcsResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        // rrSubscriptionName =
        //     createPubsubResources(
        //         getClass().getSimpleName(),
        //         pubsubResourceManager,
        //         getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
        // jobInfo =
        //     launchRRDataflowJob(
        //         spannerResourceManager,
        //         gcsResourceManager,
        //         spannerMetadataResourceManager,
        //         rrSubscriptionName.toString(),
        //         MYSQL_SOURCE_TYPE);
        // System.out.println("######2");
        // System.out.println(jobInfo.jobId());
        fwdJobInfo = launchFwdDataflowJob(
            spannerResourceManager,
            gcsResourceManager,
            pubsubResourceManager,
            "fwdMigration",
            secretClient
        );
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
    for (SpannerToSourceDbEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToSourceDbBasic() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();
    // Assert events on Mysql
    assertRowInMySQL();
    // assertThatPipeline(fwdJobInfo).isRunning();
    // System.out.println("#######1");
    // System.out.println(spannerResourceManager.getInstanceId());
    // System.out.println(spannerResourceManager.getDatabaseId());
    // gcsToSpanner();
  }

  private void gcsToSpanner() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(fwdJobInfo, Duration.ofMinutes(8)), conditionCheck);

    System.out.println("## checking");
    System.out.println(result);
    // Assert Conditions
    assertThatResult(result).meetsConditions();
  }

  private void writeRowInSpanner() {
    // Write a single record to Spanner
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(1).set("name").to("FF").build();
    spannerResourceManager.write(m1);

    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(2).set("name").to("B").build();
    spannerResourceManager.write(m2);

    System.out.println("#######1");
    System.out.println(spannerResourceManager.getInstanceId());
    System.out.println(spannerResourceManager.getDatabaseId());
  }

  private void assertRowInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE) == 2); // only one row is inserted
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("name")).isEqualTo("FF");
  }
}
