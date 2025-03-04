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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
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

  private static final String TABLE = "Authors";
  private static final HashSet<SpannerToSourceDbEndToEndIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName rrSubscriptionName;
  private SubscriptionName fwdSubscriptionName;
  protected SecretManagerResourceManager secretClient;

  private static CloudSqlResourceManager cloudSqlResourceManager;

  private static final List<String> COLUMNS = List.of("id", "name");

  private static final Integer NUM_EVENTS = 2;

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
        cloudSqlResourceManager =
            CloudMySQLResourceManager.builder(testName).build();
        jdbcSource=createMySqlDatabase();
        System.out.println("####"+cloudSqlResourceManager.getDatabaseName());
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, cloudSqlResourceManager);
        createAndUploadJarToGcs(gcsResourceManager);
        gcsResourceManager.createArtifact(
            "input/session.json",
            generateSessionFile(
                cloudSqlResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId()));
        pubsubResourceManager = setUpPubSubResourceManager();
        fwdJobInfo = launchFwdDataflowJob(
            spannerResourceManager,
            gcsResourceManager,
            pubsubResourceManager,
            "fwdMigration",
            secretClient
        );
        rrSubscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
        jobInfo =
            launchRRDataflowJob(
                spannerResourceManager,
                gcsResourceManager,
                spannerMetadataResourceManager,
                rrSubscriptionName.toString(),
                MYSQL_SOURCE_TYPE);
        System.out.println("######2");
        System.out.println(jobInfo.jobId());
        System.out.println(cloudSqlResourceManager.getDatabaseName());
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
        //spannerResourceManager,
        //spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager
        //cloudSqlResourceManager
    );
  }

  @Test
  public void spannerToSourceDbBasic() throws InterruptedException, IOException {
    assertThatPipeline(fwdJobInfo).isRunning();
    System.out.println("#######1");
    System.out.println(spannerResourceManager.getInstanceId());
    System.out.println(spannerResourceManager.getDatabaseId());
    // gcsToSpanner();
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(TABLE, cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(NUM_EVENTS)
                        .setMaxRows(NUM_EVENTS)
                        .build())).build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(fwdJobInfo, Duration.ofMinutes(8)), conditionCheck);

    System.out.println("## checking");
    System.out.println(result);
    // Assert Conditions
    assertThatResult(result).meetsConditions();
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();
    // Assert events on Mysql
    assertRowInMySQL();
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT"+ " NOT NULL");
    columns.put("name", "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, "id");
  }
  private JDBCSource createMySqlDatabase(){
    cloudSqlResourceManager.createTable(
        "Authors", createJdbcSchema());
    return
        MySQLSource.builder(
                cloudSqlResourceManager.getHost(),
                cloudSqlResourceManager.getUsername(),
                cloudSqlResourceManager.getPassword(),
                cloudSqlResourceManager.getPort())
            .setAllowedTables(Map.of(cloudSqlResourceManager.getDatabaseName(), Arrays.asList("Authors")))
            .build();
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
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(2).set("name").to("FF").build();
    spannerResourceManager.write(m1);

    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE).set("id").to(3).set("name").to("B").build();
    spannerResourceManager.write(m2);

    System.out.println("#######1");
    System.out.println(spannerResourceManager.getInstanceId());
    System.out.println(spannerResourceManager.getDatabaseId());
  }

  private void assertRowInMySQL() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> cloudSqlResourceManager.getRowCount(TABLE) == 4);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = cloudSqlResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(4);
    System.out.println(rows);
    assertThat(rows.get(3).get("id")).isEqualTo(2);
    assertThat(rows.get(3).get("name")).isEqualTo("FF");
  }

  private ConditionCheck writeJdbcData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put(COLUMNS.get(0), i);
          values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10));
          rows.add(values);
        }
        cdcEvents.put(tableName, rows);
        success &= cloudSqlResourceManager.write(tableName, rows);
        messages.add(String.format("%d rows to %s", rows.size(), tableName));

        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  private String generateSessionFile(String srcDb, String spannerDb)
      throws IOException {
    String sessionFile =
        Files.readString(
            Paths.get(Resources.getResource(SESSION_FILE_RESOURCE).getPath()));
    return sessionFile
        .replaceAll("SRC_DATABASE", srcDb)
        .replaceAll("SP_DATABASE", spannerDb);
  }
}
