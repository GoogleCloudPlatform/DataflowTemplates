/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for basic reverse replication to SQL
 * Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerSourceDbIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSQLServerSourceDbIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSQLServerSourceDbIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "SpannerToSQLServerSourceDbIT/sqlserver-schema.sql";

  private static final String TABLE_USERS = "Users";
  private static final String TABLE_USERS2 = "Users2";
  private static final String TABLE_ALL_DATATYPES = "AllDatatypes";

  private static final HashSet<SpannerToSQLServerSourceDbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MSSQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (SpannerToSQLServerSourceDbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSQLServerSourceDbIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MSSQLResourceManager.builder(testName).build();
        loadSQLFileResource(jdbcResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);

        Map<String, String> jobParameters = new HashMap<>();
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                SOURCE_SQLSERVER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSQLServerSourceDbIT instance : testInstances) {
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
  public void spannerToSQLServerBasic() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowInSpanner();
    assertRowInSQLServer();
  }

  @Test
  public void spannerToSQLServerAllDatatypesCrud() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    // 1. Insert
    writeAllDatatypesRowInSpanner();
    PipelineOperator.Result insertResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount(TABLE_ALL_DATATYPES) == 1);
    assertThatResult(insertResult).meetsConditions();
    assertAllDatatypesRowInSQLServer();

    // 2. Update
    updateAllDatatypesRowInSpanner();
    PipelineOperator.Result updateResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), this::checkAllDatatypesRowAfterUpdate);
    assertThatResult(updateResult).meetsConditions();

    // 3. Delete
    deleteAllDatatypesRowInSpanner();
    PipelineOperator.Result deleteResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount(TABLE_ALL_DATATYPES) == 0);
    assertThatResult(deleteResult).meetsConditions();
  }

  private void writeRowInSpanner() {
    // Write a single record to Users
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(TABLE_USERS)
            .set("id")
            .to(1)
            .set("full_name")
            .to("FF")
            .set("location")
            .to("AA")
            .build();
    spannerResourceManager.write(m1);

    // Write a single record to Users2
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(TABLE_USERS2).set("id").to(2).set("name").to("B").build();
    spannerResourceManager.write(m2);

    // Write a record with txBy= tag which should be skipped by reverse replication
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(PROJECT)
            .withInstanceId(spannerResourceManager.getInstanceId())
            .withDatabaseId(spannerResourceManager.getDatabaseId());
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag("txBy=forwardMigration"),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .run(
            (TransactionCallable<Void>)
                transaction -> {
                  Mutation m3 =
                      Mutation.newInsertOrUpdateBuilder(TABLE_USERS)
                          .set("id")
                          .to(99)
                          .set("full_name")
                          .to("SkippedUser")
                          .build();
                  transaction.buffer(m3);
                  return null;
                });
  }

  private void assertRowInSQLServer() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () ->
                    jdbcResourceManager.getRowCount(TABLE_USERS) == 1
                        && jdbcResourceManager.getRowCount(TABLE_USERS2) == 1);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> usersRows = jdbcResourceManager.readTable(TABLE_USERS);
    assertThat(usersRows).hasSize(1);
    assertThat(usersRows.get(0).get("id").toString()).isEqualTo("1");
    assertThat(usersRows.get(0).get("full_name")).isEqualTo("FF");
    assertThat(usersRows.get(0).get("location")).isEqualTo("AA");

    List<Map<String, Object>> users2Rows = jdbcResourceManager.readTable(TABLE_USERS2);
    assertThat(users2Rows).hasSize(1);
    assertThat(users2Rows.get(0).get("id").toString()).isEqualTo("2");
    assertThat(users2Rows.get(0).get("name")).isEqualTo("B");
  }

  private void writeAllDatatypesRowInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder(TABLE_ALL_DATATYPES)
            .set("id")
            .to(1)
            .set("tinyint_col")
            .to(10)
            .set("smallint_col")
            .to(100)
            .set("int_col")
            .to(1000)
            .set("bigint_col")
            .to(10000L)
            .set("bit_col")
            .to(true)
            .set("numeric_col")
            .to(new BigDecimal("123.45"))
            .set("float_col")
            .to(123.456)
            .set("varchar_col")
            .to("test_string")
            .set("date_col")
            .to(Date.parseDate("2023-01-01"))
            .set("timestamp_col")
            .to(Timestamp.parseTimestampDuration("2023-01-01T12:00:00Z"))
            .build());
    spannerResourceManager.write(mutations);
  }

  private void assertAllDatatypesRowInSQLServer() {
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE_ALL_DATATYPES);
    assertThat(rows).hasSize(1);
    Map<String, Object> row = rows.get(0);
    assertThat(row.get("id").toString()).isEqualTo("1");
    assertThat(row.get("tinyint_col").toString()).isEqualTo("10");
    assertThat(row.get("smallint_col").toString()).isEqualTo("100");
    assertThat(row.get("int_col").toString()).isEqualTo("1000");
    assertThat(row.get("bigint_col").toString()).isEqualTo("10000");
    assertThat(row.get("varchar_col")).isEqualTo("test_string");
  }

  private void updateAllDatatypesRowInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newUpdateBuilder(TABLE_ALL_DATATYPES)
            .set("id")
            .to(1)
            .set("varchar_col")
            .to("updated_string")
            .build());
    spannerResourceManager.write(mutations);
  }

  private boolean checkAllDatatypesRowAfterUpdate() {
    List<Map<String, Object>> rows =
        jdbcResourceManager.runSQLQuery("SELECT * FROM " + TABLE_ALL_DATATYPES + " WHERE id=1");
    if (rows.size() != 1) {
      return false;
    }
    return "updated_string".equals(rows.get(0).get("varchar_col"));
  }

  private void deleteAllDatatypesRowInSpanner() {
    Mutation m = Mutation.delete(TABLE_ALL_DATATYPES, Key.newBuilder().append(1).build());
    spannerResourceManager.write(m);
  }
}
