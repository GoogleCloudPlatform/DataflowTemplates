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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.ORACLE_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
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
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
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
public class SpannerToOracleIT extends SpannerToSourceDbITBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToOracleIT.class);
  // Test timeout configuration - can be adjusted if tests need more time
  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "oracle/SpannerToOracleIT/session.json";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleIT/oracle-schema.sql";
  private static final String TABLE = "Users";
  private static final String TABLE_WITH_VIRTUAL_GEN_COL = "TableWithVirtualGeneratedColumn";
  private static final String TABLE_WITH_STORED_GEN_COL = "TableWithStoredGeneratedColumn";
  private static final String TABLE_WITH_IDENTITY_COL = "TableWithIdentityColumn";
  private static final String BOUNDARY_CHECK_TABLE =
      "testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO";
  private static final HashSet<SpannerToOracleIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static OracleResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    try {
      Class.forName("oracle.jdbc.OracleDriver");
    } catch (Exception e) {
      LOG.warn("Failed to manually register Oracle driver", e);
    }
    skipBaseCleanup = true;
    synchronized (SpannerToOracleIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SpannerToOracleIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        jdbcResourceManager = OracleResourceManager.builder(testName).build();
        createOracleSchema(jdbcResourceManager, SpannerToOracleIT.ORACLE_SCHEMA_FILE_RESOURCE);
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
              }
            };
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
                ORACLE_SOURCE_TYPE,
                jobParameters);
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
    for (SpannerToOracleIT instance : testInstances) {
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
  @Ignore("Skipping spannerToSourceDbBasic test")
  public void spannerToSourceDbBasic() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();
    // Assert events on Oracle
    assertRowInOracle();
  }

  private void writeRowInSpanner() {
    // Write a single record to Spanner
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder("Users")
            .set("id")
            .to(1)
            .set("full_name")
            .to("FF")
            .set("from")
            .to("AA")
            .build();
    spannerResourceManager.write(m1);
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder("Users2").set("id").to(2).set("name").to("B").build();
    spannerResourceManager.write(m2);
    // Write a single record to Spanner for the given logical shard
    // Add the record with the transaction tag as txBy=
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
                      Mutation.newInsertOrUpdateBuilder("Users")
                          .set("id")
                          .to(2)
                          .set("full_name")
                          .to("GG")
                          .build();
                  transaction.buffer(m3);
                  return null;
                });
  }

  private void assertRowInOracle() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () ->
                    jdbcResourceManager.getRowCount("\"" + TABLE + "\"")
                        == 1); // only one row is inserted
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = jdbcResourceManager.readTable("\"" + TABLE + "\"");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("name")).isEqualTo("FF");
    assertThat(rows.get(0).get("from")).isEqualTo("AA");
  }

  @Test
  public void spannerToSourceDbWithGeneratedColumns() {
    assertThatPipeline(jobInfo).isRunning();
    // INSERT
    writeRowsWithGenColInSpanner();
    assertThatPipeline(jobInfo).isRunning();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () ->
                    (jdbcResourceManager.getRowCount("\"" + TABLE_WITH_STORED_GEN_COL + "\"") == 2)
                        && (jdbcResourceManager.getRowCount(
                                "\"" + TABLE_WITH_VIRTUAL_GEN_COL + "\"")
                            == 2)); // only two rows is inserted
    assertGenColRowsInOracleAfterInsert(result);
    updateRowsWithGenColsInSpanner();
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), this::checkGenColRowsInOracleAfterUpdate);
    // Delete rows in spanner.
    deleteGenColRowsInSpanner();
    PipelineOperator.Result deleteResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> allGenColRowsDeleted()); // all rows should be deleted
    assertThatResult(deleteResult).meetsConditions();
  }

  @Test
  public void spannerToOracleSourceDbMaxColAndTableNameTest()
      throws IOException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeMaxColRowsInSpanner();
    // Assert events on Oracle
    assertBoundaryRowInOracle();
  }

  @Test
  public void spannerToSourceDbWithIdentityColumns() {
    assertThatPipeline(jobInfo).isRunning();
    // INSERT
    writeRowsWithIdentityColInSpanner();
    assertThatPipeline(jobInfo).isRunning();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount("\"" + TABLE_WITH_IDENTITY_COL + "\"") == 2);
    assertIdentityColRowsInOracleAfterInsert(result);
  }

  @Test
  public void spannerToOracleGeneratedColumns() {
    LOG.info("Starting Spanner to Oracle Generated Columns IT");
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    OracleGeneratedColumnUtils.addInitialMultiColSpannerData(spannerTableData);
    OracleGeneratedColumnUtils.writeRowsInSpanner(spannerTableData, spannerResourceManager);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                OracleGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    OracleGeneratedColumnUtils.addInitialGeneratedColumnData(expectedData);
    // Assert events on Oracle
    OracleGeneratedColumnUtils.assertRowInOracle(expectedData, jdbcResourceManager);
    // Validating update and delete events.
    Map<String, List<Map<String, Value>>> updateSpannerTableData =
        OracleGeneratedColumnUtils.updateGeneratedColRowsInSpanner(spannerResourceManager);
    spannerTableData.putAll(updateSpannerTableData);
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                OracleGeneratedColumnUtils.buildConditionCheck(
                    spannerTableData, jdbcResourceManager));
    expectedData = new HashMap<>();
    OracleGeneratedColumnUtils.addUpdatedGeneratedColumnData(expectedData);
    OracleGeneratedColumnUtils.assertRowInOracle(expectedData, jdbcResourceManager);
  }

  private void writeMaxColRowsInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CHECK_TABLE).set("id").to(1);
    mutationBuilder
        .set("col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY")
        .to("SampleTestValue");
    mutations.add(mutationBuilder.build());
    spannerResourceManager.write(mutations);
    LOG.info("Inserted row into Spanner using Mutations");
  }

  private void assertBoundaryRowInOracle() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount("\"" + BOUNDARY_CHECK_TABLE + "\"") == 1);
  }

  private void writeRowsWithGenColInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(1)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to(2)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(1)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to(2)
            .build());
    spannerResourceManager.write(mutations);
  }

  private void assertGenColRowsInOracleAfterInsert(PipelineOperator.Result result) {
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows =
        jdbcResourceManager.readTable("\"" + TABLE_WITH_VIRTUAL_GEN_COL + "\"");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("column1")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("virtual_generated_column"))
        .isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("id")).isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("column1")).isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("virtual_generated_column"))
        .isEqualTo(new java.math.BigDecimal("4"));
    rows = jdbcResourceManager.readTable("\"" + TABLE_WITH_STORED_GEN_COL + "\"");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("column1")).isEqualTo(new java.math.BigDecimal("1"));
    assertThat(rows.get(0).get("stored_generated_column")).isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("id")).isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("column1")).isEqualTo(new java.math.BigDecimal("2"));
    assertThat(rows.get(1).get("stored_generated_column")).isEqualTo(new java.math.BigDecimal("4"));
  }

  private void updateRowsWithGenColsInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newUpdateBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(3)
            .build());
    mutations.add(
        Mutation.newUpdateBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(4)
            .build());
    spannerResourceManager.write(mutations);
  }

  private boolean checkGenColRowsInOracleAfterUpdate() {
    List<Map<String, Object>> rows =
        jdbcResourceManager.runSQLQuery(
            "select * from \"TableWithVirtualGeneratedColumn\" where \"id\"=1");
    if (rows.size() != 1) {
      return false;
    }
    if (!rows.get(0).get("id").equals(new java.math.BigDecimal("1"))) {
      return false;
    }
    if (!rows.get(0).get("column1").equals(new java.math.BigDecimal("4"))) {
      return false;
    }
    rows =
        jdbcResourceManager.runSQLQuery(
            "select * from \"TableWithStoredGeneratedColumn\" where \"id\"=1");
    if (rows.size() != 1) {
      return false;
    }
    if (!rows.get(0).get("id").equals(new java.math.BigDecimal("1"))) {
      return false;
    }
    if (!rows.get(0).get("column1").equals(new java.math.BigDecimal("3"))) {
      return false;
    }
    return true;
  }

  private void deleteGenColRowsInSpanner() {
    Mutation m1 = Mutation.delete(TABLE_WITH_VIRTUAL_GEN_COL, Key.newBuilder().append(1).build());
    spannerResourceManager.write(m1);
    Mutation m2 = Mutation.delete(TABLE_WITH_VIRTUAL_GEN_COL, Key.newBuilder().append(2).build());
    spannerResourceManager.write(m2);
    Mutation m3 = Mutation.delete(TABLE_WITH_STORED_GEN_COL, Key.newBuilder().append(1).build());
    spannerResourceManager.write(m3);
    Mutation m4 = Mutation.delete(TABLE_WITH_STORED_GEN_COL, Key.newBuilder().append(2).build());
    spannerResourceManager.write(m4);
  }

  private boolean allGenColRowsDeleted() {
    long rowCountTable1 = jdbcResourceManager.getRowCount("\"" + TABLE_WITH_STORED_GEN_COL + "\"");
    long rowCountTable2 = jdbcResourceManager.getRowCount("\"" + TABLE_WITH_VIRTUAL_GEN_COL + "\"");
    return (rowCountTable1 == 0) && (rowCountTable2 == 0);
  }

  private void writeRowsWithIdentityColInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_IDENTITY_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to("id1")
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_IDENTITY_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to("id2")
            .build());
    spannerResourceManager.write(mutations);
  }

  private void assertIdentityColRowsInOracleAfterInsert(PipelineOperator.Result result) {
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows =
        jdbcResourceManager.readTable("\"" + TABLE_WITH_IDENTITY_COL + "\"");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id").toString()).isEqualTo("1");
    assertThat(rows.get(0).get("column1")).isEqualTo("id1");
    assertThat(rows.get(1).get("id").toString()).isEqualTo("2");
    assertThat(rows.get(1).get("column1")).isEqualTo("id2");
  }
}
