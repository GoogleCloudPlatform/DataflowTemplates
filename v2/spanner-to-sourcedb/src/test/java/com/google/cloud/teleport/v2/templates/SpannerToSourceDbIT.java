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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.apache.beam.it.jdbc.MySQLResourceManager;
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
 * Integration test for {@link SpannerToSourceDb} Flex template for basic run including new spanner
 * tables and column rename use-case.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbIT.class);

  private static final String SPANNER_DDL_RESOURCE = "SpannerToSourceDbIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToSourceDbIT/session.json";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE = "SpannerToSourceDbIT/mysql-schema.sql";

  private static final String TABLE = "Users";
  private static final String TABLE_WITH_VIRTUAL_GEN_COL = "TableWithVirtualGeneratedColumn";
  private static final String TABLE_WITH_STORED_GEN_COL = "TableWithStoredGeneratedColumn";
  private static final String BOUNDARY_CHECK_TABLE =
      "testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO";
  private static final HashSet<SpannerToSourceDbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManager;
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
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SpannerToSourceDbIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(jdbcResourceManager, SpannerToSourceDbIT.MYSQL_SCHEMA_FILE_RESOURCE);

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
                MYSQL_SOURCE_TYPE,
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
    for (SpannerToSourceDbIT instance : testInstances) {
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

  private void assertRowInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE) == 1); // only one row is inserted
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
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
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    (jdbcResourceManager.getRowCount(TABLE_WITH_STORED_GEN_COL) == 2)
                        && (jdbcResourceManager.getRowCount(TABLE_WITH_VIRTUAL_GEN_COL)
                            == 2)); // only two rows is inserted
    assertThatResult(result).meetsConditions();
    assertGenColRowsInMySQLAfterInsert();

    updateRowsWithGenColsInSpanner();
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                this::checkGenColRowsInMySQLAfterUpdate);
    assertThatResult(result).meetsConditions();

    // Delete rows in spanner.
    deleteGenColRowsInSpanner();

    PipelineOperator.Result deleteResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> allGenColRowsDeleted()); // all rows should be deleted
    assertThatResult(deleteResult).meetsConditions();
  }

  @Test
  public void spannerToMySQLSourceDbMaxColAndTableNameTest()
      throws IOException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeMaxColRowsInSpanner();
    // Assert events on Mysql
    assertBoundaryRowInMySQL();
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

  private void assertBoundaryRowInMySQL() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(BOUNDARY_CHECK_TABLE) == 1);
    assertThatResult(result).meetsConditions();
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

  private void assertGenColRowsInMySQLAfterInsert() {
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE_WITH_VIRTUAL_GEN_COL);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("column1")).isEqualTo(1);
    assertThat(rows.get(0).get("virtual_generated_column")).isEqualTo(2);
    assertThat(rows.get(1).get("id")).isEqualTo(2);
    assertThat(rows.get(1).get("column1")).isEqualTo(2);
    assertThat(rows.get(1).get("virtual_generated_column")).isEqualTo(4);

    rows = jdbcResourceManager.readTable(TABLE_WITH_STORED_GEN_COL);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("column1")).isEqualTo(1);
    assertThat(rows.get(0).get("stored_generated_column")).isEqualTo(2);
    assertThat(rows.get(1).get("id")).isEqualTo(2);
    assertThat(rows.get(1).get("column1")).isEqualTo(2);
    assertThat(rows.get(1).get("stored_generated_column")).isEqualTo(4);
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

  private boolean checkGenColRowsInMySQLAfterUpdate() {
    List<Map<String, Object>> rows =
        jdbcResourceManager.runSQLQuery("select * from TableWithVirtualGeneratedColumn where id=1");
    if (rows.size() != 1) {
      return false;
    }
    if (!rows.get(0).get("id").equals(1)) {
      return false;
    }
    if (!rows.get(0).get("column1").equals(4)) {
      return false;
    }

    rows =
        jdbcResourceManager.runSQLQuery("select * from TableWithStoredGeneratedColumn where id=1");
    if (rows.size() != 1) {
      return false;
    }
    if (!rows.get(0).get("id").equals(1)) {
      return false;
    }
    if (!rows.get(0).get("column1").equals(3)) {
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
    long rowCountTable1 = jdbcResourceManager.getRowCount(TABLE_WITH_STORED_GEN_COL);
    long rowCountTable2 = jdbcResourceManager.getRowCount(TABLE_WITH_VIRTUAL_GEN_COL);
    return (rowCountTable1 == 0) && (rowCountTable2 == 0);
  }
}
