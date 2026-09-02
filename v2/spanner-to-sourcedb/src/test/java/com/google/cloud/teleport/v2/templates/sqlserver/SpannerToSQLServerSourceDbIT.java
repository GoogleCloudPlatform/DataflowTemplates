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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for basic run targeting SQL Server
 * including new spanner tables and column rename use-cases.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerSourceDbIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSQLServerSourceDbIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerSourceDbIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerSourceDbIT/session.json";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerSourceDbIT/sqlserver-schema.sql";

  private static final String TABLE = "Users";
  private static final String TABLE_WITH_VIRTUAL_GEN_COL = "TableWithVirtualGeneratedColumn";
  private static final String TABLE_WITH_STORED_GEN_COL = "TableWithStoredGeneratedColumn";
  private static final String TABLE_WITH_IDENTITY_COL = "TableWithIdentityColumn";
  private static final String BOUNDARY_CHECK_TABLE =
      "testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO";
  private static final HashSet<SpannerToSQLServerSourceDbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MSSQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSQLServerSourceDbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSQLServerSourceDbIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = setUpMSSQLResourceManager(testName);
        createSQLServerSchema(
            jdbcResourceManager, SpannerToSQLServerSourceDbIT.SQLSERVER_SCHEMA_FILE_RESOURCE);

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
  public void spannerToSQLServerBasic() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowInSpanner();
    assertRowInSQLServer();
  }

  private void writeRowInSpanner() {
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

  private void assertRowInSQLServer() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount(TABLE) == 1);
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("name")).isEqualTo("FF");
    assertThat(rows.get(0).get("from")).isEqualTo("AA");
  }

  @Test
  public void spannerToSQLServerWithGeneratedColumns() {
    assertThatPipeline(jobInfo).isRunning();
    writeRowsWithGenColInSpanner();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () ->
                    (jdbcResourceManager.getRowCount(TABLE_WITH_STORED_GEN_COL) == 2)
                        && (jdbcResourceManager.getRowCount(TABLE_WITH_VIRTUAL_GEN_COL) == 2));
    assertThatResult(result).meetsConditions();
    assertGenColRowsInSQLServerAfterInsert();

    updateRowsWithGenColsInSpanner();
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), this::checkGenColRowsInSQLServerAfterUpdate);
    assertThatResult(result).meetsConditions();

    deleteGenColRowsInSpanner();
    PipelineOperator.Result deleteResult =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, TEST_TIMEOUT), this::allGenColRowsDeleted);
    assertThatResult(deleteResult).meetsConditions();
  }

  @Test
  public void spannerToSQLServerMaxColAndTableNameTest() throws IOException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    writeMaxColRowsInSpanner();
    assertBoundaryRowInSQLServer();
  }

  @Test
  public void spannerToSQLServerWithIdentityColumns() {
    assertThatPipeline(jobInfo).isRunning();
    writeRowsWithIdentityColInSpanner();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> jdbcResourceManager.getRowCount(TABLE_WITH_IDENTITY_COL) == 2);
    assertThatResult(result).meetsConditions();
    assertIdentityColRowsInSQLServerAfterInsert();
  }

  @Test
  public void spannerToSQLServerGeneratedColumns() {
    LOG.info("Starting Spanner to SQL Server Generated Columns IT");
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    addInitialMultiColSpannerData(spannerTableData);

    writeGenColRowsInSpanner(spannerTableData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), buildGenColConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    addInitialGeneratedColumnData(expectedData);
    assertGenColRowsInSQLServer(expectedData);

    Map<String, List<Map<String, Value>>> updateSpannerTableData =
        updateGeneratedColRowsInSpanner();
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                buildGenColConditionCheck(updateSpannerTableData));
    assertThatResult(result).meetsConditions();

    expectedData = new HashMap<>();
    addUpdatedGeneratedColumnData(expectedData);
    assertGenColRowsInSQLServer(expectedData);
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

  private void assertBoundaryRowInSQLServer() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
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

  private void assertGenColRowsInSQLServerAfterInsert() {
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

  private boolean checkGenColRowsInSQLServerAfterUpdate() {
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

  private void assertIdentityColRowsInSQLServerAfterInsert() {
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE_WITH_IDENTITY_COL);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(1L);
    assertThat(rows.get(0).get("column1")).isEqualTo("id1");
    assertThat(rows.get(1).get("id")).isEqualTo(2L);
    assertThat(rows.get(1).get("column1")).isEqualTo("id2");
  }

  private void addInitialMultiColSpannerData(
      Map<String, List<Map<String, Value>>> spannerTableData) {
    spannerTableData.put(
        "generated_pk_column_table",
        List.of(
            Map.of("first_name_col", Value.string("a")),
            Map.of("first_name_col", Value.string("b"))));
    spannerTableData.put(
        "generated_non_pk_column_table",
        List.of(
            Map.of("first_name_col", Value.string("a"), "id", Value.int64(1)),
            Map.of("first_name_col", Value.string("b"), "id", Value.int64(2))));
    spannerTableData.put(
        "non_generated_to_generated_column_table",
        List.of(
            Map.of("first_name_col", Value.string("a")),
            Map.of("first_name_col", Value.string("b"))));
    spannerTableData.put(
        "generated_to_non_generated_column_table",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("a"),
                "generated_column_col",
                Value.string("a "),
                "generated_column_pk_col",
                Value.string("a ")),
            Map.of(
                "first_name_col",
                Value.string("b"),
                "generated_column_col",
                Value.string("b "),
                "generated_column_pk_col",
                Value.string("b "))));
  }

  private void writeGenColRowsInSpanner(Map<String, List<Map<String, Value>>> spannerTableData) {
    for (Map.Entry<String, List<Map<String, Value>>> tableDataEntry : spannerTableData.entrySet()) {
      String tableName = tableDataEntry.getKey();
      List<Map<String, Value>> rows = tableDataEntry.getValue();
      List<Mutation> mutations = new ArrayList<>(rows.size());
      for (Map<String, Value> row : rows) {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
        for (Map.Entry<String, Value> col : row.entrySet()) {
          builder.set(col.getKey()).to(col.getValue());
        }
        mutations.add(builder.build());
      }
      spannerResourceManager.write(mutations);
    }
  }

  private ConditionCheck buildGenColConditionCheck(
      Map<String, List<Map<String, Value>>> spannerTableData) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Value>>> entry : spannerTableData.entrySet()) {
      String tableName = entry.getKey();
      int numRows = entry.getValue().size();
      ConditionCheck c =
          new ConditionCheck() {
            @Override
            protected @UnknownKeyFor @NonNull @Initialized String getDescription() {
              return "Checking num rows in table " + tableName + " with " + numRows + " rows";
            }

            @Override
            protected @UnknownKeyFor @NonNull @Initialized CheckResult check() {
              return new CheckResult(
                  jdbcResourceManager.getRowCount(tableName) == numRows, getDescription());
            }
          };
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }
    return combinedCondition;
  }

  private void addInitialGeneratedColumnData(Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column_table",
        List.of(
            Map.of("first_name_col", "a", "last_name_col", "NULL", "generated_column_col", "a "),
            Map.of("first_name_col", "b", "last_name_col", "NULL", "generated_column_col", "b ")));
    expectedData.put(
        "generated_non_pk_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "a",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "a ",
                "id",
                1),
            Map.of(
                "first_name_col",
                "b",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "b ",
                "id",
                2)));
    expectedData.put(
        "non_generated_to_generated_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "a",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "a ",
                "generated_column_pk_col",
                "a "),
            Map.of(
                "first_name_col",
                "b",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "b ",
                "generated_column_pk_col",
                "b ")));
    expectedData.put(
        "generated_to_non_generated_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "a",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "a ",
                "generated_column_pk_col",
                "a "),
            Map.of(
                "first_name_col",
                "b",
                "last_name_col",
                "NULL",
                "generated_column_col",
                "b ",
                "generated_column_pk_col",
                "b ")));
  }

  private Map<String, List<Map<String, Value>>> updateGeneratedColRowsInSpanner() {
    Map<String, List<Map<String, Value>>> updateSpannerTableData = new HashMap<>();
    updateSpannerTableData.put(
        "generated_pk_column_table",
        List.of(Map.of("first_name_col", Value.string("a"), "last_name_col", Value.string("c"))));
    updateSpannerTableData.put(
        "generated_non_pk_column_table",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("c"),
                "last_name_col",
                Value.string("d"),
                "id",
                Value.int64(1))));
    updateSpannerTableData.put(
        "non_generated_to_generated_column_table",
        List.of(Map.of("last_name_col", Value.string("c"), "first_name_col", Value.string("a"))));
    updateSpannerTableData.put(
        "generated_to_non_generated_column_table",
        List.of(
            Map.of(
                "last_name_col",
                Value.string("c"),
                "first_name_col",
                Value.string("a"),
                "generated_column_col",
                Value.string("a "),
                "generated_column_pk_col",
                Value.string("a "))));

    writeGenColRowsInSpanner(updateSpannerTableData);

    List<Mutation> deleteMutations = new ArrayList<>();
    deleteMutations.add(Mutation.delete("generated_pk_column_table", Key.of("b ")));
    deleteMutations.add(Mutation.delete("generated_non_pk_column_table", Key.of(2)));
    deleteMutations.add(Mutation.delete("non_generated_to_generated_column_table", Key.of("b ")));
    deleteMutations.add(Mutation.delete("generated_to_non_generated_column_table", Key.of("b ")));
    spannerResourceManager.write(deleteMutations);

    return updateSpannerTableData;
  }

  private void addUpdatedGeneratedColumnData(Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column_table",
        List.of(Map.of("first_name_col", "a", "last_name_col", "c", "generated_column_col", "a ")));
    expectedData.put(
        "generated_non_pk_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "c",
                "last_name_col",
                "d",
                "generated_column_col",
                "c ",
                "id",
                1)));
    expectedData.put(
        "non_generated_to_generated_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "a",
                "last_name_col",
                "c",
                "generated_column_col",
                "a ",
                "generated_column_pk_col",
                "a ")));
    expectedData.put(
        "generated_to_non_generated_column_table",
        List.of(
            Map.of(
                "first_name_col",
                "a",
                "last_name_col",
                "c",
                "generated_column_col",
                "a ",
                "generated_column_pk_col",
                "a ")));
  }

  private void assertGenColRowsInSQLServer(Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> expectedTableData : expectedData.entrySet()) {
      String tableName = expectedTableData.getKey();
      List<Map<String, Object>> rows = cleanValues(jdbcResourceManager.readTable(tableName));
      assertThatRecords(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(cleanValues(expectedTableData.getValue()));
    }
  }

  private List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      Map<String, Object> cleanedRow = new HashMap<>();
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          cleanedRow.put(entry.getKey(), "NULL");
        } else if (entry.getValue() instanceof byte[]) {
          cleanedRow.put(
              entry.getKey(), Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        } else {
          cleanedRow.put(entry.getKey(), entry.getValue());
        }
      }
      result.add(cleanedRow);
    }
    return result;
  }
}
