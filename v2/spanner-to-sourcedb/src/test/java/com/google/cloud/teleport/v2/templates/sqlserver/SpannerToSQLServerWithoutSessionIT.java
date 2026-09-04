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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for basic run including new spanner
 * tables with generated column without session file targeting SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerWithoutSessionIT extends SpannerToSourceDbITBase {
  @Rule public Timeout timeout = new Timeout(25, TimeUnit.MINUTES);

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSQLServerWithoutSessionIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(10);

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerWithoutSessionIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerWithoutSessionIT/sqlserver-schema.sql";

  private static final HashSet<SpannerToSQLServerWithoutSessionIT> testInstances = new HashSet<>();
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
    synchronized (SpannerToSQLServerWithoutSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSQLServerWithoutSessionIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = setUpMSSQLResourceManager(testName);
        createSQLServerSchema(
            jdbcResourceManager, SpannerToSQLServerWithoutSessionIT.SQLSERVER_SCHEMA_FILE_RESOURCE);

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
    for (SpannerToSQLServerWithoutSessionIT instance : testInstances) {
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
  public void spannerToSQLServerGeneratedColumns() {
    LOG.info("Starting Spanner to SQL Server Generated Columns IT without session");
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
