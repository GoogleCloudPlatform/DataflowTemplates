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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.apache.beam.it.jdbc.MySQLResourceManager;
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
 * tables with generated column without session file.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToMySqlWithoutSessionIT extends SpannerToSourceDbITBase {
  @Rule public Timeout timeout = new Timeout(25, TimeUnit.MINUTES);

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySqlWithoutSessionIT.class);

  // Test timeout configuration - can be adjusted if tests need more time
  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(10);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToMySqlWithoutSessionIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToMySqlWithoutSessionIT/mysql-schema.sql";

  private static final HashSet<SpannerToMySqlWithoutSessionIT> testInstances = new HashSet<>();
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
    synchronized (SpannerToMySqlWithoutSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToMySqlWithoutSessionIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager, SpannerToMySqlWithoutSessionIT.MYSQL_SCHEMA_FILE_RESOURCE);

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
    for (SpannerToMySqlWithoutSessionIT instance : testInstances) {
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
  public void spannerToMySqlDataTypes() {
    LOG.info("Starting Spanner to MySQL Data Types IT");
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    addInitialMultiColSpannerData(spannerTableData);

    writeRowsInSpanner(spannerTableData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), buildConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    addInitailGeneratedColumnData(expectedData);
    // Assert events on Mysql
    assertRowInMySQL(expectedData);

    // Validating update and delete events.
    Map<String, List<Map<String, Value>>> updateSpannerTableData =
        updateGeneratedColRowsInSpanner();
    spannerTableData.putAll(updateSpannerTableData);
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT), buildConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    expectedData = new HashMap<>();
    addUpdatedGeneratedColumnData(expectedData);
    assertRowInMySQL(expectedData);
  }

  private ConditionCheck buildConditionCheck(
      Map<String, List<Map<String, Value>>> spannerTableData) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Value>>> entry : spannerTableData.entrySet()) {
      String tableName = getTableName(entry.getKey());
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

  private void assertRowInMySQL(Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> expectedTableData : expectedData.entrySet()) {
      String type = expectedTableData.getKey();
      String tableName = getTableName(type);

      List<Map<String, Object>> rawRows;
      if (tableName.equals("time_table")) {
        // JDBC Time objects represent a wall-clock time and not a duration (as MySQL
        // treats them).
        // Need to read them as a string to avoid a DataReadException
        rawRows =
            jdbcResourceManager.runSQLQuery(
                "SELECT id, CAST(time_col as char) as time_col FROM time_table");
      } else {
        rawRows = jdbcResourceManager.readTable(tableName);
      }

      List<Map<String, Object>> rows = cleanValues(rawRows);
      for (Map<String, Object> row : rows) {
        // Limit logs printed for very large strings.
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row: {}", rowString);
      }

      assertThatRecords(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(cleanValues(expectedTableData.getValue()));
    }
  }

  // Replaces `null` values with the string "NULL" and byte arrays with the base64
  // encoding of the
  // bytes
  private List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    for (Map<String, Object> row : rows) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue("NULL");
        } else if (entry.getValue() instanceof byte[]) {
          entry.setValue(Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        }
      }
    }
    return rows;
  }

  private void writeRowsInSpanner(Map<String, List<Map<String, Value>>> spannerTableData) {
    for (Map.Entry<String, List<Map<String, Value>>> tableDataEntry : spannerTableData.entrySet()) {
      String tableName = getTableName(tableDataEntry.getKey());
      List<Map<String, Value>> rows = tableDataEntry.getValue();
      List<Mutation> mutations = new ArrayList<>(rows.size());
      for (Map<String, Value> row : rows) {
        Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(tableName);
        for (Map.Entry<String, Value> entry : row.entrySet()) {
          m.set(getColumnName(entry.getKey())).to(entry.getValue());
        }
        mutations.add(m.build());
      }
      spannerResourceManager.write(mutations);
    }
  }

  private void addInitialMultiColSpannerData(
      Map<String, List<Map<String, Value>>> spannerTableData) {
    spannerTableData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "id", Value.int64(2),
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB"),
                "generated_column", Value.string("AA "),
                "generated_column_pk", Value.string("AA ")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"),
                "generated_column", Value.string("BB "),
                "generated_column_pk", Value.string("BB "))));
  }

  private Map<String, List<Map<String, Value>>> updateGeneratedColRowsInSpanner() {
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    spannerTableData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"),
                "generated_column", Value.string("AA "),
                "generated_column_pk", Value.string("AA "))));

    writeRowsInSpanner(spannerTableData);
    List<Mutation> deleteMutations = new ArrayList<>();
    deleteMutations.add(Mutation.delete("generated_pk_column_table", Key.of("BB ")));
    deleteMutations.add(Mutation.delete("generated_non_pk_column_table", Key.of(2)));
    deleteMutations.add(Mutation.delete("non_generated_to_generated_column_table", Key.of("BB ")));
    deleteMutations.add(Mutation.delete("generated_to_non_generated_column_table", Key.of("BB ")));
    spannerResourceManager.write(deleteMutations);

    return spannerTableData;
  }

  private void addInitailGeneratedColumnData(Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("AA"),
                "last_name_col",
                Value.string("BB"),
                "generated_column_col",
                Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "))));

    expectedData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("BB"),
                "generated_column_col", Value.string("AA ")),
            Map.of(
                "id", Value.int64(2),
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "))));

    expectedData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("BB"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "),
                "generated_column_pk_col", Value.string("BB "))));

    expectedData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("AA"),
                "last_name_col",
                Value.string("BB"),
                "generated_column_col",
                Value.string("AA "),
                "generated_column_pk_col",
                Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "),
                "generated_column_pk_col", Value.string("BB "))));
  }

  private void addUpdatedGeneratedColumnData(Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "))));

    expectedData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "))));

    expectedData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA "))));

    expectedData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA "))));
  }

  private String getTableName(String type) {
    return type + "_table";
  }

  private String getColumnName(String type) {
    if (type.equals("id")) {
      return type;
    }
    return type + "_col";
  }
}
