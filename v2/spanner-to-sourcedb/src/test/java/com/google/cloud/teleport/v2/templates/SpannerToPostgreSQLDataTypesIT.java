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

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_POSTGRESQL;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for PostgreSQL data type mappings.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToPostgreSQLDataTypesIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToPostgreSQLDataTypesIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToPostgreSQLDataTypesIT/spanner-schema.sql";

  private static final String POSTGRES_SCHEMA_FILE_RESOURCE =
      "SpannerToPostgreSQLDataTypesIT/postgresql-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static PostgresResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws Exception {
    spannerResourceManager =
        createSpannerDatabase(SpannerToPostgreSQLDataTypesIT.SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    jdbcResourceManager = PostgresResourceManager.builder("rr-pg-" + testName).build();
    loadSQLFileResource(jdbcResourceManager, POSTGRES_SCHEMA_FILE_RESOURCE);

    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

    pubsubResourceManager = setUpPubSubResourceManager();
    SubscriptionName subscriptionName =
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
            SOURCE_POSTGRESQL,
            jobParameters);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToPostgreSQLDataTypes() {
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Value>> spannerTableData = getSpannerTableData();
    writeRowsInSpanner(spannerTableData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                buildConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    assertRowInPostgreSQL();
  }

  private void writeRowsInSpanner(Map<String, List<Value>> spannerTableData) {
    for (Map.Entry<String, List<Value>> tableDataEntry : spannerTableData.entrySet()) {
      String tableName = tableDataEntry.getKey();
      String columnName = tableName.replace("_table", "_col");
      List<Value> vals = tableDataEntry.getValue();
      List<Mutation> mutations = new ArrayList<>(vals.size());
      for (int i = 0; i < vals.size(); i++) {
        Mutation m =
            Mutation.newInsertOrUpdateBuilder(tableName)
                .set("id")
                .to(i + 1)
                .set(columnName)
                .to(vals.get(i))
                .build();
        mutations.add(m);
      }
      spannerResourceManager.write(mutations);
    }
  }

  private ConditionCheck buildConditionCheck(Map<String, List<Value>> spannerTableData) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Value>> entry : spannerTableData.entrySet()) {
      String tableName = entry.getKey();
      int numRows = entry.getValue().size();
      ConditionCheck c =
          new ConditionCheck() {
            @Override
            protected @UnknownKeyFor @NonNull @Initialized String getDescription() {
              return "Checking num rows in table " + tableName;
            }

            @Override
            protected @UnknownKeyFor @NonNull @Initialized CheckResult check() {
              return new CheckResult(jdbcResourceManager.getRowCount(tableName) >= numRows);
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

  private void assertRowInPostgreSQL() {
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> expectedTableData : expectedData.entrySet()) {
      String tableName = expectedTableData.getKey();
      List<Map<String, Object>> rawRows = jdbcResourceManager.readTable(tableName);
      List<Map<String, Object>> rows = cleanValues(rawRows);

      assertThatRecords(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(cleanValues(expectedTableData.getValue()));
    }
  }

  private List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    for (Map<String, Object> row : rows) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue("NULL");
        } else if (entry.getValue() instanceof byte[]) {
          entry.setValue(Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        } else if (entry.getValue() instanceof java.sql.Timestamp) {
          entry.setValue(entry.getValue().toString());
        }
      }
    }
    return rows;
  }

  private Map<String, List<Value>> getSpannerTableData() {
    Map<String, List<Value>> spannerRowData = new HashMap<>();
    spannerRowData.put(
        "bool_table", List.of(Value.bool(false), Value.bool(true), Value.bool(null)));
    spannerRowData.put(
        "int64_table",
        List.of(
            Value.int64(30), Value.int64(2147483647), Value.int64(-2147483648), Value.int64(null)));
    spannerRowData.put(
        "float64_table",
        List.of(
            Value.float64(52.67),
            Value.float64(1.7E308),
            Value.float64(-1.7E308),
            Value.float64(null)));
    spannerRowData.put(
        "string_table",
        List.of(Value.string("abc"), Value.string("x".repeat(100)), Value.string(null)));
    spannerRowData.put(
        "bytes_table", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "date_table", List.of(Value.date(Date.parseDate("2012-09-17")), Value.date(null)));
    spannerRowData.put(
        "numeric_table", List.of(Value.numeric(new BigDecimal("68.75")), Value.numeric(null)));
    spannerRowData.put(
        "timestamp_table",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    return spannerRowData;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put("bool_table", createRows("bool_table", false, true, null));
    expectedData.put(
        "int64_table", createRows("int64_table", 30L, 2147483647L, -2147483648L, null));
    expectedData.put("float64_table", createRows("float64_table", 52.67, 1.7E308, -1.7E308, null));
    expectedData.put("string_table", createRows("string_table", "abc", "x".repeat(100), null));
    expectedData.put(
        "bytes_table", createRows("bytes_table", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "date_table", createRows("date_table", java.sql.Date.valueOf("2012-09-17"), null));
    expectedData.put("numeric_table", createRows("numeric_table", new BigDecimal("68.75"), null));
    java.sql.Timestamp ts =
        new java.sql.Timestamp(
            Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z").toSqlTimestamp().getTime());
    expectedData.put("timestamp_table", createRows("timestamp_table", ts, null));
    return expectedData;
  }

  private List<Map<String, Object>> createRows(String tableName, Object... values) {
    List<Object> vals = java.util.Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>(vals.size());
    String columnName = tableName.replace("_table", "_col");
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>(2, 1.0f);
      row.put("id", (long) (i + 1));
      row.put(columnName, vals.get(i));
      rows.add(row);
    }
    return rows;
  }
}
