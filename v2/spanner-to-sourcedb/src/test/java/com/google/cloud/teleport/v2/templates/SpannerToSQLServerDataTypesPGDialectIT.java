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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
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
import org.apache.beam.it.jdbc.MSSQLResourceManager;
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
 * Integration test for {@link SpannerToSourceDb} Flex template for SQL Server data type mappings
 * targeting a PostgreSQL-dialect Spanner database.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerDataTypesPGDialectIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSQLServerDataTypesPGDialectIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSQLServerDataTypesPGDialectIT/spanner-schema.sql";

  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "SpannerToSQLServerDataTypesPGDialectIT/sqlserver-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MSSQLResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws Exception {
    spannerResourceManager = setUpPGDialectSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createPGDialectSpannerMetadataDatabase();

    jdbcResourceManager = MSSQLResourceManager.builder(testName).build();
    loadSQLFileResource(jdbcResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);

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
            SOURCE_SQLSERVER,
            jobParameters,
            Dialect.POSTGRESQL);
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
  public void spannerToSQLServerDataTypesPGDialect() {
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Value>> spannerTableData = getSpannerTableData();
    writeRowsInSpanner(spannerTableData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                buildConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    assertRowInSQLServer();
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

  private void assertRowInSQLServer() {
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
        } else if (entry.getValue() instanceof java.sql.Date) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof java.sql.Time) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof Boolean) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof Number) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof String) {
          entry.setValue(((String) entry.getValue()).trim());
        } else {
          entry.setValue(entry.getValue().toString().trim());
        }
      }
    }
    return rows;
  }

  private Map<String, List<Value>> getSpannerTableData() {
    Map<String, List<Value>> spannerRowData = new HashMap<>();
    spannerRowData.put(
        "tinyint_table",
        List.of(Value.int64(0), Value.int64(10), Value.int64(255), Value.int64(null)));
    spannerRowData.put(
        "smallint_table",
        List.of(Value.int64(15), Value.int64(32767), Value.int64(-32768), Value.int64(null)));
    spannerRowData.put(
        "int_table",
        List.of(
            Value.int64(30), Value.int64(2147483647), Value.int64(-2147483648), Value.int64(null)));
    spannerRowData.put(
        "bigint_table",
        List.of(
            Value.int64(40),
            Value.int64(9223372036854775807L),
            Value.int64(-9223372036854775808L),
            Value.int64(null)));
    spannerRowData.put("bit_table", List.of(Value.bool(false), Value.bool(true), Value.bool(null)));
    spannerRowData.put("decimal_table", List.of(Value.pgNumeric("68.75"), Value.pgNumeric(null)));
    spannerRowData.put("numeric_table", List.of(Value.pgNumeric("68.75"), Value.pgNumeric(null)));
    spannerRowData.put(
        "money_table", List.of(Value.pgNumeric("12345.6700"), Value.pgNumeric(null)));
    spannerRowData.put(
        "smallmoney_table", List.of(Value.pgNumeric("123.4500"), Value.pgNumeric(null)));
    spannerRowData.put(
        "float_table",
        List.of(
            Value.float64(52.67),
            Value.float64(1.7E308),
            Value.float64(-1.7E308),
            Value.float64(null)));
    spannerRowData.put(
        "real_table",
        List.of(
            Value.float32(45.56f),
            Value.float32(3.4E38f),
            Value.float32(-3.4E38f),
            Value.float32(null)));
    spannerRowData.put(
        "date_table", List.of(Value.date(Date.parseDate("2012-09-17")), Value.date(null)));
    spannerRowData.put(
        "time_table",
        List.of(Value.string("15:50:00"), Value.string("23:59:59"), Value.string(null)));
    spannerRowData.put(
        "datetime2_table",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "datetimeoffset_table",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "datetime_table",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "smalldatetime_table",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:00Z")),
            Value.timestamp(null)));
    spannerRowData.put("char_table", List.of(Value.string("abc"), Value.string(null)));
    spannerRowData.put(
        "varchar_table",
        List.of(Value.string("abc"), Value.string("x".repeat(100)), Value.string(null)));
    spannerRowData.put("text_table", List.of(Value.string("sample text"), Value.string(null)));
    spannerRowData.put("nchar_table", List.of(Value.string("abc"), Value.string(null)));
    spannerRowData.put(
        "nvarchar_table",
        List.of(Value.string("abc"), Value.string("x".repeat(100)), Value.string(null)));
    spannerRowData.put("ntext_table", List.of(Value.string("sample ntext"), Value.string(null)));
    spannerRowData.put(
        "binary_table", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "varbinary_table", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "image_table", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "uniqueidentifier_table",
        List.of(Value.string("6F9619FF-8B86-D011-B42D-00C04FC964FF"), Value.string(null)));
    spannerRowData.put(
        "xml_table", List.of(Value.string("<root><item>test</item></root>"), Value.string(null)));
    return spannerRowData;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put("tinyint_table", createRows("tinyint_table", 0, 10, 255, null));
    expectedData.put("smallint_table", createRows("smallint_table", 15, 32767, -32768, null));
    expectedData.put("int_table", createRows("int_table", 30, 2147483647, -2147483648, null));
    expectedData.put(
        "bigint_table",
        createRows("bigint_table", 40L, 9223372036854775807L, -9223372036854775808L, null));
    expectedData.put("bit_table", createRows("bit_table", false, true, null));
    expectedData.put("decimal_table", createRows("decimal_table", "68.75", null));
    expectedData.put("numeric_table", createRows("numeric_table", "68.75", null));
    expectedData.put("money_table", createRows("money_table", "12345.6700", null));
    expectedData.put("smallmoney_table", createRows("smallmoney_table", "123.4500", null));
    expectedData.put("float_table", createRows("float_table", 52.67, 1.7E308, -1.7E308, null));
    expectedData.put("real_table", createRows("real_table", 45.56f, 3.4E38f, -3.4E38f, null));
    expectedData.put(
        "date_table", createRows("date_table", java.sql.Date.valueOf("2012-09-17"), null));
    expectedData.put("time_table", createRows("time_table", "15:50:00", "23:59:59", null));
    java.sql.Timestamp ts =
        new java.sql.Timestamp(
            Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z").toSqlTimestamp().getTime());
    expectedData.put("datetime2_table", createRows("datetime2_table", ts, null));
    expectedData.put("datetimeoffset_table", createRows("datetimeoffset_table", ts, null));
    expectedData.put("datetime_table", createRows("datetime_table", ts, null));
    java.sql.Timestamp smallTs =
        new java.sql.Timestamp(
            Timestamp.parseTimestampDuration("2022-08-05T08:23:00Z").toSqlTimestamp().getTime());
    expectedData.put("smalldatetime_table", createRows("smalldatetime_table", smallTs, null));
    expectedData.put("char_table", createRows("char_table", "abc", null));
    expectedData.put("varchar_table", createRows("varchar_table", "abc", "x".repeat(100), null));
    expectedData.put("text_table", createRows("text_table", "sample text", null));
    expectedData.put("nchar_table", createRows("nchar_table", "abc", null));
    expectedData.put("nvarchar_table", createRows("nvarchar_table", "abc", "x".repeat(100), null));
    expectedData.put("ntext_table", createRows("ntext_table", "sample ntext", null));
    expectedData.put(
        "binary_table", createRows("binary_table", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "varbinary_table",
        createRows("varbinary_table", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "image_table", createRows("image_table", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "uniqueidentifier_table",
        createRows("uniqueidentifier_table", "6F9619FF-8B86-D011-B42D-00C04FC964FF", null));
    expectedData.put("xml_table", createRows("xml_table", "<root><item>test</item></root>", null));
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
