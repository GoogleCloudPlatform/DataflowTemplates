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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

/** Integration test for {@link SpannerToSourceDb} Flex template for SQL Server data types. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerDataTypesIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSQLServerDataTypesIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerDataTypesIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerDataTypesIT/sqlserver-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MSSQLResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    jdbcResourceManager = setUpMSSQLResourceManager(testName);
    createSQLServerSchema(jdbcResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);

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
  public void spannerToSQLServerDataTypes() {
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
      String type = tableDataEntry.getKey();
      String tableName = getTableName(type);
      String columnName = getColumnName(type);
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
      String tableName = getTableName(entry.getKey());
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
      String type = expectedTableData.getKey();
      String tableName = getTableName(type);

      List<Map<String, Object>> rawRows = jdbcResourceManager.readTable(tableName);
      List<Map<String, Object>> rows = cleanValues(rawRows);

      for (Map<String, Object> row : rows) {
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row in {}: {}", tableName, rowString);
      }

      assertThatRecords(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(cleanValues(expectedTableData.getValue()));
    }
  }

  private List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    for (Map<String, Object> row : rows) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue("NULL");
        } else if (entry.getValue() instanceof String) {
          entry.setValue(((String) entry.getValue()).trim());
        } else if (entry.getValue() instanceof byte[]) {
          entry.setValue(Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        } else if (entry.getValue() instanceof java.sql.Timestamp) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof java.sql.Time) {
          entry.setValue(entry.getValue().toString());
        }
      }
    }
    return rows;
  }

  private Map<String, List<Value>> getSpannerTableData() {
    Map<String, List<Value>> spannerRowData = new HashMap<>();

    spannerRowData.put(
        "tinyint", List.of(Value.int64(0), Value.int64(255), Value.int64(10), Value.int64(null)));
    spannerRowData.put(
        "smallint",
        List.of(Value.int64(-32768), Value.int64(32767), Value.int64(15), Value.int64(null)));
    spannerRowData.put(
        "int",
        List.of(
            Value.int64(-2147483648), Value.int64(2147483647), Value.int64(30), Value.int64(null)));
    spannerRowData.put(
        "bigint",
        List.of(
            Value.int64(-9223372036854775808L),
            Value.int64(9223372036854775807L),
            Value.int64(40),
            Value.int64(null)));
    spannerRowData.put("bit", List.of(Value.bool(false), Value.bool(true), Value.bool(null)));
    spannerRowData.put(
        "decimal",
        List.of(
            Value.numeric(new BigDecimal("68.7500")),
            Value.numeric(new BigDecimal("99999999999999.9999")),
            Value.numeric(null)));
    spannerRowData.put(
        "numeric",
        List.of(
            Value.numeric(new BigDecimal("68.7500")),
            Value.numeric(new BigDecimal("99999999999999.9999")),
            Value.numeric(null)));
    spannerRowData.put(
        "money",
        List.of(
            Value.numeric(new BigDecimal("-922337203685477.5808")),
            Value.numeric(new BigDecimal("922337203685477.5807")),
            Value.numeric(null)));
    spannerRowData.put(
        "smallmoney",
        List.of(
            Value.numeric(new BigDecimal("-214748.3648")),
            Value.numeric(new BigDecimal("214748.3647")),
            Value.numeric(null)));
    spannerRowData.put(
        "float",
        List.of(
            Value.float64(52.67),
            Value.float64(1.79E308),
            Value.float64(-1.79E308),
            Value.float64(null)));
    spannerRowData.put(
        "real",
        List.of(
            Value.float32(45.56F),
            Value.float32(3.40E38F),
            Value.float32(-3.40E38F),
            Value.float32(null)));
    spannerRowData.put(
        "date",
        List.of(
            Value.date(Date.parseDate("2012-09-17")),
            Value.date(Date.parseDate("0001-01-01")),
            Value.date(Date.parseDate("9999-12-31")),
            Value.date(null)));
    spannerRowData.put(
        "time", List.of(Value.string("00:00:00"), Value.string("12:34:56"), Value.string(null)));
    spannerRowData.put(
        "datetime",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("1998-01-23T12:45:56Z")),
            Value.timestamp(Timestamp.parseTimestampDuration("1753-01-01T00:00:00Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "datetime2",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "smalldatetime",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("1900-01-01T00:00:00Z")),
            Value.timestamp(Timestamp.parseTimestampDuration("2079-06-06T23:59:00Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "datetimeoffset",
        List.of(
            Value.timestamp(Timestamp.parseTimestampDuration("2022-08-05T08:23:11Z")),
            Value.timestamp(null)));
    spannerRowData.put(
        "char", List.of(Value.string("a"), Value.string("sample"), Value.string(null)));
    spannerRowData.put(
        "varchar", List.of(Value.string("abc"), Value.string("x".repeat(100)), Value.string(null)));
    spannerRowData.put(
        "text", List.of(Value.string("abc"), Value.string("sample text"), Value.string(null)));
    spannerRowData.put(
        "nchar", List.of(Value.string("a"), Value.string("unicode_char"), Value.string(null)));
    spannerRowData.put(
        "nvarchar", List.of(Value.string("abc"), Value.string("unicode_val"), Value.string(null)));
    spannerRowData.put("ntext", List.of(Value.string("ntext_val"), Value.string(null)));
    spannerRowData.put(
        "binary", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "varbinary", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "image", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "uniqueidentifier",
        List.of(Value.string("6F9619FF-8B86-D011-B42D-00C04FC964FF"), Value.string(null)));
    spannerRowData.put(
        "xml", List.of(Value.string("<root><child>value</child></root>"), Value.string(null)));

    // Alternative mappings
    spannerRowData.put(
        "tinyint_to_string", List.of(Value.string("10"), Value.string("255"), Value.string(null)));
    spannerRowData.put(
        "smallint_to_string",
        List.of(Value.string("15"), Value.string("32767"), Value.string(null)));
    spannerRowData.put(
        "int_to_string",
        List.of(Value.string("30"), Value.string("2147483647"), Value.string(null)));
    spannerRowData.put(
        "bigint_to_string",
        List.of(Value.string("40"), Value.string("9223372036854775807"), Value.string(null)));
    spannerRowData.put("bit_to_int64", List.of(Value.int64(0), Value.int64(1), Value.int64(null)));
    spannerRowData.put(
        "bit_to_string", List.of(Value.string("0"), Value.string("1"), Value.string(null)));
    spannerRowData.put("decimal_to_float64", List.of(Value.float64(68.75), Value.float64(null)));
    spannerRowData.put("decimal_to_string", List.of(Value.string("68.7500"), Value.string(null)));
    spannerRowData.put("numeric_to_float64", List.of(Value.float64(68.75), Value.float64(null)));
    spannerRowData.put("numeric_to_string", List.of(Value.string("68.7500"), Value.string(null)));
    spannerRowData.put("money_to_float64", List.of(Value.float64(68.75), Value.float64(null)));
    spannerRowData.put("money_to_string", List.of(Value.string("68.7500"), Value.string(null)));
    spannerRowData.put("smallmoney_to_float64", List.of(Value.float64(68.75), Value.float64(null)));
    spannerRowData.put(
        "smallmoney_to_string", List.of(Value.string("68.7500"), Value.string(null)));
    spannerRowData.put("float_to_string", List.of(Value.string("52.67"), Value.string(null)));
    spannerRowData.put("real_to_float64", List.of(Value.float64(45.56), Value.float64(null)));
    spannerRowData.put("real_to_string", List.of(Value.string("45.56"), Value.string(null)));
    spannerRowData.put("date_to_string", List.of(Value.string("2012-09-17"), Value.string(null)));
    spannerRowData.put(
        "datetime_to_string", List.of(Value.string("1998-01-23 12:45:56"), Value.string(null)));
    spannerRowData.put(
        "datetime2_to_string", List.of(Value.string("2022-08-05 08:23:11"), Value.string(null)));
    spannerRowData.put(
        "smalldatetime_to_string",
        List.of(Value.string("1900-01-01 00:00:00"), Value.string(null)));
    spannerRowData.put(
        "char_to_bytes", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "varchar_to_bytes",
        List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "nchar_to_bytes", List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "nvarchar_to_bytes",
        List.of(Value.bytesFromBase64("eDU4MDA="), Value.bytesFromBase64(null)));
    spannerRowData.put("binary_to_string", List.of(Value.string("7835383030"), Value.string(null)));
    spannerRowData.put(
        "varbinary_to_string", List.of(Value.string("7835383030"), Value.string(null)));
    spannerRowData.put("image_to_string", List.of(Value.string("7835383030"), Value.string(null)));
    spannerRowData.put(
        "uniqueidentifier_to_bytes",
        List.of(Value.bytesFromBase64("/xmWb4aLEdC0LQDAT8lk/w=="), Value.bytesFromBase64(null)));
    spannerRowData.put(
        "xml_to_bytes",
        List.of(
            Value.bytesFromBase64("PHJvb3Q+PGNoaWxkPnZhbHVlPC9jaGlsZD48L3Jvb3Q+"),
            Value.bytesFromBase64(null)));

    return spannerRowData;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();

    expectedData.put("tinyint", createRows("tinyint", (short) 0, (short) 255, (short) 10, null));
    expectedData.put(
        "smallint", createRows("smallint", (short) -32768, (short) 32767, (short) 15, null));
    expectedData.put("int", createRows("int", -2147483648, 2147483647, 30, null));
    expectedData.put(
        "bigint", createRows("bigint", -9223372036854775808L, 9223372036854775807L, 40L, null));
    expectedData.put("bit", createRows("bit", false, true, null));
    expectedData.put(
        "decimal",
        createRows(
            "decimal", new BigDecimal("68.7500"), new BigDecimal("99999999999999.9999"), null));
    expectedData.put(
        "numeric",
        createRows(
            "numeric", new BigDecimal("68.7500"), new BigDecimal("99999999999999.9999"), null));
    expectedData.put(
        "money",
        createRows(
            "money",
            new BigDecimal("-922337203685477.5808"),
            new BigDecimal("922337203685477.5807"),
            null));
    expectedData.put(
        "smallmoney",
        createRows(
            "smallmoney", new BigDecimal("-214748.3648"), new BigDecimal("214748.3647"), null));
    expectedData.put("float", createRows("float", 52.67, 1.79E308, -1.79E308, null));
    expectedData.put("real", createRows("real", 45.56F, 3.40E38F, -3.40E38F, null));
    expectedData.put(
        "date",
        createRows(
            "date",
            java.sql.Date.valueOf("2012-09-17"),
            java.sql.Date.valueOf("0001-01-01"),
            java.sql.Date.valueOf("9999-12-31"),
            null));
    expectedData.put(
        "time",
        createRows(
            "time", java.sql.Time.valueOf("00:00:00"), java.sql.Time.valueOf("12:34:56"), null));
    expectedData.put("char", createRows("char", "a", "sample", null));
    expectedData.put("varchar", createRows("varchar", "abc", "x".repeat(100), null));
    expectedData.put("text", createRows("text", "abc", "sample text", null));
    expectedData.put("nchar", createRows("nchar", "a", "unicode_char", null));
    expectedData.put("nvarchar", createRows("nvarchar", "abc", "unicode_val", null));
    expectedData.put("ntext", createRows("ntext", "ntext_val", null));
    expectedData.put("binary", createRows("binary", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "varbinary", createRows("varbinary", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put("image", createRows("image", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "uniqueidentifier",
        createRows("uniqueidentifier", "6F9619FF-8B86-D011-B42D-00C04FC964FF", null));
    expectedData.put("xml", createRows("xml", "<root><child>value</child></root>", null));

    // Alternative mappings
    expectedData.put(
        "tinyint_to_string", createRows("tinyint_to_string", (short) 10, (short) 255, null));
    expectedData.put(
        "smallint_to_string", createRows("smallint_to_string", (short) 15, (short) 32767, null));
    expectedData.put("int_to_string", createRows("int_to_string", 30, 2147483647, null));
    expectedData.put(
        "bigint_to_string", createRows("bigint_to_string", 40L, 9223372036854775807L, null));
    expectedData.put("bit_to_int64", createRows("bit_to_int64", false, true, null));
    expectedData.put("bit_to_string", createRows("bit_to_string", false, true, null));
    expectedData.put(
        "decimal_to_float64", createRows("decimal_to_float64", new BigDecimal("68.7500"), null));
    expectedData.put(
        "decimal_to_string", createRows("decimal_to_string", new BigDecimal("68.7500"), null));
    expectedData.put(
        "numeric_to_float64", createRows("numeric_to_float64", new BigDecimal("68.7500"), null));
    expectedData.put(
        "numeric_to_string", createRows("numeric_to_string", new BigDecimal("68.7500"), null));
    expectedData.put(
        "money_to_float64", createRows("money_to_float64", new BigDecimal("68.7500"), null));
    expectedData.put(
        "money_to_string", createRows("money_to_string", new BigDecimal("68.7500"), null));
    expectedData.put(
        "smallmoney_to_float64",
        createRows("smallmoney_to_float64", new BigDecimal("68.7500"), null));
    expectedData.put(
        "smallmoney_to_string",
        createRows("smallmoney_to_string", new BigDecimal("68.7500"), null));
    expectedData.put("float_to_string", createRows("float_to_string", 52.67, null));
    expectedData.put("real_to_float64", createRows("real_to_float64", 45.56F, null));
    expectedData.put("real_to_string", createRows("real_to_string", 45.56F, null));
    expectedData.put(
        "date_to_string", createRows("date_to_string", java.sql.Date.valueOf("2012-09-17"), null));
    expectedData.put("char_to_bytes", createRows("char_to_bytes", "x5800", null));
    expectedData.put("varchar_to_bytes", createRows("varchar_to_bytes", "x5800", null));
    expectedData.put("nchar_to_bytes", createRows("nchar_to_bytes", "x5800", null));
    expectedData.put("nvarchar_to_bytes", createRows("nvarchar_to_bytes", "x5800", null));
    expectedData.put(
        "binary_to_string",
        createRows("binary_to_string", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "varbinary_to_string",
        createRows("varbinary_to_string", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "image_to_string",
        createRows("image_to_string", Base64.getDecoder().decode("eDU4MDA="), null));
    expectedData.put(
        "uniqueidentifier_to_bytes",
        createRows("uniqueidentifier_to_bytes", "6F9619FF-8B86-D011-B42D-00C04FC964FF", null));
    expectedData.put(
        "xml_to_bytes", createRows("xml_to_bytes", "<root><child>value</child></root>", null));

    return expectedData;
  }

  private List<Map<String, Object>> createRows(String type, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>(vals.size());
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>(2, 1.0f);
      row.put("id", i + 1);
      row.put(getColumnName(type), vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private String getTableName(String type) {
    return type + "_table";
  }

  private String getColumnName(String type) {
    return type + "_col";
  }
}
