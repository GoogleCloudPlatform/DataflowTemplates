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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all SQL Server data
 * types migration to PostgreSQL dialect Spanner.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDataTypesPGDialectIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerDataTypesPGDialectIT.class);
  protected PipelineLauncher.LaunchInfo jobInfo;

  protected MSSQLResourceManager msSqlResourceManager;
  protected SpannerResourceManager spannerResourceManager;

  private static final String SQLSERVER_DATA_TYPES_RESOURCE =
      "DataTypesIT/sqlserver-data-types.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "DataTypesIT/sqlserver-pg-dialect-spanner-schema.sql";

  /** Setup resource managers. */
  @Before
  public void setUp() throws Exception {
    msSqlResourceManager = setUpMSSQLResourceManager();
    spannerResourceManager = setUpPGDialectSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, msSqlResourceManager);
  }

  @Test
  public void allTypesTest() throws Exception {
    loadSQLFileResource(msSqlResourceManager, SQLSERVER_DATA_TYPES_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            msSqlResourceManager,
            spannerResourceManager,
            Map.of("maxConnections", "4"),
            null);
    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(15L)));
    assertThatResult(result).isLaunchFinished();

    // Validate supported data types.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    validateResult(spannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("%s_table", type);
      String colName = String.format("%s_col", type);
      LOG.info("Asserting type: {}", type);

      List<Struct> rows = resourceManager.readTableRecords(tableName, "id", colName);
      for (Struct row : rows) {
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row: {}", rowString);
      }
      if (type.startsWith("rowversion") || type.startsWith("timestamp")) {
        assertThat(rows).hasSize(2);
        for (Struct row : rows) {
          assertThat(row.isNull(colName)).isFalse();
          if (type.endsWith("_to_int64")) {
            long val = row.getLong(colName);
            assertThat(val).isGreaterThan(0L);
          } else if (type.endsWith("_to_bytes")) {
            com.google.cloud.ByteArray val = row.getBytes(colName);
            assertThat(val.length()).isEqualTo(8);
          } else {
            String val = row.getString(colName);
            assertThat(val.length()).isEqualTo(16);
          }
        }
        continue;
      }
      if (type.equals("vector")) {
        assertThat(rows).hasSize(2);
        for (Struct row : rows) {
          if (row.getLong("id") == 1L) {
            assertThat(row.getDoubleList(colName)).containsExactly(1.5, 2.5, 3.5).inOrder();
          } else {
            assertThat(row.isNull(colName)).isTrue();
          }
        }
        continue;
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private List<Map<String, Object>> createRows(String colPrefix, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>();
      if (colPrefix.toLowerCase().contains("_pk")) {
        row.put("id", vals.get(i));
      } else {
        row.put("id", i + 1);
      }
      row.put(String.format("%s_col", colPrefix), vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put("tinyint", createRows("tinyint", "0", "255", "128", "42", "NULL"));
    expectedData.put(
        "tinyint_to_string", createRows("tinyint_to_string", "0", "255", "128", "42", "NULL"));
    expectedData.put("tinyint_pk", createRows("tinyint_pk", "0", "255", "128", "42"));

    expectedData.put("smallint", createRows("smallint", "-32768", "32767", "0", "15", "NULL"));
    expectedData.put(
        "smallint_to_string",
        createRows("smallint_to_string", "-32768", "32767", "0", "15", "NULL"));
    expectedData.put("smallint_pk", createRows("smallint_pk", "-32768", "32767", "0", "15"));

    expectedData.put("int", createRows("int", "-2147483648", "2147483647", "0", "30", "NULL"));
    expectedData.put(
        "int_to_string",
        createRows("int_to_string", "-2147483648", "2147483647", "0", "30", "NULL"));
    expectedData.put("int_pk", createRows("int_pk", "-2147483648", "2147483647", "0", "30"));

    expectedData.put(
        "bigint",
        createRows("bigint", "-9223372036854775808", "9223372036854775807", "0", "40", "NULL"));
    expectedData.put(
        "bigint_to_string",
        createRows(
            "bigint_to_string", "-9223372036854775808", "9223372036854775807", "0", "40", "NULL"));
    expectedData.put(
        "bigint_pk",
        createRows("bigint_pk", "-9223372036854775808", "9223372036854775807", "0", "40"));

    expectedData.put("bit", createRows("bit", "false", "true", "NULL"));
    expectedData.put("bit_to_int64", createRows("bit_to_int64", "0", "1", "NULL"));
    expectedData.put("bit_to_string", createRows("bit_to_string", "false", "true", "NULL"));
    expectedData.put("bit_pk", createRows("bit_pk", "false", "true"));

    expectedData.put(
        "decimal",
        createRows(
            "decimal",
            "68.750000000",
            "9999999999999999999.999999999",
            "-9999999999999999999.999999999",
            "0.000000000",
            "NULL"));
    expectedData.put(
        "decimal_to_float64",
        createRows("decimal_to_float64", "68.75", "1.0E19", "-1.0E19", "0.0", "NULL"));
    expectedData.put(
        "decimal_to_string",
        createRows(
            "decimal_to_string",
            "68.750000000",
            "9999999999999999999.999999999",
            "-9999999999999999999.999999999",
            "0.000000000",
            "NULL"));

    expectedData.put(
        "numeric",
        createRows(
            "numeric",
            "68.750000000",
            "9999999999999999999.999999999",
            "-9999999999999999999.999999999",
            "0.000000000",
            "NULL"));
    expectedData.put(
        "numeric_to_float64",
        createRows("numeric_to_float64", "68.75", "1.0E19", "-1.0E19", "0.0", "NULL"));
    expectedData.put(
        "numeric_to_string",
        createRows(
            "numeric_to_string",
            "68.750000000",
            "9999999999999999999.999999999",
            "-9999999999999999999.999999999",
            "0.000000000",
            "NULL"));
    expectedData.put(
        "numeric_pk",
        createRows(
            "numeric_pk",
            "68.750000000",
            "9999999999999999999.999999999",
            "-9999999999999999999.999999999",
            "0.000000000"));

    expectedData.put(
        "money",
        createRows(
            "money",
            "-922337203685477.580800000",
            "922337203685477.580700000",
            "123.450000000",
            "0.000000000",
            "NULL"));
    expectedData.put(
        "money_to_float64",
        createRows(
            "money_to_float64",
            "-9.223372036854776E14",
            "9.223372036854776E14",
            "123.45",
            "0.0",
            "NULL"));
    expectedData.put(
        "money_to_string",
        createRows(
            "money_to_string",
            "-922337203685477.5808",
            "922337203685477.5807",
            "123.4500",
            "0.0000",
            "NULL"));

    expectedData.put(
        "smallmoney",
        createRows(
            "smallmoney",
            "-214748.364800000",
            "214748.364700000",
            "50.250000000",
            "0.000000000",
            "NULL"));
    expectedData.put(
        "smallmoney_to_float64",
        createRows("smallmoney_to_float64", "-214748.3648", "214748.3647", "50.25", "0.0", "NULL"));
    expectedData.put(
        "smallmoney_to_string",
        createRows(
            "smallmoney_to_string", "-214748.3648", "214748.3647", "50.2500", "0.0000", "NULL"));

    expectedData.put("float", createRows("float", "-1.79E308", "1.79E308", "45.56", "0.0", "NULL"));
    expectedData.put(
        "float_to_string",
        createRows("float_to_string", "-1.79E308", "1.79E308", "45.56", "0.0", "NULL"));

    expectedData.put("real", createRows("real", "-3.4E38", "3.4E38", "12.34", "0.0", "NULL"));
    expectedData.put(
        "real_to_float64",
        createRows("real_to_float64", "-3.4E38", "3.4E38", "12.34", "0.0", "NULL"));
    expectedData.put(
        "real_to_string",
        createRows("real_to_string", "-3.4E38", "3.4E38", "12.34", "0.0", "NULL"));

    expectedData.put("date", createRows("date", "0001-01-01", "9999-12-31", "2022-09-17", "NULL"));
    expectedData.put(
        "date_to_string",
        createRows("date_to_string", "0001-01-01", "9999-12-31", "2022-09-17", "NULL"));
    expectedData.put("date_pk", createRows("date_pk", "0001-01-01", "9999-12-31", "2022-09-17"));

    expectedData.put(
        "time",
        createRows("time", "00:00:00.0000000", "23:59:59.9999999", "15:30:45.1234567", "NULL"));
    expectedData.put(
        "time_to_bytes",
        createRows(
            "time_to_bytes",
            "MDA6MDA6MDAuMDAwMDAwMA==",
            "MjM6NTk6NTkuOTk5OTk5OQ==",
            "MTU6MzA6NDUuMTIzNDU2Nw==",
            "NULL"));
    expectedData.put(
        "time_pk",
        createRows("time_pk", "00:00:00.0000000", "23:59:59.9999999", "15:30:45.1234567"));

    expectedData.put(
        "datetime2",
        createRows(
            "datetime2",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime2_to_string",
        createRows(
            "datetime2_to_string",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime2_pk",
        createRows(
            "datetime2_pk",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z"));

    expectedData.put(
        "datetimeoffset",
        createRows(
            "datetimeoffset",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetimeoffset_to_string",
        createRows(
            "datetimeoffset_to_string",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetimeoffset_pk",
        createRows(
            "datetimeoffset_pk",
            "1970-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "9999-12-31T23:59:59Z"));

    expectedData.put(
        "datetime",
        createRows(
            "datetime",
            "1970-01-01T00:00:00Z",
            "1998-01-23T12:45:56Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime_to_string",
        createRows(
            "datetime_to_string",
            "1970-01-01T00:00:00Z",
            "1998-01-23T12:45:56Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime_pk",
        createRows(
            "datetime_pk", "1970-01-01T00:00:00Z", "1998-01-23T12:45:56Z", "9999-12-31T23:59:59Z"));

    expectedData.put(
        "smalldatetime",
        createRows(
            "smalldatetime",
            "1900-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "2079-06-06T23:59:00Z",
            "NULL"));
    expectedData.put(
        "smalldatetime_to_string",
        createRows(
            "smalldatetime_to_string",
            "1900-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "2079-06-06T23:59:00Z",
            "NULL"));
    expectedData.put(
        "smalldatetime_pk",
        createRows(
            "smalldatetime_pk",
            "1900-01-01T00:00:00Z",
            "2023-05-15T12:30:00Z",
            "2079-06-06T23:59:00Z"));

    expectedData.put("char", createRows("char", "a         ", "hello     ", "NULL"));
    expectedData.put(
        "char_to_bytes",
        createRows("char_to_bytes", "YSAgICAgICAgIA==", "aGVsbG8gICAgIA==", "NULL"));
    expectedData.put("char_pk", createRows("char_pk", "a         ", "hello     "));

    expectedData.put("varchar", createRows("varchar", "a", "test varchar", "NULL"));
    expectedData.put(
        "varchar_to_bytes", createRows("varchar_to_bytes", "Cg==", "dGVzdCB2YXJjaGFy", "NULL"));
    expectedData.put("varchar_pk", createRows("varchar_pk", "a", "test varchar"));

    expectedData.put("text", createRows("text", "a", "long text content", "NULL"));
    expectedData.put(
        "text_to_bytes", createRows("text_to_bytes", "Cg==", "bG9uZyB0ZXh0IGNvbnRlbnQ=", "NULL"));

    expectedData.put("nchar", createRows("nchar", "a         ", "unicode   ", "NULL"));
    expectedData.put(
        "nchar_to_bytes",
        createRows("nchar_to_bytes", "YSAgICAgICAgIA==", "dW5pY29kZSAgIA==", "NULL"));
    expectedData.put("nchar_pk", createRows("nchar_pk", "a         ", "unicode   "));

    expectedData.put("nvarchar", createRows("nvarchar", "a", "nvarchar test", "NULL"));
    expectedData.put(
        "nvarchar_to_bytes",
        createRows("nvarchar_to_bytes", "Cg==", "bnZhcmNoYXIgdGVzdA==", "NULL"));
    expectedData.put("nvarchar_pk", createRows("nvarchar_pk", "a", "nvarchar test"));

    expectedData.put("ntext", createRows("ntext", "a", "ntext content", "NULL"));
    expectedData.put(
        "ntext_to_bytes", createRows("ntext_to_bytes", "Cg==", "bnRleHQgY29udGVudA==", "NULL"));

    expectedData.put("binary", createRows("binary", "AAAAAA==", "EjRWeA==", "NULL"));
    expectedData.put(
        "binary_to_string", createRows("binary_to_string", "00000000", "12345678", "NULL"));
    expectedData.put("binary_pk", createRows("binary_pk", "AAAAAA==", "EjRWeA=="));

    expectedData.put("varbinary", createRows("varbinary", "AA==", "q83v", "NULL"));
    expectedData.put(
        "varbinary_to_string", createRows("varbinary_to_string", "00", "abcdef", "NULL"));
    expectedData.put("varbinary_pk", createRows("varbinary_pk", "AQ==", "q83v"));

    expectedData.put("image", createRows("image", "AA==", "AQIDBA==", "NULL"));
    expectedData.put("image_to_string", createRows("image_to_string", "00", "01020304", "NULL"));

    expectedData.put(
        "uniqueidentifier",
        createRows(
            "uniqueidentifier",
            "6f9619ff-8b86-d011-b42d-00c04fc964ff",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "NULL"));
    expectedData.put(
        "uniqueidentifier_to_bytes",
        createRows(
            "uniqueidentifier_to_bytes",
            "b5YZ/4uG0BG0LQDAT8lk/w==",
            "oO68mZwLTvi7bWu5vTgKEQ==",
            "NULL"));
    expectedData.put(
        "uniqueidentifier_to_string",
        createRows(
            "uniqueidentifier_to_string",
            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "NULL"));
    expectedData.put(
        "uniqueidentifier_pk",
        createRows(
            "uniqueidentifier_pk",
            "6f9619ff-8b86-d011-b42d-00c04fc964ff",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));

    expectedData.put(
        "xml", createRows("xml", "<root><child>value</child></root>", "<item id=\"1\"/>", "NULL"));
    expectedData.put(
        "xml_to_bytes",
        createRows(
            "xml_to_bytes",
            "PHJvb3Q+PGNoaWxkPnZhbHVlPC9jaGlsZD48L3Jvb3Q+",
            "PGl0ZW0gaWQ9IjEiLz4=",
            "NULL"));

    expectedData.put("rowversion", createRows("rowversion", "placeholder1", "placeholder2"));
    expectedData.put(
        "rowversion_to_bytes", createRows("rowversion_to_bytes", "placeholder1", "placeholder2"));
    expectedData.put(
        "rowversion_to_int64", createRows("rowversion_to_int64", "placeholder1", "placeholder2"));
    expectedData.put("timestamp", createRows("timestamp", "placeholder1", "placeholder2"));
    expectedData.put(
        "timestamp_to_bytes", createRows("timestamp_to_bytes", "placeholder1", "placeholder2"));
    expectedData.put(
        "timestamp_to_int64", createRows("timestamp_to_int64", "placeholder1", "placeholder2"));

    expectedData.put("json", createRows("json", "{\"key\": \"val1\"}", "NULL"));
    expectedData.put("json_to_string", createRows("json_to_string", "{\"key\":\"val1\"}", "NULL"));
    expectedData.put("vector", createRows("vector", Arrays.asList(1.5, 2.5, 3.5), "NULL"));

    return expectedData;
  }
}
