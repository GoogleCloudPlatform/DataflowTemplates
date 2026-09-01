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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
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
 * types migration to a PostgreSQL dialect Spanner instance.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDataTypesPGDialectIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerDataTypesPGDialectIT.class);
  protected PipelineLauncher.LaunchInfo jobInfo;

  protected MSSQLResourceManager msSqlResourceManager;
  protected SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String SQLSERVER_DUMP_FILE_RESOURCE =
      "sqlserver/SQLServerDataTypesPGDialectIT/sqlserver-data-types.sql";

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDataTypesPGDialectIT/sqlserver-pg-dialect-spanner-schema.sql";

  /** Setup resource managers. */
  @Before
  public void setUp() throws Exception {
    msSqlResourceManager = setUpSQLServerResourceManager();
    pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pgDialectSpannerResourceManager, msSqlResourceManager);
  }

  @Test
  public void allTypesTestPGDialect() throws Exception {
    loadSQLFileResource(msSqlResourceManager, SQLSERVER_DUMP_FILE_RESOURCE);
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            msSqlResourceManager,
            pgDialectSpannerResourceManager,
            Map.of("maxConnections", "4"),
            null);
    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(15L)));
    assertThatResult(result).isLaunchFinished();

    // Validate supported data types.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    validateResult(pgDialectSpannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("%s_table", type);
      String colName = type.toLowerCase().contains("_pk") ? "val" : String.format("%s_col", type);
      LOG.info("Asserting type: {}", type);

      List<Struct> rows = resourceManager.readTableRecords(tableName, "id", colName);
      for (Struct row : rows) {
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row: {}", rowString);
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }

    // Validate unsupported types.
    List<String> unsupportedTypeTables =
        List.of("geography_table", "geometry_table", "hierarchyid_table", "sql_variant_table");

    for (String table : unsupportedTypeTables) {
      // Unsupported rows should still be migrated. Each source table has 1 row.
      assertThat(resourceManager.getRowCount(table)).isEqualTo(1L);
    }
  }

  private List<Map<String, Object>> createRows(String colPrefix, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>();
      if (colPrefix.toLowerCase().contains("_pk")) {
        row.put("id", vals.get(i));
        row.put("val", vals.get(i));
      } else {
        row.put("id", i + 1);
        row.put(String.format("%s_col", colPrefix), vals.get(i));
      }
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> createPkRows(Object[] ids, Object[] vals) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < ids.length; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", ids[i]);
      row.put("val", vals[i]);
      rows.add(row);
    }
    return rows;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();

    // Scenario A: Default Type Migration
    expectedData.put("tinyint", createRows("tinyint", "0", "255", "10", "127", "NULL"));
    expectedData.put("smallint", createRows("smallint", "-32768", "32767", "0", "15", "NULL"));
    expectedData.put("int", createRows("int", "-2147483648", "2147483647", "0", "30", "NULL"));
    expectedData.put(
        "bigint",
        createRows("bigint", "-9223372036854775808", "9223372036854775807", "0", "40", "NULL"));
    expectedData.put("bit", createRows("bit", "false", "true", "NULL"));
    expectedData.put(
        "decimal",
        createRows("decimal", "-99999999.9999", "99999999.9999", "0", "12345.6789", "NULL"));
    expectedData.put(
        "numeric",
        createRows("numeric", "-99999999.9999", "99999999.9999", "0", "12345.6789", "NULL"));
    expectedData.put(
        "money",
        createRows(
            "money", "-922337203685477.5808", "922337203685477.5807", "0", "123.45", "NULL"));
    expectedData.put(
        "smallmoney",
        createRows("smallmoney", "-214748.3648", "214748.3647", "0", "123.45", "NULL"));
    expectedData.put("float", createRows("float", "-1.79E308", "1.79E308", "0.0", "45.56", "NULL"));
    expectedData.put("real", createRows("real", "-3.4E38", "3.4E38", "0.0", "45.56", "NULL"));
    expectedData.put("date", createRows("date", "0001-01-01", "9999-12-31", "2024-05-15", "NULL"));
    expectedData.put(
        "time",
        createRows("time", "00:00:00.0000000", "23:59:59.9999999", "12:34:56.7890000", "NULL"));
    expectedData.put(
        "datetime2",
        createRows(
            "datetime2",
            "1970-01-01T00:00:00Z",
            "2024-05-15T12:34:56.789Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetimeoffset",
        createRows(
            "datetimeoffset",
            "1970-01-01T00:00:00Z",
            "2024-05-15T12:34:56.789Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime",
        createRows(
            "datetime",
            "1753-01-01T00:00:00Z",
            "2024-05-15T12:34:56Z",
            "9999-12-31T23:59:59.997Z",
            "NULL"));
    expectedData.put(
        "smalldatetime",
        createRows(
            "smalldatetime",
            "1900-01-01T00:00:00Z",
            "2024-05-15T12:34:00Z",
            "2079-06-06T23:59:00Z",
            "NULL"));
    expectedData.put("char", createRows("char", "a", "test", "NULL"));
    expectedData.put("varchar", createRows("varchar", "hello", "test varchar", "NULL"));
    expectedData.put(
        "varchar_max", createRows("varchar_max", "large varchar max payload content", "NULL"));
    expectedData.put("text", createRows("text", "sample text", "NULL"));
    expectedData.put("nchar", createRows("nchar", "a", "unicode", "NULL"));
    expectedData.put("nvarchar", createRows("nvarchar", "unicode test", "special chars", "NULL"));
    expectedData.put(
        "nvarchar_max", createRows("nvarchar_max", "nvarchar max payload content", "NULL"));
    expectedData.put("ntext", createRows("ntext", "sample ntext", "NULL"));
    expectedData.put(
        "binary", createRows("binary", "AAAAAAAAAAA=", "EjRWeA==", "/////w==", "NULL"));
    expectedData.put("varbinary", createRows("varbinary", "AA==", "EjRW", "/w==", "NULL"));
    expectedData.put("varbinary_max", createRows("varbinary_max", "AQIDBAU=", "NULL"));
    expectedData.put("image", createRows("image", "AA==", "EjRW", "/w==", "NULL"));
    expectedData.put(
        "uniqueidentifier",
        createRows(
            "uniqueidentifier",
            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "NULL"));
    expectedData.put(
        "xml",
        createRows(
            "xml", "<root><child>value</child></root>", "<item id=\"1\">text</item>", "NULL"));

    // Scenario B: Alternative Type Migration
    expectedData.put(
        "tinyint_to_string", createRows("tinyint_to_string", "0", "255", "10", "127", "NULL"));
    expectedData.put(
        "smallint_to_string",
        createRows("smallint_to_string", "-32768", "32767", "0", "15", "NULL"));
    expectedData.put(
        "int_to_string",
        createRows("int_to_string", "-2147483648", "2147483647", "0", "30", "NULL"));
    expectedData.put(
        "bigint_to_string",
        createRows(
            "bigint_to_string", "-9223372036854775808", "9223372036854775807", "0", "40", "NULL"));
    expectedData.put("bit_to_int64", createRows("bit_to_int64", "0", "1", "NULL"));
    expectedData.put(
        "decimal_to_float64",
        createRows(
            "decimal_to_float64", "-99999999.9999", "99999999.9999", "0.0", "12345.6789", "NULL"));
    expectedData.put(
        "decimal_to_string",
        createRows(
            "decimal_to_string",
            "-99999999.9999",
            "99999999.9999",
            "0.0000",
            "12345.6789",
            "NULL"));
    expectedData.put(
        "numeric_to_float64",
        createRows(
            "numeric_to_float64", "-99999999.9999", "99999999.9999", "0.0", "12345.6789", "NULL"));
    expectedData.put(
        "numeric_to_string",
        createRows(
            "numeric_to_string",
            "-99999999.9999",
            "99999999.9999",
            "0.0000",
            "12345.6789",
            "NULL"));
    expectedData.put(
        "money_to_float64",
        createRows(
            "money_to_float64",
            "-922337203685477.5808",
            "922337203685477.5807",
            "0.0",
            "123.45",
            "NULL"));
    expectedData.put(
        "money_to_string",
        createRows(
            "money_to_string",
            "-922337203685477.5808",
            "922337203685477.5807",
            "0.0000",
            "123.4500",
            "NULL"));
    expectedData.put(
        "smallmoney_to_float64",
        createRows(
            "smallmoney_to_float64", "-214748.3648", "214748.3647", "0.0", "123.45", "NULL"));
    expectedData.put(
        "smallmoney_to_string",
        createRows(
            "smallmoney_to_string", "-214748.3648", "214748.3647", "0.0000", "123.4500", "NULL"));
    expectedData.put(
        "float_to_string",
        createRows("float_to_string", "-1.79E308", "1.79E308", "0.0", "45.56", "NULL"));
    expectedData.put(
        "real_to_float64",
        createRows("real_to_float64", "-3.4E38", "3.4E38", "0.0", "45.56", "NULL"));
    expectedData.put(
        "real_to_string",
        createRows("real_to_string", "-3.4E38", "3.4E38", "0.0", "45.56", "NULL"));
    expectedData.put(
        "date_to_string",
        createRows("date_to_string", "0001-01-01", "9999-12-31", "2024-05-15", "NULL"));
    expectedData.put(
        "datetime2_to_string",
        createRows(
            "datetime2_to_string",
            "1970-01-01T00:00:00Z",
            "2024-05-15T12:34:56.789Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime_to_string",
        createRows(
            "datetime_to_string",
            "1753-01-01T00:00:00Z",
            "2024-05-15T12:34:56Z",
            "9999-12-31T23:59:59.997Z",
            "NULL"));
    expectedData.put(
        "smalldatetime_to_string",
        createRows(
            "smalldatetime_to_string",
            "1900-01-01T00:00:00Z",
            "2024-05-15T12:34:00Z",
            "2079-06-06T23:59:00Z",
            "NULL"));
    expectedData.put(
        "binary_to_string",
        createRows("binary_to_string", "00000000", "12345678", "ffffffff", "NULL"));
    expectedData.put(
        "varbinary_to_string", createRows("varbinary_to_string", "00", "123456", "ff", "NULL"));

    // Scenario C: Primary Key Mapping
    expectedData.put(
        "tinyint_pk",
        createPkRows(new Object[] {"0", "127", "255"}, new Object[] {"zero", "mid", "max"}));
    expectedData.put(
        "smallint_pk",
        createPkRows(new Object[] {"-32768", "0", "32767"}, new Object[] {"min", "zero", "max"}));
    expectedData.put(
        "int_pk",
        createPkRows(
            new Object[] {"-2147483648", "0", "2147483647"}, new Object[] {"min", "zero", "max"}));
    expectedData.put(
        "bigint_pk",
        createPkRows(
            new Object[] {"-9223372036854775808", "0", "9223372036854775807"},
            new Object[] {"min", "zero", "max"}));
    expectedData.put(
        "bit_pk", createPkRows(new Object[] {false, true}, new Object[] {"false", "true"}));
    expectedData.put(
        "date_pk",
        createPkRows(
            new Object[] {"1000-01-01", "2024-05-15", "9999-12-31"},
            new Object[] {"ancient", "current", "max"}));
    expectedData.put(
        "char_pk", createPkRows(new Object[] {"pk1", "pk2"}, new Object[] {"val1", "val2"}));
    expectedData.put(
        "varchar_pk", createPkRows(new Object[] {"key1", "key2"}, new Object[] {"val1", "val2"}));
    expectedData.put(
        "nchar_pk", createPkRows(new Object[] {"npk1", "npk2"}, new Object[] {"val1", "val2"}));
    expectedData.put(
        "nvarchar_pk",
        createPkRows(new Object[] {"nkey1", "nkey2"}, new Object[] {"val1", "val2"}));
    expectedData.put(
        "binary_pk",
        createPkRows(new Object[] {"AAAAAQ==", "AAAAAg=="}, new Object[] {"b1", "b2"}));
    expectedData.put(
        "varbinary_pk", createPkRows(new Object[] {"AQI=", "AwQ="}, new Object[] {"vb1", "vb2"}));
    expectedData.put(
        "uniqueidentifier_pk",
        createPkRows(
            new Object[] {
              "6F9619FF-8B86-D011-B42D-00C04FC964FF", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"
            },
            new Object[] {"u1", "u2"}));

    return expectedData;
  }
}
