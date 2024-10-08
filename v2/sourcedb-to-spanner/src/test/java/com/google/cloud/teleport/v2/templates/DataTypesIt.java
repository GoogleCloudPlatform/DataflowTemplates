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
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all data types
 * migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class DataTypesIt extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataTypesIt.class);
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String MYSQL_DUMP_FILE_RESOURCE = "DataTypesIt/mysql/data-types.sql";

  private static final String SPANNER_DDL_RESOURCE = "DataTypesIt/mysql/spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void allTypesTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Validate supported data types.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("%s_table", type);
      String colName = String.format("%s_col", type);
      LOG.info("Asserting type: {}", type);

      List<Struct> rows = spannerResourceManager.readTableRecords(tableName, "id", colName);
      for (Struct row : rows) {
        // Limit logs printed for very large strings.
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
        List.of(
            "spatial_linestring",
            "spatial_multilinestring",
            "spatial_multipoint",
            "spatial_multipolygon",
            "spatial_point",
            "spatial_polygon");

    for (String table : unsupportedTypeTables) {
      // Unsupported rows should still be migrated. Each source table has 1 row.
      assertThat(spannerResourceManager.getRowCount(table)).isEqualTo(1L);
    }
  }

  private List<Map<String, Object>> createRows(String colPrefix, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i + 1);
      row.put(String.format("%s_col", colPrefix), vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put(
        "bigint",
        createRows("bigint", "40", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigint_unsigned",
        createRows("bigint_unsigned", "42", "0", "18446744073709551615", "NULL"));
    expectedData.put(
        "binary",
        createRows("binary", "eDU4MD" + repeatString("A", 334), repeatString("/", 340), "NULL"));
    expectedData.put("bit", createRows("bit", "f/////////8=", "NULL"));
    expectedData.put("blob", createRows("blob", "eDU4MDA=", repeatString("/", 87380), "NULL"));
    expectedData.put("bool", createRows("bool", "false", "true", "NULL"));
    expectedData.put("boolean", createRows("boolean", "false", "true", "NULL"));
    expectedData.put(
        "char", createRows("char", "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put("date", createRows("date", "2012-09-17", "1000-01-01", "9999-12-31", "NULL"));
    expectedData.put(
        "datetime",
        createRows(
            "datetime",
            "1998-01-23T12:45:56Z",
            "1000-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "decimal",
        createRows(
            "decimal",
            "68.75",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));
    expectedData.put(
        "double",
        createRows("double", "52.67", "1.7976931348623157E308", "-1.7976931348623157E308", "NULL"));
    expectedData.put("enum", createRows("enum", "1", "NULL"));
    expectedData.put("float", createRows("float", "45.56", "3.4E38", "-3.4E38", "NULL"));
    expectedData.put("int", createRows("int", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put("json", createRows("json", "{\"k1\": \"v1\"}", "NULL"));
    expectedData.put(
        "longblob", createRows("longblob", "eDU4MDA=", repeatString("/", 87380), "NULL"));
    expectedData.put(
        "longtext",
        createRows("longtext", "longtext", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put(
        "mediumblob", createRows("mediumblob", "eDU4MDA=", repeatString("/", 87380), "NULL"));
    expectedData.put("mediumint", createRows("mediumint", "20", "NULL"));
    expectedData.put(
        "mediumint_unsigned", createRows("mediumint_unsigned", "42", "0", "16777215", "NULL"));
    expectedData.put(
        "mediumtext",
        createRows("mediumtext", "mediumtext", repeatString("a", 33) + "...", "NULL"));
    expectedData.put("smallint", createRows("smallint", "15", "32767", "-32768", "NULL"));
    expectedData.put(
        "smallint_unsigned", createRows("smallint_unsigned", "42", "0", "65535", "NULL"));
    expectedData.put("text", createRows("text", "xyz", repeatString("a", 33) + "...", "NULL"));
    expectedData.put("time", createRows("time", "15:50:00", "838:59:59", "-838:59:59", "NULL"));
    expectedData.put(
        "timestamp",
        createRows(
            "timestamp",
            "2022-08-05T08:23:11Z",
            "1970-01-01T00:00:01Z",
            "2038-01-19T03:14:07Z",
            "NULL"));
    expectedData.put(
        "tinyblob", createRows("tinyblob", "eDU4MDA=", repeatString("/", 340), "NULL"));
    expectedData.put("tinyint", createRows("tinyint", "10", "127", "-128", "NULL"));
    expectedData.put("tinyint_unsigned", createRows("tinyint_unsigned", "0", "255", "NULL"));
    expectedData.put(
        "tinytext",
        createRows("tinytext", "tinytext", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put(
        "varbinary", createRows("varbinary", "eDU4MDA=", repeatString("/", 86666) + "8=", "NULL"));
    expectedData.put(
        "varchar", createRows("varchar", "abc", repeatString("a", 33) + "...", "NULL"));
    expectedData.put("year", createRows("year", "2022", "1901", "2155", "NULL"));
    expectedData.put("set", createRows("set", "v1,v2", "NULL"));
    expectedData.put(
        "integer_unsigned", createRows("integer_unsigned", "0", "42", "4294967296", "NULL"));
    expectedData.put(
        "bigint_unsigned_pk", createRows("bigint_unsigned", "0", "42", "18446744073709551615"));
    expectedData.put("string_pk", createRows("string", "Cloud", "Google", "Spanner"));
    return expectedData;
  }

  private static String repeatString(String str, int count) {
    return new String(new char[count]).replace("\0", str);
  }
}
