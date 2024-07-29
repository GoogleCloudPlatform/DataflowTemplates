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
package com.google.cloud.teleport.v2.templates.postgresql;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
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
import org.apache.beam.it.jdbc.PostgresResourceManager;
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

  public static PostgresResourceManager postgreSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String POSTGRESQL_FILE_RESOURCE = "DataTypesIt/postgresql/data-types.sql";

  private static final String SPANNER_DDL_RESOURCE = "DataTypesIt/postgresql/spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    postgreSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgreSQLResourceManager);
  }

  @Test
  public void mySQLAllTypesTest() throws Exception {
    loadSQLFileResource(postgreSQLResourceManager, POSTGRESQL_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgreSQLResourceManager,
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
        "bigint", createRows("bigint", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigserial", createRows("bigserial", "9223372036854775807", "-9223372036854775808"));
    expectedData.put("bit", createRows("bit", "0", "1", "NULL"));
    expectedData.put("bit_varying", createRows("bit_varying", "101", "NULL"));
    expectedData.put("bool", createRows("bool", "false", "true", "NULL"));
    expectedData.put("boolean", createRows("boolean", "false", "true", "NULL"));
    expectedData.put(
        "box",
        createRows(
            "box", "((999999999999999, 1000000000000001), (100000000000000123, 123))", "NULL"));
    expectedData.put("bytea", createRows("bytea", "abc", "NULL"));
    expectedData.put("char", createRows("char", "Ã", "¶", "NULL"));
    expectedData.put("character", createRows("character", "Ã", "¶", "NULL"));
    expectedData.put(
        "character_varying", createRows("character_varying", "character varying", "NULL"));
    expectedData.put(
        "cidr",
        createRows("cidr", "192.168.100.128/25", "192.168.1", "192", "::ffff:1.2.3.0/128", "NULL"));
    expectedData.put(
        "circle",
        createRows("circle", "((999999999999999, 1000000000000001), 100000000000000123)", "NULL"));
    expectedData.put(
        "date",
        createRows(
            "date",
            "4713-01-01 BC",
            "5874897-12-31",
            "0001-01-01 BC",
            "10000-01-01",
            "0001-01-01",
            "9999-12-31",
            "NULL"));
    expectedData.put(
        "decimal", createRows("decimal", "Infinity", "-Infinity", "NaN", "1.23", "NULL"));
    expectedData.put(
        "double_precision",
        createRows(
            "double_precision",
            "1.9876542e307",
            "-1.9876542e307",
            "NaN",
            "Infinity",
            "-Infinity",
            "NULL"));
    expectedData.put(
        "float4",
        createRows(
            "float4", "1.9876542e38", "-1.9876542e38", "NaN", "Infinity", "-Infinity", "NULL"));
    expectedData.put(
        "float8",
        createRows(
            "float8", "1.9876542e307", "-1.9876542e307", "NaN", "Infinity", "-Infinity", "NULL"));
    expectedData.put("inet", createRows("inet", "192.168.1.0/24", "NULL"));
    expectedData.put("int2", createRows("int2", "32767", "-32768", "NULL"));
    expectedData.put("int4", createRows("int4", "2147483647", "-2147483648", "NULL"));
    expectedData.put(
        "int8", createRows("int8", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put("interval", createRows("interval", "1 day", "1 month", "1 year", "NULL"));
    expectedData.put(
        "json",
        createRows(
            "json",
            "{\"duplicate_key\": 1, \"duplicate_key\": 2}",
            "{\"big_number\": 1e9999999999}",
            "{\"null_key\": null}",
            "{\"number\": 1e4931}",
            "NULL"));
    expectedData.put(
        "jsonb",
        createRows(
            "jsonb",
            "{\"duplicate_key\": 1, \"duplicate_key\": 2}",
            "{\"big_number\": 1e9999999999}",
            "{\"null_key\": null}",
            "{\"number\": 1e4931}",
            "NULL"));
    expectedData.put("line", createRows("line", "{ 1, 2, 3 }", "NULL"));
    expectedData.put("lseg", createRows("lseg", "[ (1, 2), (3, 4) ]", "NULL"));
    expectedData.put("macaddr", createRows("macaddr", "08:00:2b:01:02:03", "NULL"));
    expectedData.put("macaddr8", createRows("macaddr8", "08:00:2b:01:02:03:04:05", "NULL"));
    expectedData.put("money", createRows("money", "123.45", "NULL"));
    expectedData.put(
        "numeric", createRows("numeric", "Infinity", "-Infinity", "NaN", "1.23", "NULL"));
    expectedData.put("path", createRows("path", "[ (1, 2), (3, 4), (5, 6) ]", "NULL"));
    expectedData.put("pg_lsn", createRows("pg_lsn", "123/0", "NULL"));
    expectedData.put("pg_snapshot", createRows("pg_snapshot", "10:20:10,14,15", "NULL"));
    expectedData.put("point", createRows("point", "(1, 2)", "NULL"));
    expectedData.put("polygon", createRows("polygon", "( (1, 2), (3, 4) )", "NULL"));
    expectedData.put("serial2", createRows("serial2", "32767", "-32768"));
    expectedData.put("serial4", createRows("serial4", "2147483647", "-2147483648"));
    expectedData.put(
        "serial8", createRows("serial8", "9223372036854775807", "-9223372036854775808"));
    expectedData.put("smallint", createRows("smallint", "32767", "-32768", "NULL"));
    expectedData.put("smallserial", createRows("smallserial", "32767", "-32768"));
    expectedData.put("text", createRows("text", "text", "NULL"));
    expectedData.put("time", createRows("time", "00:00:00", "23:59:59", "00:00:00", "NULL"));
    expectedData.put(
        "time_without_time_zone",
        createRows("time_without_time_zone", "00:00:00", "23:59:59", "00:00:00", "NULL"));
    expectedData.put(
        "timestamp",
        createRows("timestamp", "4713-01-01 00:00:00 BC", "294276-12-31 23:59:59", "NULL"));
    expectedData.put(
        "timestamp_without_time_zone",
        createRows(
            "timestamp_without_time_zone",
            "4713-01-01 00:00:00 BC",
            "294276-12-31 23:59:59",
            "NULL"));
    expectedData.put(
        "timestamptz",
        createRows("timestamptz", "4713-01-01 00:00:00 BC", "294276-12-31 23:59:59", "NULL"));
    expectedData.put(
        "timestamp_with_time_zone",
        createRows(
            "timestamp_with_time_zone", "4713-01-01 00:00:00 BC", "294276-12-31 23:59:59", "NULL"));
    expectedData.put("timetz", createRows("timetz", "00:00:00", "00:00:00", "NULL"));
    expectedData.put(
        "time_with_time_zone", createRows("time_with_time_zone", "00:00:00", "00:00:00", "NULL"));
    expectedData.put("tsquery", createRows("tsquery", "fat & rat", "NULL"));
    expectedData.put(
        "tsvector", createRows("tsvector", "a fat cat sat on a mat and ate a fat rat", "NULL"));
    expectedData.put("txid_snapshot", createRows("txid_snapshot", "10:20:10,14,15", "NULL"));
    expectedData.put("uuid", createRows("uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "NULL"));
    expectedData.put("varbit", createRows("varbit", "101", "NULL"));
    expectedData.put("varchar", createRows("varchar", "varchar", "NULL"));
    expectedData.put("xml", createRows("xml", "<test>123</test>", "NULL"));
    return expectedData;
  }
}
