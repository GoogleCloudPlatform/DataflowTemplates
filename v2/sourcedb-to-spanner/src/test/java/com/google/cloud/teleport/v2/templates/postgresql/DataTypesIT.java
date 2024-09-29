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
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all PostgreSQL data
 * types migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class DataTypesIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataTypesIT.class);

  public static PostgresResourceManager postgreSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE = "DataTypesIt/postgresql/data-types.sql";
  private static final String SPANNER_DDL_RESOURCE = "DataTypesIt/postgresql/spanner-schema.sql";

  /** Setup resource managers. */
  @Before
  public void setUp() {
    postgreSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job, all the resources, and resource managers. */
  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgreSQLResourceManager);
  }

  @Test
  public void allTypesTest() throws Exception {
    loadSQLFileResource(postgreSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    System.setProperty("numWorkers", "20");
    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("numPartitions", "100");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgreSQLResourceManager,
            spannerResourceManager,
            jobParameters,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("t_%s", type);
      LOG.info("Asserting type:{}", type);

      List<Struct> rows = spannerResourceManager.readTableRecords(tableName, "id", "col");
      for (Struct row : rows) {
        LOG.info("Found row: {}", row);
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    HashMap<String, List<Map<String, Object>>> result = new HashMap<>();
    result.put("bigint", createRows("-9223372036854775808", "9223372036854775807", "42", "NULL"));
    result.put("bigserial", createRows("-9223372036854775808", "9223372036854775807", "42"));
    result.put("bit", createRows("MA==", "MQ==", "NULL"));
    result.put("bit_varying", createRows("MDEwMQ==", "NULL"));
    result.put("bool", createRows("false", "true", "NULL"));
    result.put("boolean", createRows("false", "true", "NULL"));
    result.put("box", createRows("(3,4),(1,2)", "NULL"));
    result.put("bytea", createRows("YWJj", "NULL"));
    result.put("char", createRows("a", "Θ", "NULL"));
    result.put("character", createRows("a", "Ξ", "NULL"));
    result.put("character_varying", createRows("testing character varying", "NULL"));
    result.put(
        "cidr",
        createRows(
            "192.168.100.128/25", "192.168.1.0/24", "192.0.0.0/24", "::ffff:1.2.3.0/128", "NULL"));
    result.put("circle", createRows("<(1,2),3>", "NULL"));
    result.put("date", createRows("0001-01-01", "9999-12-31", "NULL"));
    result.put("decimal", createRows("0.12", "NULL"));
    result.put(
        "double_precision",
        createRows(
            "-1.9876542E307", "1.9876542E307", "NaN", "-Infinity", "Infinity", "1.23", "NULL"));
    result.put(
        "float4",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "2.34", "NULL"));
    result.put(
        "float8",
        createRows(
            "-1.9876542E307", "1.9876542E307", "NaN", "-Infinity", "Infinity", "3.45", "NULL"));
    result.put("inet", createRows("192.168.1.0/24", "NULL"));
    result.put("int", createRows("-2147483648", "2147483647", "1", "NULL"));
    result.put("integer", createRows("-2147483648", "2147483647", "2", "NULL"));
    result.put("int2", createRows("-32768", "32767", "3", "NULL"));
    result.put("int4", createRows("-2147483648", "2147483647", "4", "NULL"));
    result.put("int8", createRows("-9223372036854775808", "9223372036854775807", "5", "NULL"));
    result.put("json", createRows("{\"duplicate_key\":1}", "{\"null_key\":null}", "NULL"));
    result.put("jsonb", createRows("{\"duplicate_key\":2}", "{\"null_key\":null}", "NULL"));
    result.put("line", createRows("{1,2,3}", "NULL"));
    result.put("lseg", createRows("[(1,2),(3,4)]", "NULL"));
    result.put("macaddr", createRows("08:00:2b:01:02:03", "NULL"));
    result.put("macaddr8", createRows("08:00:2b:01:02:03:04:05", "NULL"));
    result.put("money", createRows("123.45", "NULL"));
    result.put("numeric", createRows("4.56", "NULL"));
    result.put("oid", createRows("1000", "NULL"));
    result.put("path", createRows("[(1,2),(3,4),(5,6)]", "NULL"));
    result.put("pg_lsn", createRows("123/0", "NULL"));
    result.put("pg_snapshot", createRows("1000:1000:", "NULL"));
    result.put("point", createRows("(1,2)", "NULL"));
    result.put("polygon", createRows("((1,2),(3,4))", "NULL"));
    result.put(
        "real",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "5.67", "NULL"));
    result.put("serial", createRows("-2147483648", "2147483647", "6"));
    result.put("serial2", createRows("-32768", "32767", "7"));
    result.put("serial4", createRows("-2147483648", "2147483647", "8"));
    result.put("serial8", createRows("-9223372036854775808", "9223372036854775807", "9"));
    result.put("smallint", createRows("-32768", "32767", "10", "NULL"));
    result.put("smallserial", createRows("-32768", "32767", "11"));
    result.put("text", createRows("testing text", "NULL"));
    result.put("time", createRows("00:00:00", "23:59:59", "00:00:00", "NULL"));
    result.put("timestamp", createRows("1970-01-02T03:04:05.123456Z", "NULL"));
    result.put(
        "timestamptz",
        createRows("1970-02-02T18:05:06.123456000Z", "1970-02-03T05:05:06.123456000Z", "NULL"));
    result.put("time_without_time_zone", createRows("00:00:00", "23:59:59", "00:00:00", "NULL"));
    result.put(
        "timestamp_with_time_zone",
        createRows("1970-02-02T18:05:06.123456000Z", "1970-02-03T05:05:06.123456000Z", "NULL"));
    result.put("timestamp_without_time_zone", createRows("1970-01-02T03:04:05.123456Z", "NULL"));
    result.put("tsquery", createRows("'fat' & 'rat'", "NULL"));
    result.put("tsvector", createRows("'a' 'cat' 'fat' 'mat' 'on' 'sat'", "NULL"));
    result.put("txid_snapshot", createRows("10:20:10,14,15", "NULL"));
    result.put("uuid", createRows("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "NULL"));
    result.put("varbit", createRows("MTEwMA==", "NULL"));
    result.put("varchar", createRows("testing varchar", "NULL"));
    result.put("xml", createRows("<test>123</test>", "NULL"));
    return result;
  }

  private List<Map<String, Object>> createRows(Object... values) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i + 1);
      row.put("col", values[i]);
      rows.add(row);
    }
    return rows;
  }
}
