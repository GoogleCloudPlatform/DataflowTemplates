/*
 * Copyright (C) 2025 Google LLC
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
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.jline.utils.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all data types for
 * Cassandra migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class CassandraAllDataTypesIT extends SourceDbToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLDataTypesIT.class);
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static CassandraResourceManager cassandraResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String CASSANDRA_DUMP_FILE_RESOURCE =
      "DataTypesIT/cassandra-data-types.csql";

  private static final String SPANNER_DDL_RESOURCE = "DataTypesIT/cassandra-spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    cassandraResourceManager = setupCassandraResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, cassandraResourceManager);
  }

  @Test
  public void allTypesTest() throws Exception {
    loadCSQLFileResource(cassandraResourceManager, CASSANDRA_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            cassandraResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(35L)));
    assertThatResult(result).isLaunchFinished();

    // Validate supported data types.
    Map<String, List<Map<String, String>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, String>>> entry : expectedData.entrySet()) {
      String tableName = entry.getKey();
      var columnNames = entry.getValue().get(0).keySet();
      List<Struct> rows =
          spannerResourceManager.readTableRecords(tableName, columnNames.toArray(new String[] {}));
      List<Map<String, String>> readValues = new ArrayList<>();
      for (Struct row : rows) {
        ImmutableMap.Builder<String, String> rowMapBuilder =
            new ImmutableMap.Builder<String, String>();
        columnNames.forEach(
            colName ->
                rowMapBuilder.put(
                    colName,
                    (row.getValue(colName) == null)
                        ? "NULL"
                        :
                        // plain toString truncates large Jsons with `...`
                        row.getColumnType(colName) == Type.json()
                            ? row.getJson(colName)
                            : row.getValue(colName).toString()));
        readValues.add(rowMapBuilder.build());
      }
      Log.info("Spanner Cassandra Values are: {}", readValues);
      assertThat(readValues).isEqualTo(entry.getValue());
    }
  }

  private Map<String, List<Map<String, String>>> getExpectedData() {
    Map<String, List<Map<String, String>>> expectedData = new HashMap<>();
    expectedData.put("all_data_types", getAllDataTypeRows());
    return expectedData;
  }

  private List<Map<String, String>> getAllDataTypeRows() {
    List<Map<String, String>> allDataTypeRows = new ArrayList<>();
    allDataTypeRows.add(getAllDataTypeNullRow());
    allDataTypeRows.add(getAllDataTypeMaxRow());
    allDataTypeRows.add(getAllDataTypeMinRow());
    return allDataTypeRows;
  }

  private Map<String, String> getAllDataTypeNullRow() {
    return ImmutableMap.<String, String>builder()
        .put("uuid_col", "NULL")
        .put("double_float_map_col", "{}")
        .put("int_col", "0")
        .put("decimal_set_col", "[]")
        .put("date_double_map_col", "{}")
        .put("uuid_ascii_map_col", "{}")
        .put("ascii_text_map_col", "{}")
        .put("timestamp_list_col", "[]")
        .put("date_col", "NULL")
        .put("int_set_col", "[]")
        .put("blob_col", "NULL")
        .put("smallint_set_col", "[]")
        .put("varchar_list_col", "[]")
        .put("inet_list_col", "[]")
        .put("bigint_list_col", "[]")
        .put("tinyint_varint_map_col", "{}")
        .put("text_set_col", "[]")
        .put("double_set_col", "[]")
        .put("time_list_col", "[]")
        .put("frozen_ascii_list_col", "[]")
        .put("int_list_col", "[]")
        .put("ascii_list_col", "[]")
        .put("date_set_col", "[]")
        .put("double_inet_map_col", "{}")
        .put("timestamp_set_col", "[]")
        .put("ascii_col", "NULL")
        .put("time_tinyint_map_col", "{}")
        .put("float_col", "0.0")
        .put("bigint_set_col", "[]")
        .put("varchar_set_col", "[]")
        .put("timestamp_col", "NULL")
        .put("tinyint_set_col", "[]")
        .put("time_col", "P0D")
        .put("bigint_boolean_map_col", "{}")
        .put("text_list_col", "[]")
        .put("boolean_list_col", "[]")
        .put("blob_list_col", "[]")
        .put("timeuuid_set_col", "[]")
        .put("int_time_map_col", "{}")
        .put("timeuuid_col", "NULL")
        .put("time_set_col", "[]")
        .put("boolean_set_col", "[]")
        .put("bigint_col", "0")
        .put("float_set_col", "[]")
        .put("boolean_col", "false")
        .put("ascii_set_col", "[]")
        .put("uuid_list_col", "[]")
        .put("varchar_bigint_map_col", "{}")
        .put("blob_int_map_col", "{}")
        .put("smallint_col", "0")
        .put("varint_blob_map_col", "{}")
        .put("double_list_col", "[]")
        .put("float_list_col", "[]")
        .put("smallint_list_col", "[]")
        .put("varint_list_col", "[]")
        .put("text_col", "NULL")
        .put("float_smallint_map_col", "{}")
        .put("smallint_timestamp_map_col", "{}")
        .put("text_timeuuid_map_col", "{}")
        .put("timeuuid_list_col", "[]")
        .put("decimal_col", "NULL")
        .put("inet_col", "NULL")
        .put("date_list_col", "[]")
        .put("varint_col", "NULL")
        .put("uuid_set_col", "[]")
        .put("boolean_decimal_map_col", "{}")
        .put("primary_key", "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d")
        .put("blob_set_col", "[]")
        .put("varchar_col", "NULL")
        .put("inet_text_map_col", "{}")
        .put("varint_set_col", "[]")
        .put("tinyint_list_col", "[]")
        .put("timestamp_uuid_map_col", "{}")
        .put("decimal_duration_map_col", "{}")
        .put("tinyint_col", "0")
        .put("decimal_list_col", "[]")
        .put("inet_set_col", "[]")
        .put("timeuuid_varchar_map_col", "{}")
        .put("duration_list_col", "[]")
        .put("double_col", "0.0")
        .put("duration_col", "NULL")
        .put("frozen_ascii_set_col", "[]")
        .build();
  }

  private Map<String, String> getAllDataTypeMinRow() {
    return ImmutableMap.<String, String>builder()
        .put("uuid_col", "00000000-0000-0000-0000-000000000000")
        .put("double_float_map_col", "{}")
        .put("int_col", "-2147483648")
        .put("decimal_set_col", "[]")
        .put("date_double_map_col", "{}")
        .put("uuid_ascii_map_col", "{}")
        .put("ascii_text_map_col", "{}")
        .put("timestamp_list_col", "[]")
        .put("date_col", "1901-12-13")
        .put("int_set_col", "[]")
        .put("blob_col", "AAAAAAAAAAA=")
        .put("smallint_set_col", "[]")
        .put("varchar_list_col", "[]")
        .put("inet_list_col", "[]")
        .put("bigint_list_col", "[]")
        .put("tinyint_varint_map_col", "{}")
        .put("text_set_col", "[]")
        .put("double_set_col", "[]")
        .put("time_list_col", "[]")
        .put("frozen_ascii_list_col", "[]")
        .put("int_list_col", "[]")
        .put("ascii_list_col", "[]")
        .put("date_set_col", "[]")
        .put("double_inet_map_col", "{}")
        .put("timestamp_set_col", "[]")
        .put("ascii_col", "")
        .put("time_tinyint_map_col", "{}")
        .put("float_col", "-3.4028235E38")
        .put("bigint_set_col", "[]")
        .put("varchar_set_col", "[]")
        .put("timestamp_col", "1970-01-01T00:00:00Z")
        .put("tinyint_set_col", "[]")
        .put("time_col", "P0D")
        .put("bigint_boolean_map_col", "{}")
        .put("text_list_col", "[]")
        .put("boolean_list_col", "[]")
        .put("blob_list_col", "[]")
        .put("timeuuid_set_col", "[]")
        .put("int_time_map_col", "{}")
        .put("timeuuid_col", "00000000-0000-1000-9000-000000000000")
        .put("time_set_col", "[]")
        .put("boolean_set_col", "[]")
        .put("bigint_col", "-9223372036854775808")
        .put("float_set_col", "[]")
        .put("boolean_col", "false")
        .put("ascii_set_col", "[]")
        .put("uuid_list_col", "[]")
        .put("varchar_bigint_map_col", "{}")
        .put("blob_int_map_col", "{}")
        .put("smallint_col", "-32768")
        .put("varint_blob_map_col", "{}")
        .put("double_list_col", "[]")
        .put("float_list_col", "[]")
        .put("smallint_list_col", "[]")
        .put("varint_list_col", "[]")
        .put("text_col", "")
        .put("float_smallint_map_col", "{}")
        .put("smallint_timestamp_map_col", "{}")
        .put("text_timeuuid_map_col", "{}")
        .put("timeuuid_list_col", "[]")
        .put("decimal_col", "-99999999999999999999999999999.999999999")
        .put("inet_col", "/0.0.0.0")
        .put("date_list_col", "[]")
        .put("varint_col", "-9223372036854775808")
        .put("uuid_set_col", "[]")
        .put("boolean_decimal_map_col", "{}")
        .put("primary_key", "fe3263a0-1577-4851-95f8-3af47628baa4")
        .put("blob_set_col", "[]")
        .put("varchar_col", "")
        .put("inet_text_map_col", "{}")
        .put("varint_set_col", "[]")
        .put("tinyint_list_col", "[]")
        .put("timestamp_uuid_map_col", "{}")
        .put("decimal_duration_map_col", "{}")
        .put("tinyint_col", "-128")
        .put("decimal_list_col", "[]")
        .put("inet_set_col", "[]")
        .put("timeuuid_varchar_map_col", "{}")
        .put("duration_list_col", "[]")
        .put("double_col", "-1.7976931348623157E308")
        .put("duration_col", "P-10675199DT-2H-48M-5S")
        .put("frozen_ascii_set_col", "[]")
        .build();
  }

  private Map<String, String> getAllDataTypeMaxRow() {

    return ImmutableMap.<String, String>builder()
        .put("uuid_col", "ffffffff-ffff-4fff-9fff-ffffffffffff")
        .put(
            "double_float_map_col",
            "{\"-Infinity\":\"0.0\",\"1.7976931348623157E308\":\"Infinity\",\"3.14\":\"-Infinity\",\"Infinity\":\"NaN\",\"NaN\":\"3.14\"}")
        .put("int_col", "2147483647")
        .put(
            "decimal_set_col",
            "[-99999999999999999999999999999.999999999,0,99999999999999999999999999999.999999999]")
        .put(
            "date_double_map_col",
            "{\"0\":\"NaN\",\"304\":\"3.14\",\"365\":\"Infinity\",\"365243\":\"0.0\",\"7305\":\"-Infinity\"}")
        .put(
            "uuid_ascii_map_col",
            "{\"00000000-0000-1000-9000-000000000000\":\"abc\",\"ffffffff-ffff-1fff-9fff-ffffffffffff\":\"def\"}")
        .put("ascii_text_map_col", "{\"a\":\"z\",\"z\":\"a\"}")
        .put(
            "timestamp_list_col",
            "[1970-01-01T00:00:00Z,1970-01-01T00:00:00Z,1969-12-31T23:59:59.999000000Z]")
        .put("date_col", "2038-01-19")
        .put("int_set_col", "[-2147483648,0,2147483647]")
        .put("blob_col", "////////")
        .put("smallint_set_col", "[-32768,0,32767]")
        .put("varchar_list_col", "[a,~]")
        .put("inet_list_col", "[/0.0.0.0,/172.16.0.0,/255.255.255.255]")
        .put("bigint_list_col", "[-9223372036854775808,0,9223372036854775807]")
        .put(
            "tinyint_varint_map_col",
            "{\"-128\":\"184467440000000000000\",\"127\":\"-184467440000000000000\"}")
        .put("text_set_col", "[G,KNOWS,LEONARDO,LER,MATHEMATICIAN,O,OG]")
        .put("double_set_col", "[-Infinity,-1.7976931348623157E308,0.0,3.1415926,Infinity,NaN]")
        .put("time_list_col", "[P0D,P0DT23H59M59.999999999S]")
        .put("frozen_ascii_list_col", "[a,b]")
        .put("int_list_col", "[-2147483648,0,2147483647]")
        .put("ascii_list_col", "[a,~]")
        .put("date_set_col", "[1901-12-13,1970-01-01,2038-01-19]")
        .put("double_inet_map_col", "{\"3.14\":\"/255.255.255.255\",\"Infinity\":\"/0.0.0.0\"}")
        .put(
            "timestamp_set_col",
            "[1970-01-01T00:00:00Z,1970-01-01T00:00:00Z,1969-12-31T23:59:59.999000000Z]")
        .put("ascii_col", "~~~~~~~")
        .put(
            "time_tinyint_map_col",
            "{\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": 0, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": 0}\":\"127\",\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": 0, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": 86399999999999}\":\"-128\"}")
        .put("float_col", "3.4028235E38")
        .put("bigint_set_col", "[-9223372036854775808,0,9223372036854775807]")
        .put("varchar_set_col", "[a,~]")
        .put("timestamp_col", "1969-12-31T23:59:59.999000000Z")
        .put("tinyint_set_col", "[-128,0,127]")
        .put("time_col", "P0DT23H59M59.999999999S")
        .put("bigint_boolean_map_col", "{\"42\":\"true\",\"84\":\"false\"}")
        .put("text_list_col", "[G,O,OG,LER,KNOWS,LEONARDO,MATHEMATICIAN]")
        .put("boolean_list_col", "[true,false]")
        .put("blob_list_col", "[AAAAAAAAAAA=,////////]")
        .put(
            "timeuuid_set_col",
            "[00000000-0000-1000-9000-000000000000,88888888-8888-1888-9888-888888888888,ffffffff-ffff-1fff-9fff-ffffffffffff]")
        .put(
            "int_time_map_col",
            "{\"1\":\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": 0, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": 0}\",\"42\":\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": 0, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": 2400}\"}")
        .put("timeuuid_col", "ffffffff-ffff-1fff-9fff-ffffffffffff")
        .put("time_set_col", "[P0D,P0DT23H59M59.999999999S]")
        .put("boolean_set_col", "[false,true]")
        .put("bigint_col", "9223372036854775807")
        .put("float_set_col", "[-Infinity,-3.4028235E38,0.0,3.4028235E38,Infinity,NaN]")
        .put("boolean_col", "true")
        .put("ascii_set_col", "[a,~]")
        .put(
            "uuid_list_col",
            "[00000000-0000-4000-9000-900000000000,88888888-8888-4888-9888-888888888888,ffffffff-ffff-4fff-9fff-ffffffffffff]")
        .put(
            "varchar_bigint_map_col",
            "{\"abcd\":\"-9223372036854775808\",\"efgh\":\"9223372036854775807\"}")
        .put("blob_int_map_col", "{\"0000000000000001\":\"1\",\"000000000000002a\":\"42\"}")
        .put("smallint_col", "32767")
        .put(
            "varint_blob_map_col",
            "{\"-184467440000000000000\":\"8000000000000000\",\"184467440000000000000\":\"7fffffffffffffff\"}")
        .put("double_list_col", "[NaN,-Infinity,-1.7976931348623157E308,0.0,3.1415926,Infinity]")
        .put("float_list_col", "[NaN,-Infinity,-3.4028235E38,0.0,3.4028235E38,Infinity]")
        .put("smallint_list_col", "[-32768,0,32767]")
        .put("varint_list_col", "[-9223372036854775808,0,9223372036854775808]")
        .put("text_col", "NULL")
        .put("float_smallint_map_col", "{\"3.14\":\"32767\",\"Infinity\":\"-32768\"}")
        .put("smallint_timestamp_map_col", "{\"-128\":\"0\",\"127\":\"-1000\"}")
        .put(
            "text_timeuuid_map_col",
            "{\"a\":\"00000000-0000-1000-9000-000000000000\",\"~\":\"ffffffff-ffff-1fff-9fff-ffffffffffff\"}")
        .put(
            "timeuuid_list_col",
            "[00000000-0000-1000-9000-000000000000,88888888-8888-1888-9888-888888888888,ffffffff-ffff-1fff-9fff-ffffffffffff]")
        .put("decimal_col", "99999999999999999999999999999.999999999")
        .put("inet_col", "/255.255.255.255")
        .put("date_list_col", "[1901-12-13,1970-01-01,2038-01-19]")
        .put("varint_col", "9223372036854775808")
        .put(
            "uuid_set_col",
            "[00000000-0000-4000-9000-900000000000,88888888-8888-4888-9888-888888888888,ffffffff-ffff-4fff-9fff-ffffffffffff]")
        .put("boolean_decimal_map_col", "{\"false\":\"0.0\",\"true\":\"0.1\"}")
        .put("primary_key", "e6bc8562-2575-420f-9344-9fedc4945f61")
        .put("blob_set_col", "[AAAAAAAAAAA=,////////]")
        .put("varchar_col", "~~~~~~~")
        .put("inet_text_map_col", "{\"/0.0.0.0\":\"test-text\"}")
        .put("varint_set_col", "[-9223372036854775808,0,9223372036854775808]")
        .put("tinyint_list_col", "[-128,0,127]")
        .put(
            "timestamp_uuid_map_col",
            "{\"-1000\":\"ffffffff-ffff-1fff-9fff-ffffffffffff\",\"0\":\"00000000-0000-1000-9000-000000000000\"}")
        .put(
            "decimal_duration_map_col",
            "{\"12.34\":\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": -10675199, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": -10085000000000}\",\"34.45\":\"{\\\"years\\\": 0, \\\"months\\\": 0, \\\"days\\\": 10675199, \\\"hours\\\": 0, \\\"minutes\\\": 0, \\\"seconds\\\": 0, \\\"nanos\\\": 10085000000000}\"}")
        .put("tinyint_col", "127")
        .put(
            "decimal_list_col",
            "[-99999999999999999999999999999.999999999,0,99999999999999999999999999999.999999999]")
        .put("inet_set_col", "[/0.0.0.0,/172.16.0.0,/255.255.255.255]")
        .put(
            "timeuuid_varchar_map_col",
            "{\"00000000-0000-1000-9000-000000000000\":\"abc\",\"ffffffff-ffff-1fff-9fff-ffffffffffff\":\"def\"}")
        .put("duration_list_col", "[P-10675199DT-2H-48M-5S,P0D,P10675199DT2H48M5S]")
        .put("double_col", "1.7976931348623157E308")
        .put("duration_col", "P10675199DT2H48M5S")
        .put("frozen_ascii_set_col", "[a,b]")
        .build();
  }
}
