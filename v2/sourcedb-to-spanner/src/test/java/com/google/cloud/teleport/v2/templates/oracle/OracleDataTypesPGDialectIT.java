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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataTypesPGDialectIT extends SourceDbToSpannerITBase {

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager pgDialectSpannerResourceManager;
  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleDataTypesPGDialectIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDataTypesPGDialectIT/oracle-postgresql-spanner-schema.sql";

  @Before
  public void setUp() throws Exception {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pgDialectSpannerResourceManager);
  }

  @Test
  public void allTypesTestPGDialect() throws Exception {
    loadSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE, testUsername);
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_DDL_RESOURCE);

    org.apache.beam.it.common.PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            pgDialectSpannerResourceManager,
            java.util.Map.of(
                "namespace",
                testUsername,
                "maxConnections",
                "10",
                "jdbcDriverJars",
                oracleDriverGCSPath()),
            null);

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(15L)));
    assertThatResult(result).isLaunchFinished();

    Map<String, Integer> expectedCounts = getExpectedCounts();

    for (Map.Entry<String, Integer> entry : expectedCounts.entrySet()) {
      String tableName = entry.getKey();
      int expectedCount = entry.getValue();
      System.out.println("VERIFYING TABLE: " + tableName);
      String pkColumn =
          tableName.endsWith("_pk_table") ? tableName.replace("_pk_table", "_pk_col") : "id";
      List<Struct> rows = pgDialectSpannerResourceManager.readTableRecords(tableName, pkColumn);
      assertEquals("Row count mismatch for " + tableName, expectedCount, rows.size());
    }
  }

  private Map<String, Integer> getExpectedCounts() {
    Map<String, Integer> counts = new HashMap<>();
    counts.put("varchar2_table", 4);
    counts.put("varchar2_to_varchar_table", 4);
    counts.put("varchar_table", 4);
    counts.put("varchar_to_varchar_table", 4);
    counts.put("char_table", 4);
    counts.put("char_to_varchar_table", 4);
    counts.put("character_table", 4);
    counts.put("character_to_varchar_table", 4);
    counts.put("nvarchar2_table", 4);
    counts.put("nvarchar2_to_varchar_table", 4);
    counts.put("nchar_table", 4);
    counts.put("nchar_to_varchar_table", 4);
    counts.put("nchar_varying_table", 4);
    counts.put("nchar_varying_to_varchar_table", 4);
    counts.put("national_character_table", 4);
    counts.put("national_character_to_varchar_table", 4);
    counts.put("national_char_table", 4);
    counts.put("national_char_to_varchar_table", 4);
    counts.put("national_character_varying_table", 4);
    counts.put("national_character_varying_to_varchar_table", 4);
    counts.put("national_char_varying_table", 4);
    counts.put("national_char_varying_to_varchar_table", 4);
    counts.put("number_table", 6);
    counts.put("number_to_numeric_table", 6);
    counts.put("number_to_varchar_table", 6);
    counts.put("number_to_bigint_table", 6);
    counts.put("numeric_table", 6);
    counts.put("numeric_to_double_precision_table", 6);
    counts.put("numeric_to_varchar_table", 6);
    counts.put("numeric_to_bigint_table", 6);
    counts.put("decimal_table", 6);
    counts.put("decimal_to_double_precision_table", 6);
    counts.put("decimal_to_varchar_table", 6);
    counts.put("decimal_to_bigint_table", 6);
    counts.put("dec_table", 6);
    counts.put("dec_to_double_precision_table", 6);
    counts.put("dec_to_varchar_table", 6);
    counts.put("dec_to_bigint_table", 6);
    counts.put("float_table", 8);
    counts.put("float_to_numeric_table", 8);
    counts.put("float_to_varchar_table", 8);
    counts.put("double_precision_table", 8);
    counts.put("double_precision_to_numeric_table", 8);
    counts.put("double_precision_to_varchar_table", 8);
    counts.put("real_table", 8);
    counts.put("real_to_numeric_table", 8);
    counts.put("real_to_varchar_table", 8);
    counts.put("binary_float_table", 8);
    counts.put("binary_float_to_double_precision_table", 8);
    counts.put("binary_float_to_varchar_table", 8);
    counts.put("binary_double_table", 8);
    counts.put("binary_double_to_varchar_table", 8);
    counts.put("binary_double_to_numeric_table", 8);
    counts.put("integer_table", 5);
    counts.put("integer_to_numeric_table", 5);
    counts.put("integer_to_varchar_table", 5);
    counts.put("integer_to_double_precision_table", 5);
    counts.put("integer_pk_table", 3);
    counts.put("int_table", 5);
    counts.put("int_to_numeric_table", 5);
    counts.put("int_to_varchar_table", 5);
    counts.put("int_to_double_precision_table", 5);
    counts.put("int_pk_table", 3);
    counts.put("smallint_table", 5);
    counts.put("smallint_to_numeric_table", 5);
    counts.put("smallint_to_varchar_table", 5);
    counts.put("smallint_to_double_precision_table", 5);
    counts.put("smallint_pk_table", 3);
    counts.put("date_to_date_table", 3);
    counts.put("date_to_varchar_table", 3);
    counts.put("timestamp_to_varchar_table", 3);
    counts.put("timestamp_with_time_zone_table", 3);
    counts.put("timestamp_with_time_zone_to_varchar_table", 3);
    counts.put("timestamp_with_local_time_zone_table", 3);
    counts.put("timestamp_with_local_time_zone_to_varchar_table", 3);
    counts.put("interval_year_to_month_table", 2);
    counts.put("interval_year_to_month_to_bigint_months_table", 2);
    counts.put("interval_year_to_month_to_double_precision_table", 2);
    counts.put("interval_day_to_second_table", 1);
    counts.put("interval_day_to_second_to_bigint_millis_table", 1);
    counts.put("interval_day_to_second_to_double_precision_table", 1);
    counts.put("raw_table", 4);
    counts.put("raw_to_bytea_table", 4);
    counts.put("raw_to_varchar_base64_table", 4);
    counts.put("long_raw_table", 3);
    counts.put("long_raw_to_varchar_base64_table", 3);
    counts.put("blob_table", 3);
    counts.put("blob_to_varchar_base64_table", 3);
    counts.put("clob_table", 3);
    counts.put("nclob_table", 3);
    counts.put("bfile_table", 2);
    counts.put("bfile_to_bytea_table", 2);
    counts.put("bfile_to_varchar_url_table", 2);
    counts.put("long_table", 3);
    counts.put("long_to_bytea_table", 3);
    counts.put("rowid_table", 2);
    counts.put("rowid_to_bytea_table", 2);
    counts.put("rowid_to_bigint_table", 2);
    counts.put("urowid_table", 2);
    counts.put("urowid_to_bytea_table", 2);
    counts.put("urowid_to_bigint_table", 2);
    counts.put("json_table", 3);
    counts.put("json_to_varchar_table", 3);
    counts.put("json_to_bytea_table", 3);
    counts.put("xmltype_table", 3);
    counts.put("xmltype_to_bytea_table", 3);
    return counts;
  }
}
