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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kotlin.Pair;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests migration of all
 * SQL Server data types and expressions.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDatastreamToSpannerDataTypesIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerDatastreamToSpannerDataTypesIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerDataTypesIT/sqlserver-data-types.sql";
  private static final String SQLSERVER_DML_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerDataTypesIT/sqlserver-generated-col.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerDataTypesIT/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerDataTypesIT/pg-dialect-spanner-schema.sql";

  private static final List<String> UNSUPPORTED_TYPE_TABLES = List.of();

  private static boolean initialized = false;
  private static CloudSqlServerResourceManager msSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager pgDialectSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<SQLServerDatastreamToSpannerDataTypesIT> testInstances = new HashSet<>();

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SQLServerDatastreamToSpannerDataTypesIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up SQL Server test container resource manager...");
        msSqlResourceManager = setUpSqlServerResourceManager();
        LOG.info("SQL Server resource manager created with URI: {}", msSqlResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());
        LOG.info("Setting up PG dialect Spanner resource manager...");
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
        LOG.info(
            "PG dialect Spanner resource manager created with instance ID: {}",
            pgDialectSpannerResourceManager.getInstanceId());
        LOG.info("Setting up GCS resource manager...");
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        LOG.info("GCS resource manager created with bucket: {}", gcsResourceManager.getBucket());
        LOG.info("Setting up Pub/Sub resource manager...");
        pubsubResourceManager = setUpPubSubResourceManager();
        LOG.info("Pub/Sub resource manager created.");
        LOG.info("Setting up Datastream resource manager...");
        datastreamResourceManager = setUpDatastreamResourceManager();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing SQL Server DDL script...");
        executeSqlScript(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (SQLServerDatastreamToSpannerDataTypesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        msSqlResourceManager,
        spannerResourceManager,
        pgDialectSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testSqlServerDataTypesAndExpression() throws Exception {
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    addInitialExpectedDataGeneratedColumns(expectedData);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("datastreamSourceType", "sqlserver");

    SqlServerSource sqlServerSource =
        SqlServerSource.builder(
                msSqlResourceManager.getHost(),
                msSqlResourceManager.getUsername(),
                msSqlResourceManager.getPassword(),
                msSqlResourceManager.getPort(),
                msSqlResourceManager.getDatabaseName())
            .setAllowedTables(Map.of("dbo", getAllowedTables(expectedData)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "sqlserver-data-types",
            null,
            null,
            "sqlserver-datastream-to-spanner-data-types",
            spannerResourceManager,
            pubsubResourceManager,
            jobParameters,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            sqlServerSource);
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);

    LOG.info("Executing SQL Server DML script...");
    executeSqlScript(msSqlResourceManager, SQLSERVER_DML_RESOURCE);
    expectedData = getExpectedData();
    addUpdatedExpectedDataGeneratedColumns(expectedData);

    condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process DML data...");
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(15)), condition);
    assertThatResult(result).meetsConditions();

    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }

    validateResult(spannerResourceManager, expectedData);
  }

  @Test
  public void testSqlServerDataTypesPGDialect() throws Exception {
    LOG.info("Creating PG Dialect Spanner DDL...");
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_DDL_RESOURCE);
    Map<String, List<Map<String, Object>>> expectedData = getExpectedDataPGDialect();

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("datastreamSourceType", "sqlserver");

    SqlServerSource sqlServerSource =
        SqlServerSource.builder(
                msSqlResourceManager.getHost(),
                msSqlResourceManager.getUsername(),
                msSqlResourceManager.getPassword(),
                msSqlResourceManager.getPort(),
                msSqlResourceManager.getDatabaseName())
            .setAllowedTables(Map.of("dbo", getAllowedTables(expectedData)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "sqlserver-data-types-pg-dialect",
            null,
            null,
            "sqlserver-datastream-to-spanner-data-types-pg-dialect",
            pgDialectSpannerResourceManager,
            pubsubResourceManager,
            jobParameters,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            sqlServerSource);
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck condition = buildConditionCheck(pgDialectSpannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                condition);
    assertThatResult(result).meetsConditions();

    validateResult(pgDialectSpannerResourceManager, expectedData);
  }

  private String getTableName(String type) {
    if (type.endsWith("_column")) {
      return type;
    }
    return type + "_table";
  }

  private List<String> getAllowedTables(Map<String, List<Map<String, Object>>> expectedData) {
    List<String> tableNames = new ArrayList<>(expectedData.size());
    for (String tablePrefix : expectedData.keySet()) {
      tableNames.add(getTableName(tablePrefix));
    }
    return tableNames;
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    Set<String> ignoredTypeMappings = Set.of();
    List<AssertionError> errors = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      if (ignoredTypeMappings.contains(type)) {
        LOG.warn("Mapping for {} is ignored...", type);
        continue;
      }
      String tableName = getTableName(type);
      LOG.info("Asserting type: {}", type);

      List<Struct> rows =
          resourceManager.readTableRecords(tableName, entry.getValue().get(0).keySet());
      for (Struct row : rows) {
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row: {}", rowString);
      }
      try {
        SpannerAsserts.assertThatStructs(rows)
            .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
      } catch (AssertionError e) {
        LOG.error("Assertion failed for type: {}", type, e);
        errors.add(e);
      }
    }
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }

    for (String table : UNSUPPORTED_TYPE_TABLES) {
      if (ignoredTypeMappings.contains(table)) {
        continue;
      }
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
      } else {
        row.put("id", i + 1);
      }
      row.put(String.format("%s_col", colPrefix), vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> createPkRows(
      String colPrefix, Pair<Object, Object>... pkAndValPairs) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Pair<Object, Object> pair : pkAndValPairs) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", pair.getFirst());
      row.put(String.format("%s_col", colPrefix), pair.getSecond());
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> createMultiColumnRows(
      List<List<Pair<String, Object>>> rowsValues) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (List<Pair<String, Object>> rowValues : rowsValues) {
      Map<String, Object> row = new HashMap<>();
      for (Pair<String, Object> colValue : rowValues) {
        row.put(colValue.getFirst(), colValue.getSecond());
      }
      rows.add(row);
    }
    return rows;
  }

  private ConditionCheck buildConditionCheck(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    Set<String> ignoredTables = Set.of();

    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      if (ignoredTables.contains(entry.getKey())) {
        continue;
      }
      String tableName = getTableName(entry.getKey());
      int numRows = entry.getValue().size();
      ConditionCheck c =
          SpannerRowsCheck.builder(resourceManager, tableName).setMinRows(numRows).build();
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }

    ConditionCheck unsupportedTableCondition = null;
    for (String unsupportedTypeTable : UNSUPPORTED_TYPE_TABLES) {
      if (ignoredTables.contains(unsupportedTypeTable)) {
        continue;
      }
      ConditionCheck c =
          SpannerRowsCheck.builder(resourceManager, unsupportedTypeTable).setMinRows(1).build();
      if (unsupportedTableCondition == null) {
        unsupportedTableCondition = c;
      } else {
        unsupportedTableCondition = unsupportedTableCondition.and(c);
      }
    }

    return combinedCondition != null && unsupportedTableCondition != null
        ? combinedCondition.and(unsupportedTableCondition)
        : combinedCondition != null ? combinedCondition : unsupportedTableCondition;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();

    expectedData.put("tinyint", createRows("tinyint", "10", "255", "0", "NULL"));
    expectedData.put(
        "tinyint_to_string", createRows("tinyint_to_string", "10", "255", "0", "NULL"));
    expectedData.put("tinyint_pk", createRows("tinyint_pk", "10", "255", "0"));

    expectedData.put("smallint", createRows("smallint", "15", "32767", "-32768", "NULL"));
    expectedData.put(
        "smallint_to_string", createRows("smallint_to_string", "15", "32767", "-32768", "NULL"));
    expectedData.put("smallint_pk", createRows("smallint_pk", "15", "32767", "-32768"));

    expectedData.put("int", createRows("int", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put(
        "int_to_string", createRows("int_to_string", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put("int_pk", createRows("int_pk", "30", "2147483647", "-2147483648"));

    expectedData.put(
        "bigint",
        createRows("bigint", "40", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigint_to_string",
        createRows(
            "bigint_to_string", "40", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigint_pk", createRows("bigint_pk", "40", "9223372036854775807", "-9223372036854775808"));

    expectedData.put("bit", createRows("bit", "false", "true", "NULL"));
    expectedData.put("bit_to_int64", createRows("bit_to_int64", "0", "1", "NULL"));
    expectedData.put("bit_to_string", createRows("bit_to_string", "false", "true", "NULL"));
    expectedData.put("bit_pk", createRows("bit_pk", "false", "true"));

    expectedData.put(
        "decimal",
        createRows(
            "decimal",
            "68.75",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));
    expectedData.put(
        "decimal_to_string",
        createRows(
            "decimal_to_string",
            "68.750000000",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));
    expectedData.put(
        "decimal_to_float64",
        createRows(
            "decimal_to_float64",
            "68.75",
            "9.999999999999999E22",
            "-9.999999999999999E22",
            "NULL"));

    expectedData.put(
        "numeric",
        createRows(
            "numeric",
            "68.75",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));
    expectedData.put(
        "numeric_to_string",
        createRows(
            "numeric_to_string",
            "68.750000000",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));

    expectedData.put(
        "money",
        createRows("money", "922337203685477.5807", "-922337203685477.5808", "100.5", "NULL"));
    expectedData.put(
        "money_to_string",
        createRows(
            "money_to_string",
            "922337203685477.5807",
            "-922337203685477.5808",
            "100.5000",
            "NULL"));

    expectedData.put(
        "smallmoney", createRows("smallmoney", "214748.3647", "-214748.3648", "50.25", "NULL"));
    expectedData.put(
        "smallmoney_to_string",
        createRows("smallmoney_to_string", "214748.3647", "-214748.3648", "50.2500", "NULL"));

    expectedData.put("float", createRows("float", "45.56", "1.79E308", "-1.79E308", "NULL"));
    expectedData.put(
        "float_to_string",
        createRows("float_to_string", "45.56", "1.79E+308", "-1.79E+308", "NULL"));

    expectedData.put("real", createRows("real", "45.56", "3.4E38", "-3.4E38", "NULL"));
    expectedData.put(
        "real_to_float64", createRows("real_to_float64", "45.56", "3.4E38", "-3.4E38", "NULL"));
    expectedData.put(
        "real_to_string", createRows("real_to_string", "45.56", "3.4E+38", "-3.4E+38", "NULL"));

    expectedData.put("date", createRows("date", "2022-09-17", "0001-01-01", "9999-12-31", "NULL"));
    expectedData.put(
        "date_to_string",
        createRows("date_to_string", "2022-09-17", "0001-01-01", "9999-12-31", "NULL"));
    expectedData.put("date_pk", createRows("date_pk", "2022-09-17", "0001-01-01", "9999-12-31"));

    expectedData.put("time", createRows("time", "PT15H50M", "PT0S", "PT23H59M59.999999S", "NULL"));
    expectedData.put("time_pk", createRows("time_pk", "PT15H50M", "PT0S", "PT23H59M59.999999S"));

    expectedData.put(
        "datetime2",
        createRows(
            "datetime2",
            "2022-08-05T08:23:11.123456000Z",
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59.999999000Z",
            "NULL"));
    expectedData.put("datetime2_to_string", createRows("datetime2_to_string", "", "", "", "NULL"));
    expectedData.put(
        "datetime2_pk",
        createRows(
            "datetime2_pk",
            "2022-08-05T08:23:11.123456000Z",
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59.999999000Z"));

    expectedData.put(
        "datetimeoffset",
        createRows(
            "datetimeoffset",
            "2022-08-05T08:23:11.123456000Z",
            "1754-08-30T22:43:41.128654848Z",
            "1816-03-30T05:56:08.066276376Z",
            "NULL"));
    expectedData.put(
        "datetimeoffset_to_string",
        createRows(
            "datetimeoffset_to_string",
            "2022-08-05T08:23:11.123456Z",
            "1754-08-30T22:43:41.128654848Z",
            "1816-03-30T05:56:08.066276376Z",
            "NULL"));
    expectedData.put(
        "datetimeoffset_pk",
        createRows(
            "datetimeoffset_pk",
            "2022-08-05T08:23:11.123456000Z",
            "1754-08-30T22:43:41.128654848Z"));

    expectedData.put(
        "datetime",
        createRows(
            "datetime",
            "1998-01-23T12:45:56Z",
            "1753-01-01T00:00:00Z",
            "9999-12-31T23:59:59.997000000Z",
            "NULL"));
    expectedData.put("datetime_to_string", createRows("datetime_to_string", "", "", "", "NULL"));
    expectedData.put(
        "datetime_pk",
        createRows(
            "datetime_pk",
            "1998-01-23T12:45:56Z",
            "1753-01-01T00:00:00Z",
            "9999-12-31T23:59:59.997000000Z"));

    expectedData.put(
        "smalldatetime",
        createRows(
            "smalldatetime",
            "2022-08-05T08:23:00Z",
            "1900-01-01T00:00:00Z",
            "2079-06-06T23:59:00Z",
            "NULL"));
    expectedData.put(
        "smalldatetime_to_string", createRows("smalldatetime_to_string", "", "", "", "NULL"));
    expectedData.put(
        "smalldatetime_pk",
        createRows(
            "smalldatetime_pk",
            "2022-08-05T08:23:00Z",
            "1900-01-01T00:00:00Z",
            "2079-06-06T23:59:00Z"));

    String charA = String.format("%-255s", "a");
    String charSample = String.format("%-255s", "sample_char");
    String ncharSample = String.format("%-255s", "sample_nchar");

    expectedData.put(
        "char",
        createRows(
            "char", Value.string(charA).toString(), Value.string(charSample).toString(), "NULL"));
    expectedData.put(
        "char_to_bytes",
        createRows(
            "char_to_bytes",
            "Cu/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+8=",
            "/5/v37+f7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+8=",
            "NULL"));
    expectedData.put(
        "char_pk",
        createPkRows(
            "char_pk",
            new Pair<>(
                Value.string(String.format("%-100s", "pk1")).toString(),
                Value.string(String.format("%-100s", "val1")).toString()),
            new Pair<>(
                Value.string(String.format("%-100s", "pk2")).toString(),
                Value.string(String.format("%-100s", "val2")).toString())));

    expectedData.put("varchar", createRows("varchar", "abc", "test_varchar", "NULL"));
    expectedData.put(
        "varchar_to_bytes", createRows("varchar_to_bytes", "Crw=", "/u/vn7+f", "NULL"));
    expectedData.put(
        "varchar_pk",
        createPkRows("varchar_pk", new Pair<>("vpk1", "vval1"), new Pair<>("vpk2", "vval2")));

    expectedData.put("text", createRows("text", "sample text data", "extended text", "NULL"));

    expectedData.put(
        "nchar",
        createRows(
            "nchar", Value.string(charA).toString(), Value.string(ncharSample).toString(), "NULL"));
    expectedData.put(
        "nchar_to_bytes",
        createRows(
            "nchar_to_bytes",
            "Cu/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+8=",
            "/5/v3/z67+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+8=",
            "NULL"));
    expectedData.put(
        "nchar_pk",
        createPkRows(
            "nchar_pk",
            new Pair<>(
                Value.string(String.format("%-100s", "npk1")).toString(),
                Value.string(String.format("%-100s", "nval1")).toString()),
            new Pair<>(
                Value.string(String.format("%-100s", "npk2")).toString(),
                Value.string(String.format("%-100s", "nval2")).toString())));

    expectedData.put("nvarchar", createRows("nvarchar", "abc", "unicode_ñ_ä_test", "NULL"));
    expectedData.put(
        "nvarchar_to_bytes", createRows("nvarchar_to_bytes", "Crw=", "7/z93+/v/u8=", "NULL"));
    expectedData.put(
        "nvarchar_pk",
        createPkRows("nvarchar_pk", new Pair<>("nvpk1", "nvval1"), new Pair<>("nvpk2", "nvval2")));

    expectedData.put("ntext", createRows("ntext", "sample ntext data", "extended ntext", "NULL"));

    expectedData.put(
        "binary",
        createRows(
            "binary",
            "3/qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo=",
            "n4qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo=",
            "NULL"));
    expectedData.put(
        "binary_to_string",
        createRows(
            "binary_to_string",
            Value.string("EjQ" + "A".repeat(337)).toString(),
            Value.string("AP8" + "A".repeat(337)).toString(),
            "NULL"));
    expectedData.put(
        "binary_pk",
        createRows(
            "binary_pk",
            "3/qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqu8=",
            "n4qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqu8="));

    expectedData.put("varbinary", createRows("varbinary", "3+/v7w==", "72bv7w==", "NULL"));
    expectedData.put(
        "varbinary_to_string", createRows("varbinary_to_string", "EjSrzQ==", "yv66vg==", "NULL"));
    expectedData.put("varbinary_pk", createRows("varbinary_pk", "3+/v7w==", "72bv7w=="));

    expectedData.put("image", createRows("image", "76/v7w==", "+e9K7w==", "NULL"));
    expectedData.put(
        "image_to_string", createRows("image_to_string", "iVBORw==", "/9j/4A==", "NULL"));

    expectedData.put(
        "uniqueidentifier",
        createRows(
            "uniqueidentifier",
            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "NULL"));
    expectedData.put(
        "uniqueidentifier_pk",
        createRows(
            "uniqueidentifier_pk",
            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"));

    expectedData.put(
        "xml",
        createRows(
            "xml",
            "<root><elem>test</elem></root>",
            Value.string("<user id=\"1\"><name>sqlserver</name></user>").toString(),
            "NULL"));

    return expectedData;
  }

  private void addInitialExpectedDataGeneratedColumns(
      Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA BB")))));

    expectedData.put(
        "generated_non_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("id", 1),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA BB")),
                Arrays.asList(
                    new Pair<>("id", 10),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA BB")))));

    expectedData.put(
        "non_generated_to_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA BB"),
                    new Pair<>("generated_column_pk_col", "AA ")))));

    expectedData.put(
        "generated_to_non_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA "),
                    new Pair<>("generated_column_pk_col", "AA BB")))));
  }

  private void addUpdatedExpectedDataGeneratedColumns(
      Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA BB")))));
    expectedData.put(
        "generated_non_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("id", 2),
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "NULL")),
                Arrays.asList(
                    new Pair<>("id", 3),
                    new Pair<>("first_name_col", "DD"),
                    new Pair<>("last_name_col", "EE"),
                    new Pair<>("generated_column_col", "NULL")),
                Arrays.asList(
                    new Pair<>("id", 11),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "NULL")))));

    expectedData.put(
        "non_generated_to_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "NULL"),
                    new Pair<>("generated_column_pk_col", "CC ")))));

    expectedData.put(
        "generated_to_non_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA "),
                    new Pair<>("generated_column_pk_col", "AA BB")))));
  }

  private Map<String, List<Map<String, Object>>> getExpectedDataPGDialect() {
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    expectedData.put(
        "decimal",
        createRows(
            "decimal",
            "68.750000000",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));
    expectedData.put(
        "decimal_to_float64",
        createRows(
            "decimal_to_float64",
            "68.75",
            "9.999999999999999E22",
            "-9.999999999999999E22",
            "NULL"));
    expectedData.put(
        "numeric",
        createRows(
            "numeric",
            "68.750000000",
            "99999999999999999999999.999999999",
            "-99999999999999999999999.999999999",
            "NULL"));
    expectedData.put(
        "money",
        createRows(
            "money",
            "922337203685477.580700000",
            "-922337203685477.580800000",
            "100.500000000",
            "NULL"));
    expectedData.put(
        "smallmoney",
        createRows("smallmoney", "214748.364700000", "-214748.364800000", "50.250000000", "NULL"));
    expectedData.put("time", createRows("time", "PT15H50M", "PT0S", "PT23H59M59.999999S", "NULL"));
    expectedData.put("time_pk", createRows("time_pk", "PT15H50M", "PT0S", "PT23H59M59.999999S"));

    return expectedData;
  }
}
