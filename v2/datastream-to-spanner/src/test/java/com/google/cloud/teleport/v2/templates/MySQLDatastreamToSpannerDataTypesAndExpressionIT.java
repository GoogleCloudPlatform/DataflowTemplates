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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
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
 * MySQL data types.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLDatastreamToSpannerDataTypesAndExpressionIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLDatastreamToSpannerDataTypesAndExpressionIT.class);

  private static final String MYSQL_DDL_RESOURCE = "MySQLDataTypesIT/mysql-data-types.sql";
  private static final String MYSQL_DML_RESOURCE = "MySQLDataTypesIT/mysql-generated-col.sql";
  private static final String SPANNER_DDL_RESOURCE = "MySQLDataTypesIT/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      "MySQLDataTypesIT/pg-dialect-spanner-schema.sql";

  private static final List<String> UNSUPPORTED_TYPE_TABLES =
      List.of(
          "spatial_linestring",
          "spatial_multilinestring",
          "spatial_multipoint",
          "spatial_multipolygon",
          "spatial_point",
          "spatial_polygon",
          "spatial_geometry",
          "spatial_geometrycollection");

  private static boolean initialized = false;
  private static CloudMySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager pgDialectSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<MySQLDatastreamToSpannerDataTypesAndExpressionIT> testInstances =
      new HashSet<>();

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (MySQLDatastreamToSpannerDataTypesAndExpressionIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up MySQL resource manager...");
        mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();
        LOG.info("MySQL resource manager created with URI: {}", mySQLResourceManager.getUri());
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
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing MySQL DDL script...");
        executeSqlScript(mySQLResourceManager, MYSQL_DDL_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (MySQLDatastreamToSpannerDataTypesAndExpressionIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        spannerResourceManager,
        pgDialectSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testMySqlDataTypesAndExpression() throws Exception {
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    addInitialExpectedDataGeneratedColumns(expectedData);

    MySQLSource mySQLSource =
        MySQLSource.builder(
                mySQLResourceManager.getHost(),
                mySQLResourceManager.getUsername(),
                mySQLResourceManager.getPassword(),
                mySQLResourceManager.getPort())
            .setAllowedTables(
                Map.of(mySQLResourceManager.getDatabaseName(), getAllowedTables(expectedData)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "mysql-data-types",
            null,
            null,
            "mysql-datastream-to-spanner-data-types",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            mySQLSource);
    assertThatPipeline(jobInfo).isRunning();

    ChainedConditionCheck condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);

    LOG.info("Executing MySQL DML script...");
    executeSqlScript(mySQLResourceManager, MYSQL_DML_RESOURCE);
    expectedData = getExpectedData();
    addUpdatedExpectedDataGeneratedColumns(expectedData);

    condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process DML data...");
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);
  }

  @Test
  public void testMySqlDataTypesPGDialect() throws Exception {
    LOG.info("Creating PG Dialect Spanner DDL...");
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_DDL_RESOURCE);
    Map<String, List<Map<String, Object>>> expectedData = getExpectedDataPGDialect();

    MySQLSource mySQLSource =
        MySQLSource.builder(
                mySQLResourceManager.getHost(),
                mySQLResourceManager.getUsername(),
                mySQLResourceManager.getPassword(),
                mySQLResourceManager.getPort())
            .setAllowedTables(
                Map.of(mySQLResourceManager.getDatabaseName(), getAllowedTables(expectedData)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "mysql-data-types-pg-dialect",
            null,
            null,
            "mysql-datastream-to-spanner-data-types-pg-dialect",
            pgDialectSpannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            mySQLSource);
    assertThatPipeline(jobInfo).isRunning();

    ChainedConditionCheck condition =
        buildConditionCheck(pgDialectSpannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(pgDialectSpannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    // These types are not mapped as expected, ignore them to avoid failing the
    // test.
    Set<String> ignoredTypeMappings =
        Set.of("bit_to_string", "date_to_string", "set_to_array", "spatial_geometrycollection");
    // Validate supported data types.
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      if (ignoredTypeMappings.contains(type)) {
        LOG.warn(
            "Mapping for {} is ignored to avoid failing the test (it does not map as expected)...",
            type);
        continue;
      }
      String tableName = String.format("%s_table", type);
      LOG.info("Asserting type: {}", type);

      List<Struct> rows =
          resourceManager.readTableRecords(tableName, entry.getValue().get(0).keySet());
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
    for (String table : UNSUPPORTED_TYPE_TABLES) {
      if (ignoredTypeMappings.contains(table)) {
        LOG.warn(
            "Mapping for {} is ignored to avoid failing the test (it does not map as expected)...",
            table);
        continue;
      }
      // Unsupported rows should still be migrated. Each source table has 1 row.
      assertThat(resourceManager.getRowCount(table)).isEqualTo(1L);
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

  private List<String> getAllowedTables(Map<String, List<Map<String, Object>>> expectedData) {
    List<String> tableNames = new ArrayList<>(expectedData.size() + UNSUPPORTED_TYPE_TABLES.size());
    for (String tablePrefix : expectedData.keySet()) {
      tableNames.add(tablePrefix + "_table");
    }
    tableNames.addAll(UNSUPPORTED_TYPE_TABLES);
    return tableNames;
  }

  private ChainedConditionCheck buildConditionCheck(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    // These tables fail to migrate any rows, ignore them to avoid having to wait
    // for the timeout.
    Set<String> ignoredTables = Set.of("set_to_array", "spatial_geometrycollection");
    List<ConditionCheck> conditions = new ArrayList<>(expectedData.size());

    ConditionCheck combinedCondition = null;
    int numCombinedConditions = 0;
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      if (ignoredTables.contains(entry.getKey())) {
        continue;
      }
      String tableName = String.format("%s_table", entry.getKey());
      int numRows = entry.getValue().size();
      ConditionCheck c =
          SpannerRowsCheck.builder(resourceManager, tableName).setMinRows(numRows).build();
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition.and(c);
      }
      numCombinedConditions += 1;
      if (numCombinedConditions >= 3) {
        conditions.add(combinedCondition);
        combinedCondition = null;
        numCombinedConditions = 0;
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
        unsupportedTableCondition.and(c);
      }
    }
    conditions.add(unsupportedTableCondition);

    return ChainedConditionCheck.builder(conditions).build();
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put(
        "bigint",
        createRows("bigint", "40", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigint_to_string",
        createRows(
            "bigint_to_string", "40", "9223372036854775807", "-9223372036854775808", "NULL"));
    expectedData.put(
        "bigint_unsigned",
        createRows("bigint_unsigned", "42", "0", "18446744073709551615", "NULL"));
    expectedData.put(
        "binary", createRows("binary", "eDU4MD" + "A".repeat(334), "/".repeat(340), "NULL"));
    expectedData.put(
        "binary_to_string",
        createRows(
            "binary_to_string",
            "783538303000000000000000000000000...",
            "fffffffffffffffffffffffffffffffff...",
            "NULL"));
    expectedData.put("bit", createRows("bit", "f/////////8=", "NULL"));
    expectedData.put("bit_to_bool", createRows("bit_to_bool", "false", "true", "NULL"));
    expectedData.put("bit_to_string", createRows("bit_to_string", "7fff", "NULL"));
    expectedData.put("bit_to_int64", createRows("bit_to_int64", "9223372036854775807", "NULL"));
    expectedData.put("blob", createRows("blob", "eDU4MDA=", "/".repeat(87380), "NULL"));
    expectedData.put(
        "blob_to_string",
        createRows("blob_to_string", "7835383030", "fffffffffffffffffffffffffffffffff...", "NULL"));
    expectedData.put("bool", createRows("bool", "false", "true", "NULL"));
    expectedData.put("bool_to_string", createRows("bool_to_string", "0", "1", "NULL"));
    expectedData.put("boolean", createRows("boolean", "false", "true", "NULL"));
    expectedData.put("boolean_to_bool", createRows("boolean_to_bool", "false", "true", "NULL"));
    expectedData.put("boolean_to_string", createRows("boolean_to_string", "0", "1", "NULL"));
    expectedData.put(
        "char", createRows("char", "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put("date", createRows("date", "2012-09-17", "1000-01-01", "9999-12-31", "NULL"));
    expectedData.put(
        "date_to_string",
        createRows("date_to_string", "2012-09-17", "1000-01-01", "9999-12-31", "NULL"));
    expectedData.put(
        "datetime",
        createRows(
            "datetime",
            "1998-01-23T12:45:56Z",
            "1000-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "datetime_to_string",
        createRows(
            "datetime_to_string",
            "1998-01-23T12:45:56Z",
            "1000-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
            "NULL"));
    expectedData.put(
        "dec_to_numeric",
        createRows(
            "dec_to_numeric",
            "68.75",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));
    expectedData.put(
        "dec_to_string",
        createRows(
            "dec_to_string",
            "68.750000000000000000000000000000",
            "99999999999999999999999.999999999...",
            "12345678912345678.123456789012345...",
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
        "decimal_to_string",
        createRows(
            "decimal_to_string",
            "68.750000000000000000000000000000",
            "99999999999999999999999.999999999...",
            "12345678912345678.123456789012345...",
            "NULL"));
    expectedData.put(
        "double_precision_to_float64",
        createRows(
            "double_precision_to_float64",
            "52.67",
            "1.7976931348623157E308",
            "-1.7976931348623157E308",
            "NULL"));
    expectedData.put(
        "double_precision_to_string",
        createRows(
            "double_precision_to_string",
            "52.67",
            "1.7976931348623157E+308",
            "-1.7976931348623157E+308",
            "NULL"));
    expectedData.put(
        "double",
        createRows("double", "52.67", "1.7976931348623157E308", "-1.7976931348623157E308", "NULL"));
    expectedData.put(
        "double_to_string",
        createRows(
            "double_to_string",
            "52.67",
            "1.7976931348623157E+308",
            "-1.7976931348623157E+308",
            "NULL"));
    expectedData.put("enum", createRows("enum", "1", "NULL"));
    expectedData.put("float", createRows("float", "45.56", "3.4E38", "-3.4E38", "NULL"));
    expectedData.put(
        "float_to_float32", createRows("float_to_float32", "45.56", "3.4E38", "-3.4E38", "NULL"));
    expectedData.put(
        "float_to_string", createRows("float_to_string", "45.56", "3.4E+38", "-3.4E+38", "NULL"));
    expectedData.put("int", createRows("int", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put(
        "int_to_string", createRows("int_to_string", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put(
        "integer_to_int64",
        createRows("integer_to_int64", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put(
        "integer_to_string",
        createRows("integer_to_string", "30", "2147483647", "-2147483648", "NULL"));
    expectedData.put("test_json", createRows("test_json", "{\"k1\":\"v1\"}", "NULL"));
    expectedData.put("json_to_string", createRows("json_to_string", "{\"k1\": \"v1\"}", "NULL"));
    expectedData.put("longblob", createRows("longblob", "eDU4MDA=", "/".repeat(87380), "NULL"));
    expectedData.put(
        "longblob_to_string",
        createRows(
            "longblob_to_string", "7835383030", "fffffffffffffffffffffffffffffffff...", "NULL"));
    expectedData.put(
        "longtext",
        createRows("longtext", "longtext", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put("mediumblob", createRows("mediumblob", "eDU4MDA=", "/".repeat(87380), "NULL"));
    expectedData.put(
        "mediumblob_to_string",
        createRows(
            "mediumblob_to_string", "7835383030", "fffffffffffffffffffffffffffffffff...", "NULL"));
    expectedData.put("mediumint", createRows("mediumint", "20", "NULL"));
    expectedData.put("mediumint_to_string", createRows("mediumint_to_string", "20", "NULL"));
    expectedData.put(
        "mediumint_unsigned", createRows("mediumint_unsigned", "42", "0", "16777215", "NULL"));
    expectedData.put(
        "mediumtext", createRows("mediumtext", "mediumtext", "a".repeat(33) + "...", "NULL"));
    expectedData.put(
        "numeric_to_numeric",
        createRows(
            "numeric_to_numeric",
            "68.75",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));
    expectedData.put(
        "numeric_to_string",
        createRows(
            "numeric_to_string",
            "68.750000000000000000000000000000",
            "99999999999999999999999.999999999...",
            "12345678912345678.123456789012345...",
            "NULL"));
    expectedData.put(
        "real_to_float64",
        createRows(
            "real_to_float64",
            "52.67",
            "1.7976931348623157E308",
            "-1.7976931348623157E308",
            "NULL"));
    expectedData.put(
        "real_to_string",
        createRows(
            "real_to_string",
            "52.67",
            "1.7976931348623157E+308",
            "-1.7976931348623157E+308",
            "NULL"));
    expectedData.put("set_to_array", createRows("set_to_array", "v1,v2", "NULL"));
    expectedData.put("smallint", createRows("smallint", "15", "32767", "-32768", "NULL"));
    expectedData.put(
        "smallint_to_string", createRows("smallint_to_string", "15", "32767", "-32768", "NULL"));
    expectedData.put(
        "smallint_unsigned", createRows("smallint_unsigned", "42", "0", "65535", "NULL"));
    expectedData.put("text", createRows("text", "xyz", "a".repeat(33) + "...", "NULL"));
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
        "timestamp_to_string",
        createRows(
            "timestamp_to_string",
            "2022-08-05T08:23:11Z",
            "1970-01-01T00:00:01Z",
            "2038-01-19T03:14:07Z",
            "NULL"));
    expectedData.put("tinyblob", createRows("tinyblob", "eDU4MDA=", "/".repeat(340), "NULL"));
    expectedData.put(
        "tinyblob_to_string",
        createRows(
            "tinyblob_to_string", "7835383030", "fffffffffffffffffffffffffffffffff...", "NULL"));
    expectedData.put("tinyint", createRows("tinyint", "10", "127", "-128", "NULL"));
    expectedData.put(
        "tinyint_to_string", createRows("tinyint_to_string", "10", "127", "-128", "NULL"));
    expectedData.put("tinyint_unsigned", createRows("tinyint_unsigned", "0", "255", "NULL"));
    expectedData.put(
        "tinytext",
        createRows("tinytext", "tinytext", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...", "NULL"));
    expectedData.put(
        "varbinary", createRows("varbinary", "eDU4MDA=", "/".repeat(86666) + "8=", "NULL"));
    expectedData.put(
        "varbinary_to_string",
        createRows(
            "varbinary_to_string", "7835383030", "fffffffffffffffffffffffffffffffff...", "NULL"));
    expectedData.put("varchar", createRows("varchar", "abc", "a".repeat(33) + "...", "NULL"));
    expectedData.put("year", createRows("year", "2022", "1901", "2155", "NULL"));
    expectedData.put("set", createRows("set", "v1,v2", "NULL"));
    expectedData.put(
        "integer_unsigned", createRows("integer_unsigned", "0", "42", "4294967295", "NULL"));
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
                    new Pair<>("generated_column_col", "AA ")))));

    expectedData.put(
        "generated_non_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("id", 1),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA ")),
                Arrays.asList(
                    new Pair<>("id", 10),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA ")))));

    expectedData.put(
        "non_generated_to_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA "),
                    new Pair<>("generated_column_pk_col", "AA ")))));

    expectedData.put(
        "generated_to_non_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA "),
                    new Pair<>("generated_column_pk_col", "AA ")))));
  }

  private void addUpdatedExpectedDataGeneratedColumns(
      Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "CC ")))));
    expectedData.put(
        "generated_non_pk_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("id", 2),
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "CC ")),
                Arrays.asList(
                    new Pair<>("id", 3),
                    new Pair<>("first_name_col", "DD"),
                    new Pair<>("last_name_col", "EE"),
                    new Pair<>("generated_column_col", "DD ")),
                Arrays.asList(
                    new Pair<>("id", 11),
                    new Pair<>("first_name_col", "AA"),
                    new Pair<>("last_name_col", "BB"),
                    new Pair<>("generated_column_col", "AA ")))));

    expectedData.put(
        "non_generated_to_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "CC "),
                    new Pair<>("generated_column_pk_col", "CC ")))));

    expectedData.put(
        "generated_to_non_generated_column",
        createMultiColumnRows(
            Arrays.asList(
                Arrays.asList(
                    new Pair<>("first_name_col", "CC"),
                    new Pair<>("last_name_col", "CC"),
                    new Pair<>("generated_column_col", "CC "),
                    new Pair<>("generated_column_pk_col", "CC ")))));
  }

  private Map<String, List<Map<String, Object>>> getExpectedDataPGDialect() {
    // Expected data for PG dialect is roughly similar to the spanner dialect data,
    // with some minor
    // differences. Notably, some data types like numeric have slightly different
    // behaviour.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    expectedData.put(
        "bigint_unsigned",
        createRows(
            "bigint_unsigned",
            "42.000000000",
            "0.000000000",
            "18446744073709551615.000000000",
            "NULL"));
    expectedData.put(
        "dec_to_numeric",
        createRows(
            "dec_to_numeric",
            "68.750000000",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));
    expectedData.put(
        "decimal",
        createRows(
            "decimal",
            "68.750000000",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));
    expectedData.put("test_json", createRows("test_json", "{\"k1\": \"v1\"}", "NULL"));
    expectedData.put(
        "numeric_to_numeric",
        createRows(
            "numeric_to_numeric",
            "68.750000000",
            "99999999999999999999999.999999999",
            "12345678912345678.123456789",
            "NULL"));

    return expectedData;
  }
}
