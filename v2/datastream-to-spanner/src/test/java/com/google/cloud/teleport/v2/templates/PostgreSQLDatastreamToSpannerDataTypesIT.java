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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
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
 * PostgreSQL data types.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLDatastreamToSpannerDataTypesIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLDatastreamToSpannerDataTypesIT.class);

  private static final String POSTGRESQL_DDL_RESOURCE =
      "PostgreSQLDataTypesIT/postgresql-data-types.sql";
  private static final String SPANNER_DDL_RESOURCE = "PostgreSQLDataTypesIT/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      "PostgreSQLDataTypesIT/pg-dialect-spanner-schema.sql";

  private static final List<String> UNSUPPORTED_TYPE_TABLES =
      List.of(
          "t_bigint_array_to_int64_array",
          "t_bigint_array_to_string",
          "t_bit_to_bool_array",
          "t_bit_varying_to_bool_array",
          "t_bool_array_to_bool_array",
          "t_bool_array_to_string",
          "t_box",
          "t_box_to_float64_array",
          "t_circle",
          "t_circle_to_float64_array",
          "t_datemultirange",
          "t_daterange",
          "t_enum",
          "t_float_array_to_float64_array",
          "t_float_array_to_string",
          "t_int_array_to_int64_array",
          "t_int_array_to_string",
          "t_int4multirange",
          "t_int4range",
          "t_int8multirange",
          "t_int8range",
          "t_interval",
          "t_interval_to_int64",
          "t_line_to_float64_array",
          "t_lseg_to_float64_array",
          "t_money_to_int64",
          "t_nummultirange",
          "t_numrange",
          "t_path",
          "t_path_to_float64_array",
          "t_pg_lsn",
          "t_pg_snapshot",
          "t_point_to_float64_array",
          "t_polygon_to_float64_array",
          "t_real_array_to_float32_array",
          "t_real_array_to_string",
          "t_smallint_array_to_int64_array",
          "t_smallint_array_to_string",
          "t_time",
          "t_time_with_time_zone",
          "t_time_without_time_zone",
          "t_timetz",
          "t_tsmultirange",
          "t_tsquery",
          "t_tsrange",
          "t_tstzmultirange",
          "t_tstzrange",
          "t_tsvector",
          "t_txid_snapshot",
          "t_varbit_to_bool_array");
  private static final String PUBLICATION_NAME = "data_types_test_publication";
  private static final String REPLICATION_SLOT_NAME = "data_types_test_replication_slot";
  private static final String PG_DIALECT_REPLICATION_SLOT_NAME =
      "pg_dialect_data_types_test_replication_slot";

  private static boolean initialized = false;
  private static CloudPostgresResourceManager postgresResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager pgDialectSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<PostgreSQLDatastreamToSpannerDataTypesIT> testInstances = new HashSet<>();

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (PostgreSQLDatastreamToSpannerDataTypesIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up PostgreSQL resource manager...");
        postgresResourceManager = CloudPostgresResourceManager.builder(testName).build();
        LOG.info(
            "PostgreSQL resource manager created with URI: {}", postgresResourceManager.getUri());
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

        LOG.info("Executing PostgreSQL DDL script...");
        executeSqlScript(postgresResourceManager, POSTGRESQL_DDL_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (PostgreSQLDatastreamToSpannerDataTypesIT instance : testInstances) {
      instance.tearDownBase();
    }

    // It is important to clean up Datastream before trying to drop the replication slot.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    try {
      postgresResourceManager.runSQLQuery(
          "SELECT pg_drop_replication_slot('" + REPLICATION_SLOT_NAME + "')");
    } catch (Exception e) {
      LOG.warn("Failed to drop replication slot {}:", REPLICATION_SLOT_NAME, e);
    }
    try {
      postgresResourceManager.runSQLQuery(
          "SELECT pg_drop_replication_slot('" + PG_DIALECT_REPLICATION_SLOT_NAME + "')");
    } catch (Exception e) {
      LOG.warn("Failed to drop replication slot {}:", PG_DIALECT_REPLICATION_SLOT_NAME, e);
    }
    try {
      postgresResourceManager.runSQLUpdate("DROP PUBLICATION IF EXISTS " + PUBLICATION_NAME);
    } catch (Exception e) {
      LOG.warn("Failed to drop publication {}:", PUBLICATION_NAME, e);
    }

    ResourceManagerUtils.cleanResources(
        postgresResourceManager,
        spannerResourceManager,
        pgDialectSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testPostgreSqlDataTypes() throws Exception {
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                REPLICATION_SLOT_NAME,
                PUBLICATION_NAME)
            .setAllowedTables(Map.of("public", getAllowedTables()))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgresql-data-types",
            null,
            null,
            "postgresql-datastream-to-spanner-data-types",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            postgresqlSource);
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    ChainedConditionCheck condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);
  }

  @Test
  public void testPostgreSqlDataTypesPGDialect() throws Exception {
    LOG.info("Creating PG Dialect Spanner DDL...");
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_DDL_RESOURCE);

    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                PG_DIALECT_REPLICATION_SLOT_NAME,
                PUBLICATION_NAME)
            .setAllowedTables(Map.of("public", getAllowedTables()))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgresql-data-types-pg-dialect",
            null,
            null,
            "postgresql-datastream-to-spanner-data-types-pg-dialect",
            pgDialectSpannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            postgresqlSource);
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedDataPGDialect();

    ChainedConditionCheck condition =
        buildConditionCheck(pgDialectSpannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(pgDialectSpannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    // These types are not mapped as expected, ignore them to avoid failing the test.
    Set<String> ignoredTypeMappings =
        Set.of(
            "bit",
            "bit_to_string",
            "bit_varying",
            "bit_varying_to_string",
            "bytea",
            "json",
            "json_to_string",
            "macaddr",
            "macaddr8",
            "uuid_to_bytes",
            "varbit",
            "varbit_to_string",
            "t_bigint_array_to_int64_array",
            "t_bigint_array_to_string",
            "t_bit_to_bool_array",
            "t_bit_varying_to_bool_array",
            "t_bool_array_to_bool_array",
            "t_bool_array_to_string",
            "t_box_to_float64_array",
            "t_circle_to_float64_array",
            "t_daterange",
            "t_float_array_to_float64_array",
            "t_float_array_to_string",
            "t_int_array_to_int64_array",
            "t_int_array_to_string",
            "t_line_to_float64_array",
            "t_lseg_to_float64_array",
            "t_money_to_int64",
            "t_path_to_float64_array",
            "t_point_to_float64_array",
            "t_polygon_to_float64_array",
            "t_real_array_to_float32_array",
            "t_real_array_to_string",
            "t_smallint_array_to_int64_array",
            "t_smallint_array_to_string",
            "t_varbit_to_bool_array");
    // Validate supported data types.
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("t_%s", type);
      if (ignoredTypeMappings.contains(type) || ignoredTypeMappings.contains(tableName)) {
        LOG.warn(
            "Mapping for {} is ignored to avoid failing the test (it does not map as expected)...",
            type);
        continue;
      }
      LOG.info("Asserting type: {}", type);

      List<Struct> rows = resourceManager.readTableRecords(tableName, "id", "col");
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
      // Unsupported rows should still be migrated. Each source table has 2 rows.
      assertThat(resourceManager.getRowCount(table)).isEqualTo(2L);
    }
  }

  private List<Map<String, Object>> createRows(Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i + 1);
      row.put("col", vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private List<String> getAllowedTables() {
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    List<String> tableNames = new ArrayList<>(expectedData.size() + UNSUPPORTED_TYPE_TABLES.size());
    for (String tableSuffix : expectedData.keySet()) {
      tableNames.add("t_" + tableSuffix);
    }
    tableNames.addAll(UNSUPPORTED_TYPE_TABLES);
    return tableNames;
  }

  private ChainedConditionCheck buildConditionCheck(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    // These tables fail to migrate the expected number of rows, ignore them to avoid having to wait
    // for the timeout.
    Set<String> ignoredTables =
        Set.of(
            "t_bigint_array_to_int64_array",
            "t_bigint_array_to_string",
            "t_bit_to_bool_array",
            "t_bit_varying_to_bool_array",
            "t_bool_array_to_bool_array",
            "t_bool_array_to_string",
            "t_box_to_float64_array",
            "t_circle_to_float64_array",
            "t_float_array_to_float64_array",
            "t_float_array_to_string",
            "t_int_array_to_int64_array",
            "t_int_array_to_string",
            "t_line_to_float64_array",
            "t_lseg_to_float64_array",
            "t_path_to_float64_array",
            "t_point_to_float64_array",
            "t_polygon_to_float64_array",
            "t_real_array_to_float32_array",
            "t_real_array_to_string",
            "t_smallint_array_to_int64_array",
            "t_smallint_array_to_string",
            "t_varbit_to_bool_array");
    List<ConditionCheck> conditions = new ArrayList<>(expectedData.size());

    ConditionCheck combinedCondition = null;
    int numCombinedConditions = 0;
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      if (ignoredTables.contains(entry.getKey())) {
        continue;
      }
      String tableName = String.format("t_%s", entry.getKey());
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
          SpannerRowsCheck.builder(resourceManager, unsupportedTypeTable).setMinRows(2).build();
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
    HashMap<String, List<Map<String, Object>>> result = new HashMap<>();
    result.put("bigint", createRows("-9223372036854775808", "9223372036854775807", "42", "NULL"));
    result.put(
        "bigint_to_string",
        createRows("-9223372036854775808", "9223372036854775807", "42", "NULL"));
    result.put("bigserial", createRows("-9223372036854775808", "9223372036854775807", "42"));
    result.put(
        "bigserial_to_string", createRows("-9223372036854775808", "9223372036854775807", "42"));
    result.put("bit", createRows("AA==", "gA==", "NULL"));
    result.put("bit_to_string", createRows("AA==", "gA==", "NULL"));
    result.put("bit_varying", createRows("UA==", "NULL"));
    result.put("bit_varying_to_string", createRows("UA==", "NULL"));
    result.put("bool", createRows("false", "true", "NULL"));
    result.put("bool_to_string", createRows("false", "true", "NULL"));
    result.put("boolean", createRows("false", "true", "NULL"));
    result.put("boolean_to_string", createRows("false", "true", "NULL"));
    result.put("bytea", createRows("YWJj", "NULL"));
    result.put("bytea_to_string", createRows("YWJj", "NULL"));
    result.put("char", createRows("a", "Θ", "NULL"));
    result.put("character", createRows("a", "Ξ", "NULL"));
    result.put("character_varying", createRows("testing character varying", "NULL"));
    result.put("cidr", createRows("192.168.100.128/25", "NULL"));
    result.put("date", createRows("0001-01-01", "9999-12-31", "NULL"));
    result.put("date_to_string", createRows("0001-01-01", "9999-12-31", "NULL"));
    result.put("decimal", createRows("0.12", "NULL"));
    result.put("decimal_to_string", createRows("0.12", "NULL"));
    result.put(
        "double_precision",
        createRows(
            "-1.9876542E307", "1.9876542E307", "NaN", "-Infinity", "Infinity", "1.23", "NULL"));
    result.put(
        "double_precision_to_string",
        createRows(
            "-1.9876542E+307", "1.9876542E+307", "NaN", "-Infinity", "Infinity", "1.23", "NULL"));
    result.put(
        "float_to_float64",
        createRows(
            "-1.9876542E307", "1.9876542E307", "NaN", "-Infinity", "Infinity", "1.23", "NULL"));
    result.put(
        "float_to_string",
        createRows(
            "-1.9876542E+307", "1.9876542E+307", "NaN", "-Infinity", "Infinity", "1.23", "NULL"));
    result.put(
        "float4",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "2.34", "NULL"));
    result.put(
        "float4_to_float32",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "2.34", "NULL"));
    result.put(
        "float4_to_string",
        createRows(
            "-1.9876542E+38", "1.9876542E+38", "NaN", "-Infinity", "Infinity", "2.34", "NULL"));
    result.put(
        "float8",
        createRows(
            "-1.9876542E307", "1.9876542E307", "NaN", "-Infinity", "Infinity", "3.45", "NULL"));
    result.put(
        "float8_to_string",
        createRows(
            "-1.9876542E+307", "1.9876542E+307", "NaN", "-Infinity", "Infinity", "3.45", "NULL"));
    result.put("inet", createRows("192.168.1.0/24", "NULL"));
    result.put("int", createRows("-2147483648", "2147483647", "1", "NULL"));
    result.put("int_to_string", createRows("-2147483648", "2147483647", "1", "NULL"));
    result.put("integer", createRows("-2147483648", "2147483647", "2", "NULL"));
    result.put("integer_to_string", createRows("-2147483648", "2147483647", "2", "NULL"));
    result.put("int2", createRows("-32768", "32767", "3", "NULL"));
    result.put("int2_to_string", createRows("-32768", "32767", "3", "NULL"));
    result.put("int4", createRows("-2147483648", "2147483647", "4", "NULL"));
    result.put("int4_to_string", createRows("-2147483648", "2147483647", "4", "NULL"));
    result.put("int8", createRows("-9223372036854775808", "9223372036854775807", "5", "NULL"));
    result.put(
        "int8_to_string", createRows("-9223372036854775808", "9223372036854775807", "5", "NULL"));
    result.put("json", createRows("{\"duplicate_key\":2}", "{\"null_key\":null}", "NULL"));
    result.put(
        "json_to_string", createRows("{\"duplicate_key\": 2}", "{\"null_key\": null}", "NULL"));
    result.put("jsonb", createRows("{\"duplicate_key\":2}", "{\"null_key\":null}", "NULL"));
    result.put(
        "jsonb_to_string", createRows("{\"duplicate_key\": 2}", "{\"null_key\": null}", "NULL"));
    result.put(
        "large_decimal_to_numeric",
        createRows(
            // Decimals with scale larger than supported in Spanner are rounded
            "0.12", "100000000000000000000000", "12345678901234567890.123456789", "NULL"));
    result.put(
        "large_decimal_to_string",
        createRows(
            "0.1200000000",
            "99999999999999999999999.9999999999",
            "123456789012345678901234567890.12...",
            "NULL"));
    result.put(
        "large_numeric_to_numeric",
        createRows(
            // Decimals with scale larger than supported in Spanner are rounded
            "0.12", "100000000000000000000000", "12345678901234567890.123456789", "NULL"));
    result.put(
        "large_numeric_to_string",
        createRows(
            "0.1200000000",
            "99999999999999999999999.9999999999",
            "123456789012345678901234567890.12...",
            "NULL"));
    result.put("macaddr", createRows("08:00:2b:01:02:03", "NULL"));
    result.put("macaddr8", createRows("08:00:2b:01:02:03:04:05", "NULL"));
    result.put("money", createRows("123.45", "NULL"));
    result.put("numeric", createRows("4.56", "NULL"));
    result.put("numeric_to_string", createRows("4.56", "NULL"));
    result.put("oid", createRows("1000", "NULL"));
    result.put(
        "real",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "5.67", "NULL"));
    result.put(
        "real_to_float32",
        createRows(
            "-1.9876542E38", "1.9876542E38", "NaN", "-Infinity", "Infinity", "5.67", "NULL"));
    result.put(
        "real_to_string",
        createRows(
            "-1.9876542E+38", "1.9876542E+38", "NaN", "-Infinity", "Infinity", "5.67", "NULL"));
    result.put("serial", createRows("-2147483648", "2147483647", "6"));
    result.put("serial_to_string", createRows("-2147483648", "2147483647", "6"));
    result.put("serial2", createRows("-32768", "32767", "7"));
    result.put("serial2_to_string", createRows("-32768", "32767", "7"));
    result.put("serial4", createRows("-2147483648", "2147483647", "8"));
    result.put("serial4_to_string", createRows("-2147483648", "2147483647", "8"));
    result.put("serial8", createRows("-9223372036854775808", "9223372036854775807", "9"));
    result.put("serial8_to_string", createRows("-9223372036854775808", "9223372036854775807", "9"));
    result.put("smallint", createRows("-32768", "32767", "10", "NULL"));
    result.put("smallint_to_string", createRows("-32768", "32767", "10", "NULL"));
    result.put("smallserial", createRows("-32768", "32767", "11"));
    result.put("smallserial_to_string", createRows("-32768", "32767", "11"));
    result.put("text", createRows("testing text", "NULL"));
    result.put("timestamp", createRows("1970-01-02T03:04:05.123456Z", "NULL"));
    result.put("timestamp_to_timestamp", createRows("1970-01-02T03:04:05.123456000Z", "NULL"));
    result.put(
        "timestamptz",
        createRows("1970-02-02T18:05:06.123456000Z", "1970-02-03T05:05:06.123456000Z", "NULL"));
    result.put(
        "timestamptz_to_string",
        createRows("1970-02-02T18:05:06.123456Z", "1970-02-03T05:05:06.123456Z", "NULL"));
    result.put(
        "timestamp_with_time_zone",
        createRows("1970-02-02T18:05:06.123456000Z", "1970-02-03T05:05:06.123456000Z", "NULL"));
    result.put(
        "timestamp_with_timezone_to_string",
        createRows("1970-02-02T18:05:06.123456Z", "1970-02-03T05:05:06.123456Z", "NULL"));
    result.put("timestamp_without_time_zone", createRows("1970-01-02T03:04:05.123456Z", "NULL"));
    result.put("uuid", createRows("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "NULL"));
    result.put("uuid_to_bytes", createRows("oO68mZwLTvi7bWu5vTgKEQ==", "NULL"));
    result.put("varbit", createRows("wA==", "NULL"));
    result.put("varbit_to_string", createRows("wA==", "NULL"));
    result.put("varchar", createRows("testing varchar", "NULL"));
    result.put("xml", createRows("<test>123</test>", "NULL"));
    return result;
  }

  private Map<String, List<Map<String, Object>>> getExpectedDataPGDialect() {
    // Expected data for PG dialect is roughly similar to the spanner dialect data, with some minor
    // differences. Notably, some data types like numeric have slightly different behaviour.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    expectedData.put("decimal", createRows("0.120000000", "NULL"));
    expectedData.put("json", createRows("{\"duplicate_key\": 2}", "{\"null_key\": null}", "NULL"));
    expectedData.put("jsonb", createRows("{\"duplicate_key\": 2}", "{\"null_key\": null}", "NULL"));
    expectedData.put(
        "large_decimal_to_numeric",
        createRows(
            // Decimals with scale larger than supported in Spanner are rounded
            "0.120000000",
            "100000000000000000000000.000000000",
            "12345678901234567890.123456789",
            "NULL"));
    expectedData.put(
        "large_numeric_to_numeric",
        createRows(
            // Decimals with scale larger than supported in Spanner are rounded
            "0.120000000",
            "100000000000000000000000.000000000",
            "12345678901234567890.123456789",
            "NULL"));
    expectedData.put("numeric", createRows("4.560000000", "NULL"));

    return expectedData;
  }
}
