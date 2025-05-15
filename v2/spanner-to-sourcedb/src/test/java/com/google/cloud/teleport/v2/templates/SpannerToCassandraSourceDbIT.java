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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.CASSANDRA_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDbIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToCassandraSourceDbIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToCassandraSourceIT/spanner-schema.sql";
  private static final String CASSANDRA_SCHEMA_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-schema.sql";
  private static final String CASSANDRA_CONFIG_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-config-template.conf";

  private static final String EMPTY_STRING_JSON_TABLE = "EmptyStringJsonTable";
  private static final String USER_TABLE = "Users";
  private static final String USER_TABLE_2 = "Users2";
  private static final String ALL_DATA_TYPES_TABLE = "AllDatatypeColumns";
  private static final String ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE = "AllDatatypeTransformation";
  private static final String BOUNDARY_CONVERSION_TABLE = "BoundaryConversionTestTable";
  private static final String BOUNDARY_SIZE_TABLE = "testtable_03tpcovf16ed0klxm3v808ch3btgq0uk";
  private static final HashSet<SpannerToCassandraSourceDbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraResourceManager cassandraResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;
  private final List<Throwable> assertionErrors = new ArrayList<>();

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToCassandraSourceDbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadCassandraConfigToGcs(
            gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                CASSANDRA_SOURCE_TYPE);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToCassandraSourceDbIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        cassandraResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToCassandraWithMaxColumnsAndTableName()
      throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowWithMaxColumnsNameAndTableInSpanner();
    assertRowWithMaxColumnsInCassandra();
  }

  private void writeRowWithMaxColumnsNameAndTableInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_SIZE_TABLE).set("id").to(1);
    mutationBuilder.set("col_qcbf69rmxtre3b_03tpcovf16ed").to("SampleTestValue");

    mutations.add(mutationBuilder.build());
    spannerResourceManager.write(mutations);
    LOG.info("Inserted row into Spanner using Mutations");
  }

  private void assertRowWithMaxColumnsInCassandra() {

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                () -> getRowCount(BOUNDARY_SIZE_TABLE) == 1);
    assertThatResult(result).meetsConditions();
    LOG.info("Successfully validated columns in Cassandra");
  }

  /**
   * Tests the data flow from Spanner to Cassandra.
   *
   * <p>This test ensures that a basic row is successfully written to Spanner and subsequently
   * appears in Cassandra, validating end-to-end data consistency.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   */
  @Test
  public void spannerToCasandraSourceDbBasic() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeBasicRowInSpanner();
    assertBasicRowInCassandraDB();
  }

  /**
   * Tests the data flow from Spanner to Cassandra.
   *
   * <p>This test ensures that a basic row is successfully deleted from Spanner and subsequently
   * deleted in Cassandra, validating end-to-end data consistency.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   */
  @Test
  public void spannerToCasandraSourceDbJSONEmptyOperation()
      throws InterruptedException, IOException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    writeJSONEmptyInSpanner();
    assertJSONEmptyRowInCassandraDB();
  }

  /** Insert Empty rows in Spanner. */
  private void writeJSONEmptyInSpanner() {
    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(EMPTY_STRING_JSON_TABLE)
            .set("varchar_column")
            .to("SampleVarchar")
            .set("empty_column")
            .to("")
            .set("double_float_map_col")
            .to(Value.json("{}"))
            .set("decimal_set_col")
            .to(Value.json("[]"))
            .set("date_double_map_col")
            .to(Value.json("{}"))
            .set("uuid_ascii_map_col")
            .to(Value.json("{}"))
            .set("ascii_text_map_col")
            .to(Value.json("{}"))
            .set("timestamp_list_col")
            .to(Value.json("[]"))
            .set("int_set_col")
            .to(Value.json("[]"))
            .set("smallint_set_col")
            .to(Value.json("[]"))
            .set("varchar_list_col")
            .to(Value.json("[]"))
            .set("inet_list_col")
            .to(Value.json("[]"))
            .set("bigint_list_col")
            .to(Value.json("[]"))
            .set("tinyint_varint_map_col")
            .to(Value.json("{}"))
            .set("text_set_col")
            .to(Value.json("[]"))
            .set("double_set_col")
            .to(Value.json("[]"))
            .set("time_list_col")
            .to(Value.json("[]"))
            .set("frozen_ascii_list_col")
            .to(Value.json("[]"))
            .set("int_list_col")
            .to(Value.json("[]"))
            .set("ascii_list_col")
            .to(Value.json("[]"))
            .set("date_set_col")
            .to(Value.json("[]"))
            .set("double_inet_map_col")
            .to(Value.json("{}"))
            .set("timestamp_set_col")
            .to(Value.json("[]"))
            .set("time_tinyint_map_col")
            .to(Value.json("{}"))
            .set("bigint_set_col")
            .to(Value.json("[]"))
            .set("varchar_set_col")
            .to(Value.json("[]"))
            .set("tinyint_set_col")
            .to(Value.json("[]"))
            .set("bigint_boolean_map_col")
            .to(Value.json("{}"))
            .set("text_list_col")
            .to(Value.json("[]"))
            .set("boolean_list_col")
            .to(Value.json("[]"))
            .set("blob_list_col")
            .to(Value.json("[]"))
            .set("timeuuid_set_col")
            .to(Value.json("[]"))
            .set("int_time_map_col")
            .to(Value.json("{}"))
            .set("time_set_col")
            .to(Value.json("[]"))
            .set("boolean_set_col")
            .to(Value.json("[]"))
            .set("float_set_col")
            .to(Value.json("[]"))
            .set("ascii_set_col")
            .to(Value.json("[]"))
            .set("uuid_list_col")
            .to(Value.json("[]"))
            .set("varchar_bigint_map_col")
            .to(Value.json("{}"))
            .set("blob_int_map_col")
            .to(Value.json("{}"))
            .set("varint_blob_map_col")
            .to(Value.json("{}"))
            .set("double_list_col")
            .to(Value.json("[]"))
            .set("float_list_col")
            .to(Value.json("[]"))
            .set("smallint_list_col")
            .to(Value.json("[]"))
            .set("varint_list_col")
            .to(Value.json("[]"))
            .set("float_smallint_map_col")
            .to(Value.json("{}"))
            .set("smallint_timestamp_map_col")
            .to(Value.json("{}"))
            .set("text_timeuuid_map_col")
            .to(Value.json("{}"))
            .set("timeuuid_list_col")
            .to(Value.json("[]"))
            .set("date_list_col")
            .to(Value.json("[]"))
            .set("uuid_set_col")
            .to(Value.json("[]"))
            .set("boolean_decimal_map_col")
            .to(Value.json("{}"))
            .set("blob_set_col")
            .to(Value.json("[]"))
            .set("inet_text_map_col")
            .to(Value.json("{}"))
            .set("varint_set_col")
            .to(Value.json("[]"))
            .set("tinyint_list_col")
            .to(Value.json("[]"))
            .set("timestamp_uuid_map_col")
            .to(Value.json("{}"))
            .set("decimal_duration_map_col")
            .to(Value.json("{}"))
            .set("decimal_list_col")
            .to(Value.json("[]"))
            .set("inet_set_col")
            .to(Value.json("[]"))
            .set("timeuuid_varchar_map_col")
            .to(Value.json("{}"))
            .set("duration_list_col")
            .to(Value.json("[]"))
            .set("frozen_ascii_set_col")
            .to(Value.json("[]"));

    Mutation mutation = mutationBuilder.build();
    spannerResourceManager.write(mutation);
  }

  /**
   * Asserts that Empty Record in the Cassandra database.
   *
   * @throws InterruptedException if the thread is interrupted while waiting for the row count
   *     condition.
   * @throws RuntimeException if reading from the Cassandra table fails.
   */
  private void assertJSONEmptyRowInCassandraDB()
      throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(EMPTY_STRING_JSON_TABLE) == 1);
    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(EMPTY_STRING_JSON_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to read from Cassandra table: " + EMPTY_STRING_JSON_TABLE, e);
    }

    assertThat(rows).hasSize(1);
    Row row = rows.iterator().next();
    LOG.info("[AssertJSONEmptyRowInCassandraDB] row: {}", row.getFormattedContents());

    assertAll(
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar"),
        () -> assertThat(row.getString("empty_column")).isEmpty(),
        () -> assertThat(row.getMap("double_float_map_col", Double.class, Float.class)).isEmpty(),
        () ->
            assertThat(row.getMap("date_double_map_col", LocalDate.class, Double.class)).isEmpty(),
        () -> assertThat(row.getMap("uuid_ascii_map_col", UUID.class, String.class)).isEmpty(),
        () -> assertThat(row.getMap("ascii_text_map_col", String.class, String.class)).isEmpty(),
        () ->
            assertThat(row.getMap("tinyint_varint_map_col", String.class, String.class)).isEmpty(),
        () -> assertThat(row.getMap("time_tinyint_map_col", String.class, Byte.class)).isEmpty(),
        () -> assertThat(row.getMap("bigint_boolean_map_col", Long.class, Boolean.class)).isEmpty(),
        () -> assertThat(row.getMap("varchar_bigint_map_col", String.class, Long.class)).isEmpty(),
        () -> assertThat(row.getMap("blob_int_map_col", ByteBuffer.class, Integer.class)).isEmpty(),
        () ->
            assertThat(row.getMap("varint_blob_map_col", String.class, ByteBuffer.class)).isEmpty(),
        () -> assertThat(row.getMap("float_smallint_map_col", Float.class, Short.class)).isEmpty(),
        () ->
            assertThat(row.getMap("smallint_timestamp_map_col", Short.class, Instant.class))
                .isEmpty(),
        () -> assertThat(row.getMap("text_timeuuid_map_col", String.class, UUID.class)).isEmpty(),
        () ->
            assertThat(row.getMap("inet_text_map_col", InetAddress.class, String.class)).isEmpty(),
        () -> assertThat(row.getMap("timestamp_uuid_map_col", String.class, UUID.class)).isEmpty(),
        () ->
            assertThat(row.getMap("boolean_decimal_map_col", Boolean.class, BigDecimal.class))
                .isEmpty(),
        () ->
            assertThat(row.getMap("decimal_duration_map_col", BigDecimal.class, String.class))
                .isEmpty(),
        () ->
            assertThat(row.getMap("double_inet_map_col", Double.class, InetAddress.class))
                .isEmpty(),
        () ->
            assertThat(row.getMap("timeuuid_varchar_map_col", UUID.class, String.class)).isEmpty(),
        () -> assertThat(row.getMap("int_time_map_col", Integer.class, String.class)).isEmpty(),
        () -> assertThat(row.getList("timestamp_list_col", Instant.class)).isEmpty(),
        () -> assertThat(row.getList("varchar_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("inet_list_col", InetAddress.class)).isEmpty(),
        () -> assertThat(row.getList("bigint_list_col", Long.class)).isEmpty(),
        () -> assertThat(row.getList("time_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("frozen_ascii_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("int_list_col", Integer.class)).isEmpty(),
        () -> assertThat(row.getList("ascii_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("date_list_col", LocalDate.class)).isEmpty(),
        () -> assertThat(row.getList("double_list_col", Double.class)).isEmpty(),
        () -> assertThat(row.getList("float_list_col", Float.class)).isEmpty(),
        () -> assertThat(row.getList("smallint_list_col", Short.class)).isEmpty(),
        () -> assertThat(row.getList("varint_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("text_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("boolean_list_col", Boolean.class)).isEmpty(),
        () -> assertThat(row.getList("blob_list_col", ByteBuffer.class)).isEmpty(),
        () -> assertThat(row.getList("timeuuid_list_col", UUID.class)).isEmpty(),
        () -> assertThat(row.getList("duration_list_col", String.class)).isEmpty(),
        () -> assertThat(row.getList("decimal_list_col", BigDecimal.class)).isEmpty(),
        () -> assertThat(row.getList("tinyint_list_col", Byte.class)).isEmpty(),
        () -> assertThat(row.getSet("decimal_set_col", BigDecimal.class)).isEmpty(),
        () -> assertThat(row.getSet("int_set_col", Integer.class)).isEmpty(),
        () -> assertThat(row.getSet("smallint_set_col", Short.class)).isEmpty(),
        () -> assertThat(row.getSet("text_set_col", String.class)).isEmpty(),
        () -> assertThat(row.getSet("double_set_col", Double.class)).isEmpty(),
        () -> assertThat(row.getSet("date_set_col", LocalDate.class)).isEmpty(),
        () -> assertThat(row.getSet("timestamp_set_col", String.class)).isEmpty(),
        () -> assertThat(row.getSet("bigint_set_col", Long.class)).isEmpty(),
        () -> assertThat(row.getSet("varchar_set_col", String.class)).isEmpty(),
        () -> assertThat(row.getSet("tinyint_set_col", Byte.class)).isEmpty(),
        () -> assertThat(row.getSet("boolean_set_col", Boolean.class)).isEmpty(),
        () -> assertThat(row.getSet("float_set_col", Float.class)).isEmpty(),
        () -> assertThat(row.getSet("ascii_set_col", String.class)).isEmpty(),
        () -> assertThat(row.getSet("uuid_set_col", UUID.class)).isEmpty(),
        () -> assertThat(row.getSet("varint_set_col", String.class)).isEmpty(),
        () -> assertThat(row.getSet("blob_set_col", ByteBuffer.class)).isEmpty(),
        () -> assertThat(row.getSet("inet_set_col", InetAddress.class)).isEmpty(),
        () -> assertThat(row.getSet("frozen_ascii_set_col", String.class)).isEmpty());
  }

  /**
   * Tests the data flow from Spanner to Cassandra.
   *
   * <p>This test ensures that a basic row is successfully deleted from Spanner and subsequently
   * deleted in Cassandra, validating end-to-end data consistency.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   */
  @Test
  public void spannerToCasandraSourceDbDeleteOperation() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeDeleteInSpanner();
    assertDeleteRowInCassandraDB();
  }

  /** De basic rows to multiple tables in Google Cloud Spanner. */
  private void writeDeleteInSpanner() {

    Mutation insertOrUpdateMutation =
        Mutation.newInsertOrUpdateBuilder(USER_TABLE_2)
            .set("id")
            .to(4)
            .set("full_name")
            .to("GG")
            .build();
    spannerResourceManager.write(insertOrUpdateMutation);

    KeySet allRows = KeySet.all();
    Mutation deleteAllMutation = Mutation.delete(USER_TABLE_2, allRows);
    spannerResourceManager.write(deleteAllMutation);
  }

  /**
   * Asserts that delete the Cassandra database.
   *
   * @throws InterruptedException if the thread is interrupted while waiting for the row count
   *     condition.
   * @throws RuntimeException if reading from the Cassandra table fails.
   */
  private void assertDeleteRowInCassandraDB() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(USER_TABLE_2) == 0);
    assertThatResult(result).meetsConditions();
  }

  /**
   * Tests the data type conversion from Spanner to Cassandra.
   *
   * <p>This test ensures that all supported data types are correctly written to Spanner and
   * subsequently retrieved from Cassandra, verifying data integrity and type conversions.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   * @throws MultipleFailureException if multiple assertions fail during validation.
   */
  @Test
  public void spannerToCassandraSourceAllDataTypeConversionTest()
      throws InterruptedException, IOException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    writeAllDataTypeRowsInSpanner();
    assertAllDataTypeRowsInCassandraDB();
  }

  /**
   * Tests the conversion of string data types from Spanner to actual data type in Cassandra.
   *
   * <p>This test ensures that string-based data types are correctly written to Spanner and
   * subsequently retrieved from Cassandra, verifying data integrity and conversion accuracy.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   * @throws MultipleFailureException if multiple assertions fail during validation.
   */
  @Test
  public void spannerToCassandraSourceDataTypeStringConversionTest()
      throws InterruptedException, IOException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    writeAllRowsAsStringInSpanner();
    assertStringToActualRowsInCassandraDB();
  }

  /**
   * Validates Boundary and Map Data Type Conversions from Spanner to Cassandra.
   *
   * <p>This test ensures that boundary values for various data types and their equivalent map data
   * types are correctly converted and transferred from Spanner to Cassandra. It verifies that the
   * string-based representations used in Spanner are accurately translated into their appropriate
   * data types in Cassandra, maintaining data integrity and precision.
   *
   * <p>The test involves inserting maximum and boundary data values into Spanner, then reading and
   * asserting the values from Cassandra to ensure consistent data conversion and integrity.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during test execution.
   * @throws MultipleFailureException if multiple assertions fail during validation.
   */
  @Test
  public void validateBoundaryAndMapDataConversionsBetweenSpannerAndCassandra()
      throws InterruptedException, IOException, MultipleFailureException {
    // Test Insert
    assertThatPipeline(jobInfo).isRunning();
    insertMaxBoundaryValuesIntoSpanner();
    insertMinBoundaryValuesIntoSpanner();
    assertCassandraBoundaryData();

    // Test Delete
    deleteBoundaryValuesInSpanner();
    assertCassandraAfterDelete();

    // Test Update
    insertMaxBoundaryValuesIntoSpanner();
    updateBoundaryValuesInSpanner();
    assertCassandraAfterUpdate();
  }

  /**
   * Retrieves the total row count of a specified table in Cassandra.
   *
   * <p>This method executes a `SELECT COUNT(*)` query on the given table and returns the number of
   * rows present in it.
   *
   * @param tableName the name of the table whose row count is to be retrieved.
   * @return the total number of rows in the specified table.
   * @throws RuntimeException if the query does not return a result.
   */
  private long getRowCount(String tableName) {
    String query = String.format("SELECT COUNT(*) FROM %s", tableName);
    ResultSet resultSet = cassandraResourceManager.executeStatement(query);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + tableName);
    }
  }

  private void writeBasicRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(USER_TABLE)
            .set("id")
            .to(1)
            .set("full_name")
            .to("A")
            .set("from")
            .to("B")
            .build();
    spannerResourceManager.write(m1);

    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder(USER_TABLE)
            .set("id")
            .to(2)
            .set("full_name")
            .to("BB")
            .build();
    spannerResourceManager.write(m2);
  }

  private void assertBasicRowInCassandraDB() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount(USER_TABLE) == 2);
    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      LOG.info("Reading from Cassandra table: {}", USER_TABLE);
      rows = cassandraResourceManager.readTable(USER_TABLE);
      LOG.info("Cassandra Rows: {}", rows.toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + USER_TABLE, e);
    }

    assertThat(rows).hasSize(2);

    for (Row row : rows) {
      LOG.info("Cassandra Row to Assert: {}", row.getFormattedContents());
      int id = row.getInt("id");
      if (id == 1) {
        assertThat(row.getString("full_name")).isEqualTo("A");
        assertThat(row.getString("from")).isEqualTo("B");
      } else if (id == 2) {
        assertThat(row.getString("full_name")).isEqualTo("BB");
      } else {
        throw new AssertionError("Unexpected row ID found: " + id);
      }
    }
  }

  /**
   * Writes a row containing all supported data types into the Spanner database.
   *
   * <p>This method creates and inserts a row into the {@code ALL_DATA_TYPES_TABLE} with various
   * data types, including text, numerical, date/time, boolean, byte arrays, lists, sets, and maps.
   * The values are set explicitly to ensure compatibility with Spanner's schema.
   *
   * @throws RuntimeException if writing to Spanner fails.
   */
  private void writeAllDataTypeRowsInSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(ALL_DATA_TYPES_TABLE)
            .set("varchar_column")
            .to("SampleVarchar")
            .set("tinyint_column")
            .to(127)
            .set("text_column")
            .to("This is some sample text data for the text column.")
            .set("date_column")
            .to(Value.date(Date.fromJavaUtilDate(java.sql.Date.valueOf("2025-01-27"))))
            .set("smallint_column")
            .to(32767)
            .set("mediumint_column")
            .to(8388607)
            .set("int_column")
            .to(2147483647)
            .set("bigint_column")
            .to(9223372036854775807L)
            .set("float_column")
            .to(3.14159)
            .set("double_column")
            .to(2.718281828459045)
            .set("decimal_column")
            .to(new BigDecimal("12345.6789"))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2025-01-27T10:30:00Z")))
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2025-01-27T10:30:00Z")))
            .set("time_column")
            .to("12:30:00")
            .set("year_column")
            .to("2025")
            .set("char_column")
            .to("CHAR_DATA")
            .set("tinytext_column")
            .to("Short text for tinytext.")
            .set("mediumtext_column")
            .to("Longer text data for mediumtext column.")
            .set("longtext_column")
            .to("Very long text data that exceeds the medium text column length for long text.")
            .set("enum_column")
            .to("OptionA")
            .set("bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("other_bool_column")
            .to(Value.bool(Boolean.FALSE))
            .set("bytes_column")
            .to(Value.bytes(ByteArray.copyFrom("Hello world")))
            .set("list_text_column")
            .to(Value.json("[\"apple\", \"banana\", \"cherry\"]"))
            .set("list_int_column")
            .to(Value.json("[1, 2, 3, 4, 5]"))
            .set("frozen_list_bigint_column")
            .to(Value.json("[123456789012345, 987654321012345]"))
            .set("set_text_column")
            .to(Value.json("[\"apple\", \"orange\", \"banana\"]"))
            .set("set_date_column")
            .to(Value.json("[\"2025-01-27\", \"2025-02-01\"]"))
            .set("frozen_set_bool_column")
            .to(Value.json("[true, false]"))
            .set("map_text_to_int_column")
            .to(Value.json("{\"key1\": 10, \"key2\": 20}"))
            .set("map_date_to_text_column")
            .to(Value.json("{\"2025-01-27\": \"event1\", \"2025-02-01\": \"event2\"}"))
            .set("frozen_map_int_to_bool_column")
            .to(Value.json("{\"1\": true, \"2\": false}"))
            .set("map_text_to_list_column")
            .to(Value.json("{\"fruit\": [\"apple\", \"banana\"], \"color\": [\"red\", \"green\"]}"))
            .set("map_text_to_set_column")
            .to(
                Value.json(
                    "{\"fruit\": [\"apple\", \"banana\"], \"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("frozen_map_text_to_list_column")
            .to(Value.json("{\"fruits\": [\"apple\", \"banana\"]}"))
            .set("frozen_map_text_to_set_column")
            .to(Value.json("{\"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("frozen_set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("frozen_list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("varint_column")
            .to("123456789")
            .set("inet_column")
            .to("192.168.1.10")
            .set("timeuuid_column")
            .to("f47ac10b-58cc-11e1-b86c-0800200c9a66")
            .set("duration_column")
            .to("P1DT2H3M4S")
            .set("uuid_column")
            .to("123e4567-e89b-12d3-a456-426614174000")
            .set("ascii_column")
            .to("SimpleASCIIText")
            .set("list_text_column_from_array")
            .to(Value.stringArray(ImmutableList.of("apple", "banana", "cherry")))
            .set("set_text_column_from_array")
            .to(Value.stringArray(ImmutableList.of("orange", "grape", "pear")))
            .build();

    spannerResourceManager.write(mutation);

    @SuppressWarnings({"rawtypes", "unchecked"})
    Mutation mutationAllNull =
        Mutation.newInsertOrUpdateBuilder(ALL_DATA_TYPES_TABLE)
            .set("varchar_column")
            .to("ForNull") // Only this column has a value
            .set("tinyint_column")
            .to(Value.int64(null))
            .set("text_column")
            .to(Value.string(null))
            .set("date_column")
            .to(Value.date(null))
            .set("smallint_column")
            .to(Value.int64(null))
            .set("mediumint_column")
            .to(Value.int64(null))
            .set("int_column")
            .to(Value.int64(null))
            .set("bigint_column")
            .to(Value.int64(null))
            .set("float_column")
            .to(Value.float64(null))
            .set("double_column")
            .to(Value.float64(null))
            .set("decimal_column")
            .to(Value.numeric(null))
            .set("datetime_column")
            .to(Value.timestamp(null))
            .set("timestamp_column")
            .to(Value.timestamp(null))
            .set("time_column")
            .to(Value.string(null))
            .set("year_column")
            .to(Value.string(null))
            .set("char_column")
            .to(Value.string(null))
            .set("tinytext_column")
            .to(Value.string(null))
            .set("mediumtext_column")
            .to(Value.string(null))
            .set("longtext_column")
            .to(Value.string(null))
            .set("enum_column")
            .to(Value.string(null))
            .set("bool_column")
            .to(Value.bool(null))
            .set("other_bool_column")
            .to(Value.bool(null))
            .set("bytes_column")
            .to(Value.bytes(null))
            .set("list_text_column")
            .to(Value.json(null))
            .set("list_int_column")
            .to(Value.json(null))
            .set("frozen_list_bigint_column")
            .to(Value.json(null))
            .set("set_text_column")
            .to(Value.json(null))
            .set("set_date_column")
            .to(Value.json(null))
            .set("frozen_set_bool_column")
            .to(Value.json(null))
            .set("map_text_to_int_column")
            .to(Value.json(null))
            .set("map_date_to_text_column")
            .to(Value.json(null))
            .set("frozen_map_int_to_bool_column")
            .to(Value.json(null))
            .set("map_text_to_list_column")
            .to(Value.json(null))
            .set("map_text_to_set_column")
            .to(Value.json(null))
            .set("set_of_maps_column")
            .to(Value.json(null))
            .set("list_of_sets_column")
            .to(Value.json(null))
            .set("frozen_map_text_to_list_column")
            .to(Value.json(null))
            .set("frozen_map_text_to_set_column")
            .to(Value.json(null))
            .set("frozen_set_of_maps_column")
            .to(Value.json(null))
            .set("frozen_list_of_sets_column")
            .to(Value.json(null))
            .set("varint_column")
            .to(Value.string(null))
            .set("inet_column")
            .to(Value.string(null))
            .set("timeuuid_column")
            .to(Value.string(null))
            .set("duration_column")
            .to(Value.string(null))
            .set("uuid_column")
            .to(Value.string(null))
            .set("ascii_column")
            .to(Value.string(null))
            .set("list_text_column_from_array")
            .to(Value.stringArray((java.util.List) null))
            .set("set_text_column_from_array")
            .to(Value.stringArray((java.util.List) null))
            .build();

    spannerResourceManager.write(mutationAllNull);

    Mutation mutationForInsertOrUpdatePrimaryKey =
        Mutation.newInsertOrUpdateBuilder(ALL_DATA_TYPES_TABLE)
            .set("varchar_column")
            .to("PKey")
            .build();

    spannerResourceManager.write(mutationForInsertOrUpdatePrimaryKey);
  }

  /**
   * Executes multiple assertions and collects all assertion failures.
   *
   * <p>This method takes a variable number of {@link Runnable} assertions and executes them
   * sequentially. If any assertions fail, their errors are collected, and a {@link
   * MultipleFailureException} is thrown containing all assertion errors.
   *
   * @param assertions One or more assertions provided as {@link Runnable} lambdas.
   * @throws MultipleFailureException if one or more assertions fail.
   */
  private void assertAll(Runnable... assertions) throws MultipleFailureException {
    for (Runnable assertion : assertions) {
      try {
        assertion.run();
      } catch (AssertionError e) {
        assertionErrors.add(e);
      }
    }
    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }

  private void assertAllDataTypeRowsInCassandraDB()
      throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(ALL_DATA_TYPES_TABLE) == 3);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(ALL_DATA_TYPES_TABLE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + ALL_DATA_TYPES_TABLE, e);
    }

    assertThat(rows).hasSize(3);
    for (Row row : rows) {
      LOG.info("Cassandra Row to Assert for All Data Types: {}", row.getFormattedContents());
      String varcharColumn = row.getString("varchar_column");
      if (Objects.equals(varcharColumn, "SampleVarchar")) {
        assertAll(
            () -> assertThat(row.getLong("bigint_column")).isEqualTo(9223372036854775807L),
            () -> assertThat(row.getBoolean("bool_column")).isTrue(),
            () -> assertThat(row.getString("char_column")).isEqualTo("CHAR_DATA"),
            () ->
                assertThat(row.getLocalDate("date_column"))
                    .isEqualTo(java.time.LocalDate.of(2025, 1, 27)),
            () ->
                assertThat(row.getInstant("datetime_column"))
                    .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00.000Z")),
            () ->
                assertThat(row.getBigDecimal("decimal_column"))
                    .isEqualTo(new BigDecimal("12345.6789")),
            () -> assertThat(row.getDouble("double_column")).isEqualTo(2.718281828459045),
            () -> assertThat(row.getFloat("float_column")).isEqualTo(3.14159f),

            // Collections (frozen, list, set, map)
            () ->
                assertThat(row.getList("frozen_list_bigint_column", Long.class))
                    .isEqualTo(Arrays.asList(123456789012345L, 987654321012345L)),
            () ->
                assertThat(row.getSet("frozen_set_bool_column", Boolean.class))
                    .isEqualTo(new HashSet<>(Arrays.asList(false, true))),
            () ->
                assertThat(
                        row.getMap("frozen_map_int_to_bool_column", Integer.class, Boolean.class))
                    .isEqualTo(Map.of(1, true, 2, false)),
            () ->
                assertThat(row.getMap("frozen_map_text_to_list_column", String.class, List.class))
                    .isEqualTo(Map.of("fruits", Arrays.asList("apple", "banana"))),
            () ->
                assertThat(row.getMap("frozen_map_text_to_set_column", String.class, Set.class))
                    .isEqualTo(
                        Map.of("vegetables", new HashSet<>(Arrays.asList("carrot", "spinach")))),
            () ->
                assertThat(row.getSet("frozen_set_of_maps_column", Map.class))
                    .isEqualTo(
                        new HashSet<>(
                            Arrays.asList(
                                Map.of("key1", 10, "key2", 20), Map.of("keyA", 5, "keyB", 10)))),

            // Lists and Sets
            () ->
                assertThat(row.getList("list_int_column", Integer.class))
                    .isEqualTo(Arrays.asList(1, 2, 3, 4, 5)),
            () ->
                assertThat(row.getList("list_text_column", String.class))
                    .isEqualTo(Arrays.asList("apple", "banana", "cherry")),
            () ->
                assertThat(row.getList("list_of_sets_column", Set.class))
                    .isEqualTo(
                        Arrays.asList(
                            new HashSet<>(Arrays.asList("apple", "banana")),
                            new HashSet<>(Arrays.asList("carrot", "spinach")))),

            // Maps
            () ->
                assertThat(
                        row.getMap(
                            "map_date_to_text_column", java.time.LocalDate.class, String.class))
                    .isEqualTo(
                        Map.of(
                            java.time.LocalDate.parse("2025-01-27"), "event1",
                            java.time.LocalDate.parse("2025-02-01"), "event2")),
            () ->
                assertThat(row.getMap("map_text_to_int_column", String.class, Integer.class))
                    .isEqualTo(Map.of("key1", 10, "key2", 20)),
            () ->
                assertThat(row.getMap("map_text_to_list_column", String.class, List.class))
                    .isEqualTo(
                        Map.of(
                            "color",
                            Arrays.asList("red", "green"),
                            "fruit",
                            Arrays.asList("apple", "banana"))),
            () ->
                assertThat(row.getMap("map_text_to_set_column", String.class, Set.class))
                    .isEqualTo(
                        Map.of(
                            "fruit",
                            new HashSet<>(Arrays.asList("apple", "banana")),
                            "vegetables",
                            new HashSet<>(Arrays.asList("carrot", "spinach")))),

            // Sets
            () ->
                assertThat(row.getSet("set_date_column", java.time.LocalDate.class))
                    .isEqualTo(
                        new HashSet<>(
                            Arrays.asList(
                                java.time.LocalDate.parse("2025-01-27"),
                                java.time.LocalDate.parse("2025-02-01")))),
            () ->
                assertThat(row.getSet("set_text_column", String.class))
                    .isEqualTo(new HashSet<>(Arrays.asList("apple", "orange", "banana"))),
            () ->
                assertThat(row.getSet("set_of_maps_column", Map.class))
                    .isEqualTo(
                        new HashSet<>(
                            Arrays.asList(
                                Map.of("key1", 10, "key2", 20), Map.of("keyA", 5, "keyB", 10)))),

            // Other Basic Types
            () -> assertThat(row.getShort("smallint_column")).isEqualTo((short) 32767),
            () -> assertThat(row.getInt("mediumint_column")).isEqualTo(8388607),
            () -> assertThat(row.getInt("int_column")).isEqualTo(2147483647),
            () -> assertThat(row.getString("enum_column")).isEqualTo("OptionA"),
            () -> assertThat(row.getString("year_column")).isEqualTo("2025"),
            () ->
                assertThat(row.getString("longtext_column"))
                    .isEqualTo(
                        "Very long text data that exceeds the medium text column length for long text."),
            () ->
                assertThat(row.getString("tinytext_column")).isEqualTo("Short text for tinytext."),
            () ->
                assertThat(row.getString("mediumtext_column"))
                    .isEqualTo("Longer text data for mediumtext column."),
            () ->
                assertThat(row.getString("text_column"))
                    .isEqualTo("This is some sample text data for the text column."),
            () ->
                assertThat(row.getLocalTime("time_column"))
                    .isEqualTo(java.time.LocalTime.parse("12:30:00.000000000")),
            () ->
                assertThat(row.getInstant("timestamp_column"))
                    .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00Z")),
            () ->
                assertThat(row.getBigInteger("varint_column"))
                    .isEqualTo(java.math.BigInteger.valueOf(123456789L)),
            () ->
                assertThat(row.getBytesUnsafe("bytes_column"))
                    .isEqualTo(ByteBuffer.wrap(ByteArray.copyFrom("Hello world").toByteArray())),
            () -> {
              InetAddress inetAddress = row.getInetAddress("inet_column");
              assertThat(inetAddress.getHostAddress()).isEqualTo("192.168.1.10");
            },
            () ->
                assertThat(row.getCqlDuration("duration_column"))
                    .isEqualTo(CqlDuration.from("P1DT2H3M4S")),
            () ->
                assertThat(row.getUuid("timeuuid_column"))
                    .isEqualTo(UUID.fromString("f47ac10b-58cc-11e1-b86c-0800200c9a66")),
            () ->
                assertThat(row.getUuid("uuid_column"))
                    .isEqualTo(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
            () -> assertThat(row.getString("ascii_column")).isEqualTo("SimpleASCIIText"),
            () ->
                assertThat(row.getList("list_text_column_from_array", String.class))
                    .isEqualTo(Arrays.asList("apple", "banana", "cherry")),
            () ->
                assertThat(row.getSet("set_text_column_from_array", String.class))
                    .isEqualTo(new HashSet<>(Arrays.asList("orange", "grape", "pear"))));
      } else if (Objects.equals(varcharColumn, "PKey")
          || Objects.equals(varcharColumn, "ForNull")) {
        assertAll(
            () -> assertThat(row.isNull("tinyint_column")).isTrue(),
            () -> assertThat(row.isNull("text_column")).isTrue(),
            () -> assertThat(row.isNull("date_column")).isTrue(),
            () -> assertThat(row.isNull("smallint_column")).isTrue(),
            () -> assertThat(row.isNull("mediumint_column")).isTrue(),
            () -> assertThat(row.isNull("int_column")).isTrue(),
            () -> assertThat(row.isNull("bigint_column")).isTrue(),
            () -> assertThat(row.isNull("float_column")).isTrue(),
            () -> assertThat(row.isNull("double_column")).isTrue(),
            () -> assertThat(row.isNull("decimal_column")).isTrue(),
            () -> assertThat(row.isNull("datetime_column")).isTrue(),
            () -> assertThat(row.isNull("timestamp_column")).isTrue(),
            () -> assertThat(row.isNull("time_column")).isTrue(),
            () -> assertThat(row.isNull("year_column")).isTrue(),
            () -> assertThat(row.isNull("char_column")).isTrue(),
            () -> assertThat(row.isNull("tinytext_column")).isTrue(),
            () -> assertThat(row.isNull("mediumtext_column")).isTrue(),
            () -> assertThat(row.isNull("longtext_column")).isTrue(),
            () -> assertThat(row.isNull("enum_column")).isTrue(),
            () -> assertThat(row.isNull("bool_column")).isTrue(),
            () -> assertThat(row.isNull("other_bool_column")).isTrue(),
            () -> assertThat(row.isNull("bytes_column")).isTrue(),
            () -> assertThat(row.isNull("list_text_column")).isTrue(),
            () -> assertThat(row.isNull("list_int_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_list_bigint_column")).isTrue(),
            () -> assertThat(row.isNull("set_text_column")).isTrue(),
            () -> assertThat(row.isNull("set_date_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_set_bool_column")).isTrue(),
            () -> assertThat(row.isNull("map_text_to_int_column")).isTrue(),
            () -> assertThat(row.isNull("map_date_to_text_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_map_int_to_bool_column")).isTrue(),
            () -> assertThat(row.isNull("map_text_to_list_column")).isTrue(),
            () -> assertThat(row.isNull("map_text_to_set_column")).isTrue(),
            () -> assertThat(row.isNull("set_of_maps_column")).isTrue(),
            () -> assertThat(row.isNull("list_of_sets_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_map_text_to_list_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_map_text_to_set_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_set_of_maps_column")).isTrue(),
            () -> assertThat(row.isNull("frozen_list_of_sets_column")).isTrue(),
            () -> assertThat(row.isNull("varint_column")).isTrue(),
            () -> assertThat(row.isNull("inet_column")).isTrue(),
            () -> assertThat(row.isNull("timeuuid_column")).isTrue(),
            () -> assertThat(row.isNull("duration_column")).isTrue(),
            () -> assertThat(row.isNull("uuid_column")).isTrue(),
            () -> assertThat(row.isNull("ascii_column")).isTrue(),
            () -> assertThat(row.isNull("list_text_column_from_array")).isTrue(),
            () -> assertThat(row.isNull("set_text_column_from_array")).isTrue());
      } else {
        throw new AssertionError("Unexpected row found: " + varcharColumn);
      }
    }
  }

  private void writeAllRowsAsStringInSpanner() {
    Mutation m;
    m =
        Mutation.newInsertOrUpdateBuilder(ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE)
            .set("varchar_column")
            .to("SampleVarchar")
            .set("tinyint_column")
            .to(String.valueOf(127))
            .set("text_column")
            .to("This is some sample text data for the text column.")
            .set("date_column")
            .to(String.valueOf(Date.fromJavaUtilDate(java.sql.Date.valueOf("2025-01-27"))))
            .set("smallint_column")
            .to(String.valueOf(32767))
            .set("mediumint_column")
            .to(String.valueOf(8388607))
            .set("int_column")
            .to(String.valueOf(2147483647))
            .set("bigint_column")
            .to(String.valueOf(9223372036854775807L))
            .set("float_column")
            .to(String.valueOf(3.14159f))
            .set("double_column")
            .to(String.valueOf(2.718281828459045))
            .set("decimal_column")
            .to(new BigDecimal("12345.6789").toPlainString())
            .set("datetime_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00Z")))
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00.123456Z")))
            .set("time_column")
            .to("12:30:00")
            .set("year_column")
            .to("2025")
            .set("char_column")
            .to("CHAR_DATA")
            .set("tinytext_column")
            .to("Short text for tinytext.")
            .set("mediumtext_column")
            .to("Longer text data for mediumtext column.")
            .set("longtext_column")
            .to("Very long text data that exceeds the medium text column length for long text.")
            .set("enum_column")
            .to("OptionA")
            .set("bool_column")
            .to(String.valueOf(Boolean.TRUE))
            .set("other_bool_column")
            .to(String.valueOf(Boolean.FALSE))
            .set("list_text_column")
            .to(Value.json("[\"apple\", \"banana\", \"cherry\"]"))
            .set("list_int_column")
            .to(Value.json("[1, 2, 3, 4, 5]"))
            .set("frozen_list_bigint_column")
            .to(Value.json("[123456789012345, 987654321012345]"))
            .set("set_text_column")
            .to(Value.json("[\"apple\", \"orange\", \"banana\"]"))
            .set("set_date_column")
            .to(Value.json("[\"2025-01-27\", \"2025-02-01\"]"))
            .set("frozen_set_bool_column")
            .to(Value.json("[true, false]"))
            .set("map_text_to_int_column")
            .to(Value.json("{\"key1\": 10, \"key2\": 20}"))
            .set("map_date_to_text_column")
            .to(Value.json("{\"2025-01-27\": \"event1\", \"2025-02-01\": \"event2\"}"))
            .set("frozen_map_int_to_bool_column")
            .to(Value.json("{\"1\": true, \"2\": false}"))
            .set("map_text_to_list_column")
            .to(Value.json("{\"fruit\": [\"apple\", \"banana\"], \"color\": [\"red\", \"green\"]}"))
            .set("map_text_to_set_column")
            .to(
                Value.json(
                    "{\"fruit\": [\"apple\", \"banana\"], \"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("frozen_map_text_to_list_column")
            .to(Value.json("{\"fruits\": [\"apple\", \"banana\"]}"))
            .set("frozen_map_text_to_set_column")
            .to(Value.json("{\"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("frozen_set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("frozen_list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("varint_column")
            .to("123456789")
            .set("inet_column")
            .to("192.168.1.100")
            .set("timeuuid_column")
            .to("f47ac10b-58cc-11e1-b86c-0800200c9a66")
            .set("duration_column")
            .to("P2D") // Example ISO-8601 duration
            .set("uuid_column")
            .to("123e4567-e89b-12d3-a456-426614174000")
            .set("ascii_column")
            .to("SampleASCII")
            .set("bytes_column")
            .to("c2FtcGxlQnl0ZXM=")
            .build();

    spannerResourceManager.write(m);

    m =
        Mutation.newInsertOrUpdateBuilder(ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE)
            .set("varchar_column")
            .to("SampleVarchar2")
            .set("tinyint_column")
            .to(String.valueOf(127))
            .set("text_column")
            .to("This is some sample text data for the text column.")
            .set("date_column")
            .to(String.valueOf(Date.fromJavaUtilDate(java.sql.Date.valueOf("2025-01-27"))))
            .set("smallint_column")
            .to(String.valueOf(32767))
            .set("mediumint_column")
            .to(String.valueOf(8388607))
            .set("int_column")
            .to(String.valueOf(2147483647))
            .set("bigint_column")
            .to(String.valueOf(9223372036854775807L))
            .set("float_column")
            .to(String.valueOf(3.14159f))
            .set("double_column")
            .to(String.valueOf(2.718281828459045))
            .set("decimal_column")
            .to(new BigDecimal("12345.6789").toPlainString())
            .set("datetime_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00Z")))
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00.123456Z")))
            .set("time_column")
            .to("12:30:00")
            .set("year_column")
            .to("2025")
            .set("char_column")
            .to("CHAR_DATA")
            .set("tinytext_column")
            .to("Short text for tinytext.")
            .set("mediumtext_column")
            .to("Longer text data for mediumtext column.")
            .set("longtext_column")
            .to("Very long text data that exceeds the medium text column length for long text.")
            .set("enum_column")
            .to("OptionA")
            .set("bool_column")
            .to(String.valueOf(Boolean.TRUE))
            .set("other_bool_column")
            .to(String.valueOf(Boolean.FALSE))
            .set("list_text_column")
            .to(Value.json("[\"apple\", \"banana\", \"cherry\"]"))
            .set("list_int_column")
            .to(Value.json("[1, 2, 3, 4, 5]"))
            .set("frozen_list_bigint_column")
            .to(Value.json("[123456789012345, 987654321012345]"))
            .set("set_text_column")
            .to(Value.json("[\"apple\", \"orange\", \"banana\"]"))
            .set("set_date_column")
            .to(Value.json("[\"2025-01-27\", \"2025-02-01\"]"))
            .set("frozen_set_bool_column")
            .to(Value.json("[true, false]"))
            .set("map_text_to_int_column")
            .to(Value.json("{\"key1\": 10, \"key2\": 20}"))
            .set("map_date_to_text_column")
            .to(Value.json("{\"2025-01-27\": \"event1\", \"2025-02-01\": \"event2\"}"))
            .set("frozen_map_int_to_bool_column")
            .to(Value.json("{\"1\": true, \"2\": false}"))
            .set("map_text_to_list_column")
            .to(Value.json("{\"fruit\": [\"apple\", \"banana\"], \"color\": [\"red\", \"green\"]}"))
            .set("map_text_to_set_column")
            .to(
                Value.json(
                    "{\"fruit\": [\"apple\", \"banana\"], \"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("frozen_map_text_to_list_column")
            .to(Value.json("{\"fruits\": [\"apple\", \"banana\"]}"))
            .set("frozen_map_text_to_set_column")
            .to(Value.json("{\"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("frozen_set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("frozen_list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("varint_column")
            .to("123456789")
            .set("inet_column")
            .to("192.168.1.101")
            .set("timeuuid_column")
            .to("f2c74f2e-1c4a-11ec-9621-0242ac130002")
            .set("duration_column")
            .to("PT5H10M")
            .set("uuid_column")
            .to("0e3f2a1c-dc40-4e73-9e0d-2d7d0ef7be7f")
            .set("ascii_column")
            .to("SampleASCII2")
            .set("bytes_column")
            .to("YW5vdGhlclNhbXBsZUJ5dGVz")
            .build();

    spannerResourceManager.write(m);

    m =
        Mutation.newUpdateBuilder(ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE)
            .set("varchar_column")
            .to("SampleVarchar")
            .set("tinyint_column")
            .to(String.valueOf(122))
            .set("text_column")
            .to("This is some sample text data for the text column.")
            .set("date_column")
            .to(String.valueOf(Date.fromJavaUtilDate(java.sql.Date.valueOf("2025-01-27"))))
            .set("smallint_column")
            .to(String.valueOf(32767))
            .set("mediumint_column")
            .to(String.valueOf(8388607))
            .set("int_column")
            .to(String.valueOf(2147483647))
            .set("bigint_column")
            .to(String.valueOf(9223372036854775807L))
            .set("float_column")
            .to(String.valueOf(3.14159f))
            .set("double_column")
            .to(String.valueOf(2.718281828459045))
            .set("decimal_column")
            .to(new BigDecimal("12345.6789").toPlainString())
            .set("datetime_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00Z")))
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("2025-01-27T10:30:00.123456Z")))
            .set("time_column")
            .to("12:30:00")
            .set("year_column")
            .to("2025")
            .set("char_column")
            .to("CHAR_DATA")
            .set("tinytext_column")
            .to("Short text for tinytext.")
            .set("mediumtext_column")
            .to("Longer text data for mediumtext column.")
            .set("longtext_column")
            .to("Very long text data that exceeds the medium text column length for long text.")
            .set("enum_column")
            .to("OptionA")
            .set("bool_column")
            .to(String.valueOf(Boolean.TRUE))
            .set("other_bool_column")
            .to(String.valueOf(Boolean.FALSE))
            .set("list_text_column")
            .to(Value.json("[\"apple\", \"banana\", \"cherry\"]"))
            .set("list_int_column")
            .to(Value.json("[1, 2, 3, 4, 5]"))
            .set("frozen_list_bigint_column")
            .to(Value.json("[123456789012345, 987654321012345]"))
            .set("set_text_column")
            .to(Value.json("[\"apple\", \"orange\", \"banana\"]"))
            .set("set_date_column")
            .to(Value.json("[\"2025-01-27\", \"2025-02-01\"]"))
            .set("frozen_set_bool_column")
            .to(Value.json("[true, false]"))
            .set("map_text_to_int_column")
            .to(Value.json("{\"key1\": 10, \"key2\": 20}"))
            .set("map_date_to_text_column")
            .to(Value.json("{\"2025-01-27\": \"event1\", \"2025-02-01\": \"event2\"}"))
            .set("frozen_map_int_to_bool_column")
            .to(Value.json("{\"1\": true, \"2\": false}"))
            .set("map_text_to_list_column")
            .to(Value.json("{\"fruit\": [\"apple\", \"banana\"], \"color\": [\"red\", \"green\"]}"))
            .set("map_text_to_set_column")
            .to(
                Value.json(
                    "{\"fruit\": [\"apple\", \"banana\"], \"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("frozen_map_text_to_list_column")
            .to(Value.json("{\"fruits\": [\"apple\", \"banana\"]}"))
            .set("frozen_map_text_to_set_column")
            .to(Value.json("{\"vegetables\": [\"carrot\", \"spinach\"]}"))
            .set("frozen_set_of_maps_column")
            .to(Value.json("[{\"key1\": 10, \"key2\": 20}, {\"keyA\": 5, \"keyB\": 10}]"))
            .set("frozen_list_of_sets_column")
            .to(Value.json("[[\"apple\", \"banana\"], [\"carrot\", \"spinach\"]]"))
            .set("varint_column")
            .to("123456789")
            .set("inet_column")
            .to("10.0.0.1")
            .set("timeuuid_column")
            .to("e2f94108-1c4a-11ec-8b57-0242ac130003")
            .set("duration_column")
            .to("P4DT12H30M5S")
            .set("uuid_column")
            .to("f03e2a1c-dc40-4e73-9e0d-2d7d0ef7ae7e")
            .set("ascii_column")
            .to("UpdatedSampleASCII")
            .set("bytes_column")
            .to("dXBkYXRlZFNhbXBsZUJ5dGVz")
            .build();

    spannerResourceManager.write(m);
  }

  private void assertStringToActualRowsInCassandraDB() throws MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                () -> getRowCount(ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE) == 2);
    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to read from Cassandra table: " + ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE, e);
    }

    assertThat(rows).hasSize(2);
    Row row = rows.iterator().next();
    LOG.info("[AssertStringToActualRowsInCassandraDB] row: {}", row.getFormattedContents());
    assertAll(
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar2"),
        () -> assertThat(row.getByte("tinyint_column")).isEqualTo((byte) 127),
        () -> assertThat(row.getLong("bigint_column")).isEqualTo(9223372036854775807L),
        () -> assertThat(row.getBoolean("bool_column")).isTrue(),
        () -> assertThat(row.getString("char_column")).isEqualTo("CHAR_DATA"),
        () ->
            assertThat(row.getLocalDate("date_column"))
                .isEqualTo(java.time.LocalDate.of(2025, 1, 27)),
        () ->
            assertThat(row.getInstant("datetime_column"))
                .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00.000Z")),
        () ->
            assertThat(row.getBigDecimal("decimal_column")).isEqualTo(new BigDecimal("12345.6789")),
        () -> assertThat(row.getDouble("double_column")).isEqualTo(2.718281828459045),
        () -> assertThat(row.getFloat("float_column")).isEqualTo(3.14159f),

        // Collections (frozen, list, set, map)
        () ->
            assertThat(row.getList("frozen_list_bigint_column", Long.class))
                .isEqualTo(Arrays.asList(123456789012345L, 987654321012345L)),
        () ->
            assertThat(row.getSet("frozen_set_bool_column", Boolean.class))
                .isEqualTo(new HashSet<>(Arrays.asList(false, true))),
        () ->
            assertThat(row.getMap("frozen_map_int_to_bool_column", Integer.class, Boolean.class))
                .isEqualTo(Map.of(1, true, 2, false)),
        () ->
            assertThat(row.getMap("frozen_map_text_to_list_column", String.class, List.class))
                .isEqualTo(Map.of("fruits", Arrays.asList("apple", "banana"))),
        () ->
            assertThat(row.getMap("frozen_map_text_to_set_column", String.class, Set.class))
                .isEqualTo(Map.of("vegetables", new HashSet<>(Arrays.asList("carrot", "spinach")))),
        () ->
            assertThat(row.getSet("frozen_set_of_maps_column", Map.class))
                .isEqualTo(
                    new HashSet<>(
                        Arrays.asList(
                            Map.of("key1", 10, "key2", 20), Map.of("keyA", 5, "keyB", 10)))),

        // Lists and Sets
        () ->
            assertThat(row.getList("list_int_column", Integer.class))
                .isEqualTo(Arrays.asList(1, 2, 3, 4, 5)),
        () ->
            assertThat(row.getList("list_text_column", String.class))
                .isEqualTo(Arrays.asList("apple", "banana", "cherry")),
        () ->
            assertThat(row.getList("list_of_sets_column", Set.class))
                .isEqualTo(
                    Arrays.asList(
                        new HashSet<>(Arrays.asList("apple", "banana")),
                        new HashSet<>(Arrays.asList("carrot", "spinach")))),

        // Maps
        () ->
            assertThat(
                    row.getMap("map_date_to_text_column", java.time.LocalDate.class, String.class))
                .isEqualTo(
                    Map.of(
                        java.time.LocalDate.parse("2025-01-27"), "event1",
                        java.time.LocalDate.parse("2025-02-01"), "event2")),
        () ->
            assertThat(row.getMap("map_text_to_int_column", String.class, Integer.class))
                .isEqualTo(Map.of("key1", 10, "key2", 20)),
        () ->
            assertThat(row.getMap("map_text_to_list_column", String.class, List.class))
                .isEqualTo(
                    Map.of(
                        "color",
                        Arrays.asList("red", "green"),
                        "fruit",
                        Arrays.asList("apple", "banana"))),
        () ->
            assertThat(row.getMap("map_text_to_set_column", String.class, Set.class))
                .isEqualTo(
                    Map.of(
                        "fruit",
                        new HashSet<>(Arrays.asList("apple", "banana")),
                        "vegetables",
                        new HashSet<>(Arrays.asList("carrot", "spinach")))),

        // Sets
        () ->
            assertThat(row.getSet("set_date_column", java.time.LocalDate.class))
                .isEqualTo(
                    new HashSet<>(
                        Arrays.asList(
                            java.time.LocalDate.parse("2025-01-27"),
                            java.time.LocalDate.parse("2025-02-01")))),
        () ->
            assertThat(row.getSet("set_text_column", String.class))
                .isEqualTo(new HashSet<>(Arrays.asList("apple", "orange", "banana"))),
        () ->
            assertThat(row.getSet("set_of_maps_column", Map.class))
                .isEqualTo(
                    new HashSet<>(
                        Arrays.asList(
                            Map.of("key1", 10, "key2", 20), Map.of("keyA", 5, "keyB", 10)))),

        // Other Basic Types
        () -> assertThat(row.getShort("smallint_column")).isEqualTo((short) 32767),
        () -> assertThat(row.getInt("mediumint_column")).isEqualTo(8388607),
        () -> assertThat(row.getInt("int_column")).isEqualTo(2147483647),
        () -> assertThat(row.getString("enum_column")).isEqualTo("OptionA"),
        () -> assertThat(row.getString("year_column")).isEqualTo("2025"),
        () ->
            assertThat(row.getString("longtext_column"))
                .isEqualTo(
                    "Very long text data that exceeds the medium text column length for long text."),
        () -> assertThat(row.getString("tinytext_column")).isEqualTo("Short text for tinytext."),
        () ->
            assertThat(row.getString("mediumtext_column"))
                .isEqualTo("Longer text data for mediumtext column."),
        () ->
            assertThat(row.getString("text_column"))
                .isEqualTo("This is some sample text data for the text column."),
        () ->
            assertThat(row.getLocalTime("time_column"))
                .isEqualTo(java.time.LocalTime.parse("12:30:00.000000000")),
        () ->
            assertThat(row.getInstant("timestamp_column"))
                .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00.123Z")),
        () ->
            assertThat(row.getBigInteger("varint_column"))
                .isEqualTo(java.math.BigInteger.valueOf(123456789L)),
        () -> {
          InetAddress inetAddress = row.getInetAddress("inet_column");
          assertThat(inetAddress.getHostAddress()).isEqualTo("192.168.1.101");
        },
        () ->
            assertThat(row.getCqlDuration("duration_column"))
                .isEqualTo(CqlDuration.from("PT5H10M")),
        () ->
            assertThat(row.getUuid("timeuuid_column"))
                .isEqualTo(UUID.fromString("f2c74f2e-1c4a-11ec-9621-0242ac130002")),
        () ->
            assertThat(row.getUuid("uuid_column"))
                .isEqualTo(UUID.fromString("0e3f2a1c-dc40-4e73-9e0d-2d7d0ef7be7f")),
        () -> assertThat(row.getString("ascii_column")).isEqualTo("SampleASCII2"),
        () -> {
          ByteBuffer actualBytes = row.getBytesUnsafe("bytes_column");
          byte[] actualByteArray = new byte[actualBytes.remaining()];
          actualBytes.get(actualByteArray);
          String actualString = new String(actualByteArray, StandardCharsets.UTF_8);
          assertThat(actualString).isEqualTo("anotherSampleBytes");
        });
  }

  private void insertMaxBoundaryValuesIntoSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CONVERSION_TABLE)
            .set("varchar_column")
            .to("MaxBoundaryVarchar")
            .set("tinyint_column")
            .to(Byte.MAX_VALUE)
            .set("smallint_column")
            .to(Short.MAX_VALUE)
            .set("int_column")
            .to(Integer.MAX_VALUE)
            .set("bigint_column")
            .to(Long.MAX_VALUE)
            .set("float_column")
            .to(Float.POSITIVE_INFINITY)
            .set("double_column")
            .to(Double.POSITIVE_INFINITY)
            .set("decimal_column")
            .to(new BigDecimal("99999999999999999999999999999.999999999").toPlainString())
            .set("bool_column")
            .to(Boolean.TRUE)
            .set("ascii_column")
            .to("ASCII_TEXT")
            .set("text_column")
            .to("Text data")
            .set("bytes_column")
            .to("////////")
            .set("date_column")
            .to(Date.parseDate("9999-12-31"))
            .set("time_column")
            .to("23:59:59.999999")
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("9999-12-31T23:59:59.999999Z")))
            .set("duration_column")
            .to("P10675199DT2H48M5S")
            .set("uuid_column")
            .to("ffffffff-ffff-4fff-9fff-ffffffffffff")
            .set("timeuuid_column")
            .to("ffffffff-ffff-1fff-9fff-ffffffffffff")
            .set("inet_column")
            .to("192.168.0.1")
            .set("map_bool_column")
            .to(Value.json("{\"true\": false}"))
            .set("map_float_column")
            .to(Value.json("{\"3.4028235E38\": \"Infinity\"}"))
            .set("map_double_column")
            .to(Value.json("{\"2.718281828459045\": \"Infinity\"}"))
            .set("map_tinyint_column")
            .to(Value.json("{\"127\": \"127\"}"))
            .set("map_smallint_column")
            .to(Value.json("{\"32767\": \"32767\"}"))
            .set("map_int_column")
            .to(Value.json("{\"2147483647\": \"2147483647\"}"))
            .set("map_bigint_column")
            .to(Value.json("{\"9223372036854775807\": \"9223372036854775807\"}"))
            .set("map_varint_column")
            .to(Value.json("{\"100000000000000000000\": \"100000000000000000000\"}"))
            .set("map_decimal_column")
            .to(Value.json("{\"12345.6789\": \"99999999999999999999999999999.999999999\"}"))
            .set("map_ascii_column")
            .to(Value.json("{\"example1\": \"max_str1\", \"example2\": \"max_str2\"}"))
            .set("map_varchar_column")
            .to(Value.json("{\"key1\": \"max_value1\", \"key2\": \"max_value2\"}"))
            .set("map_blob_column")
            .to(Value.json("{\"R29vZ2xl\": \"U29tZURhdGE=\"}"))
            .set("map_date_column")
            .to(Value.json("{\"2025-01-27\": \"9999-12-31\"}"))
            .set("map_time_column")
            .to(Value.json("{\"12:30:00\": \"23:59:59\"}"))
            .set("map_timestamp_column")
            .to(Value.json("{\"2025-01-01T00:00:00Z\": \"9999-12-31T23:59:59.999999Z\"}"))
            .set("map_duration_column")
            .to(Value.json("{\"P4DT1H\": \"P10675199DT2H48M5S\"}"))
            .set("map_uuid_column")
            .to(
                Value.json(
                    "{\"123e4567-e89b-12d3-a456-426614174000\": \"ffffffff-ffff-4fff-9fff-ffffffffffff\"}"))
            .set("map_timeuuid_column")
            .to(
                Value.json(
                    "{\"321e4567-e89b-12d3-a456-426614174000\": \"ffffffff-ffff-1fff-9fff-ffffffffffff\"}"))
            .set("map_inet_column")
            .to(
                Value.json(
                    "{\"255.255.255.255\": \"::1\",\"3031:3233:3435:3637:3839:4041:4243:4445\": \"::ffff:192.0.2.128\" }"))
            .build();
    spannerResourceManager.write(mutation);
  }

  private void insertMinBoundaryValuesIntoSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CONVERSION_TABLE)
            .set("varchar_column")
            .to("MinBoundaryVarchar")
            .set("tinyint_column")
            .to(Byte.MIN_VALUE)
            .set("smallint_column")
            .to(Short.MIN_VALUE)
            .set("int_column")
            .to(Integer.MIN_VALUE)
            .set("bigint_column")
            .to(Long.MIN_VALUE)
            .set("float_column")
            .to(Float.NEGATIVE_INFINITY)
            .set("double_column")
            .to(Double.NEGATIVE_INFINITY)
            .set("decimal_column")
            .to(new BigDecimal("-99999999999999999999999999999.999999999").toPlainString())
            .set("bool_column")
            .to(Boolean.FALSE)
            .set("bytes_column")
            .to("AAAAAAAAAAA=")
            .set("date_column")
            .to(Date.parseDate("0001-01-01"))
            .set("time_column")
            .to("00:00:00.000000")
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("0001-01-01T00:00:00.000000Z")))
            .set("duration_column")
            .to("-PT0S")
            .set("uuid_column")
            .to("00000000-0000-0000-0000-000000000000")
            .set("timeuuid_column")
            .to("00000000-0000-1000-9000-000000000000")
            .set("inet_column")
            .to("0.0.0.0")
            .set("map_bool_column")
            .to(Value.json("{\"false\": true}"))
            .set("map_float_column")
            .to(Value.json("{\"-1.4E-45\": \"-Infinity\", \"NaN\": \"NaN\"}"))
            .set("map_double_column")
            .to(Value.json("{\"-2.718281828459045\": \"-Infinity\"}"))
            .set("map_tinyint_column")
            .to(Value.json("{\"-128\": \"-128\"}"))
            .set("map_smallint_column")
            .to(Value.json("{\"-32768\": \"-32768\"}"))
            .set("map_int_column")
            .to(Value.json("{\"-2147483648\": \"-2147483648\"}"))
            .set("map_bigint_column")
            .to(Value.json("{\"-9223372036854775808\": \"-9223372036854775808\"}"))
            .set("map_varint_column")
            .to(Value.json("{\"-100000000000000000000\": \"-100000000000000000000\"}"))
            .set("map_decimal_column")
            .to(Value.json("{\"-98765.4321\": \"-99999999999999999999999999999.999999999\"}"))
            .set("map_ascii_column")
            .to(Value.json("{\"exampleMin1\": \"min_str1\", \"exampleMin2\": \"min_str2\"}"))
            .set("map_varchar_column")
            .to(Value.json("{\"keyMin1\": \"min_value1\", \"keyMin2\": \"min_value2\"}"))
            .set("map_blob_column")
            .to(Value.json("{\"U29tZU5ld2RhdGE=\": \"Q29tcGFueQ==\"}"))
            .set("map_date_column")
            .to(Value.json("{\"0001-01-01\": \"1800-01-01\"}"))
            .set("map_time_column")
            .to(Value.json("{\"00:00:00\": \"01:00:00\"}"))
            .set("map_timestamp_column")
            .to(Value.json("{\"0001-01-01T00:00:00Z\": \"1900-01-01T00:00:00Z\"}"))
            .set("map_duration_column")
            .to(Value.json("{\"-P4DT1H\": \"-P10675199DT2H48M5S\"}"))
            .set("map_uuid_column")
            .to(
                Value.json(
                    "{\"00000000-0000-0000-0000-000000000000\": \"11111111-1111-1111-1111-111111111111\"}"))
            .set("map_timeuuid_column")
            .to(
                Value.json(
                    "{\"00000000-0000-1000-9000-000000000000\": \"10000000-0000-1000-9000-111111111111\"}"))
            .set("map_inet_column")
            .to(Value.json("{\"0.0.0.0\": \"0:0:0:0:0:0:0:0\"}"))
            .build();
    spannerResourceManager.write(mutation);
  }

  private void assertCassandraBoundaryData() throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(BOUNDARY_CONVERSION_TABLE) == 2);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(BOUNDARY_CONVERSION_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to read from Cassandra table: " + BOUNDARY_CONVERSION_TABLE, e);
    }
    assertThat(rows).hasSize(2);
    for (Row row : rows) {
      String varcharColumn = row.getString("varchar_column");
      if (Objects.equals(varcharColumn, "MaxBoundaryVarchar")) {
        assertAll(
            () -> assertThat(row.getByte("tinyint_column")).isEqualTo(Byte.MAX_VALUE),
            () -> assertThat(row.getShort("smallint_column")).isEqualTo(Short.MAX_VALUE),
            () -> assertThat(row.getInt("int_column")).isEqualTo(Integer.MAX_VALUE),
            () -> assertThat(row.getLong("bigint_column")).isEqualTo(Long.MAX_VALUE),
            () -> assertThat(row.getFloat("float_column")).isEqualTo(Float.POSITIVE_INFINITY),
            () -> assertThat(row.getDouble("double_column")).isEqualTo(Double.POSITIVE_INFINITY),
            () ->
                assertThat(row.getBigDecimal("decimal_column"))
                    .isEqualTo(new BigDecimal("99999999999999999999999999999.999999999")),
            () -> assertThat(row.getBoolean("bool_column")).isTrue(),
            () -> assertThat(row.getString("ascii_column")).isEqualTo("ASCII_TEXT"),
            () -> assertThat(row.getString("text_column")).isEqualTo("Text data"),
            () ->
                assertThat(row.getCqlDuration("duration_column"))
                    .isEqualTo(CqlDuration.from("P10675199DT2H48M5S")),
            () ->
                assertThat(row.getUuid("timeuuid_column"))
                    .isEqualTo(UUID.fromString("ffffffff-ffff-1fff-9fff-ffffffffffff")),
            () ->
                assertThat(row.getUuid("uuid_column"))
                    .isEqualTo(UUID.fromString("ffffffff-ffff-4fff-9fff-ffffffffffff")),
            () -> {
              byte[] expectedBytes = Base64.getDecoder().decode("////////");
              ByteBuffer actualBytes = row.getBytesUnsafe("bytes_column");
              assertThat(actualBytes).isEqualTo(ByteBuffer.wrap(expectedBytes));
            },
            () ->
                assertThat(row.getLocalDate("date_column"))
                    .isEqualTo(LocalDate.parse("9999-12-31")),
            () ->
                assertThat(row.getLocalTime("time_column"))
                    .isEqualTo(java.time.LocalTime.parse("23:59:59.999999")),
            () ->
                assertThat(row.getInstant("timestamp_column"))
                    .isEqualTo(java.time.Instant.parse("9999-12-31T23:59:59.999Z")),
            // Maps
            () ->
                assertThat(row.getMap("map_bool_column", Boolean.class, Boolean.class))
                    .isEqualTo(Map.of(true, false)),
            () -> {
              Map<Float, Float> expected = Map.of(3.4028235E38f, Float.POSITIVE_INFINITY);
              Map<Float, Float> actual = row.getMap("map_float_column", Float.class, Float.class);
              expected.forEach(
                  (key, expectedValue) -> {
                    Assertions.assertThat(actual.containsKey(key))
                        .withFailMessage("Actual map is missing key: %s", key)
                        .isTrue();
                    Float actualValue = actual.get(key);
                    if (Float.isNaN(expectedValue)) {
                      Assertions.assertThat(Float.isNaN(actualValue))
                          .withFailMessage("Value for key %s should be NaN", key)
                          .isTrue();
                    } else if (Float.isInfinite(expectedValue)) {
                      Assertions.assertThat(actualValue)
                          .withFailMessage("Value for key %s should be Infinity", key)
                          .isEqualTo(Float.POSITIVE_INFINITY);
                    } else {
                      Assertions.assertThat(actualValue)
                          .withFailMessage("Value for key %s is incorrect", key)
                          .isEqualTo(expectedValue);
                    }
                  });
              Set<Float> unexpectedKeys =
                  actual.keySet().stream()
                      .filter(key -> !expected.containsKey(key))
                      .collect(Collectors.toSet());
              Assertions.assertThat(unexpectedKeys)
                  .withFailMessage("Actual map has unexpected keys: %s", unexpectedKeys)
                  .isEmpty();
            },
            () ->
                assertThat(row.getMap("map_double_column", Double.class, Double.class))
                    .isEqualTo(Map.of(2.718281828459045, Double.POSITIVE_INFINITY)),
            () ->
                assertThat(row.getMap("map_tinyint_column", Byte.class, Byte.class))
                    .isEqualTo(Map.of((byte) 127, (byte) 127)),
            () ->
                assertThat(row.getMap("map_smallint_column", Short.class, Short.class))
                    .isEqualTo(Map.of((short) 32767, (short) 32767)),
            () ->
                assertThat(row.getMap("map_int_column", Integer.class, Integer.class))
                    .isEqualTo(Map.of(2147483647, 2147483647)),
            () ->
                assertThat(row.getMap("map_bigint_column", Long.class, Long.class))
                    .isEqualTo(Map.of(9223372036854775807L, 9223372036854775807L)),
            () ->
                assertThat(row.getMap("map_varint_column", BigInteger.class, BigInteger.class))
                    .isEqualTo(
                        Map.of(
                            new BigInteger("100000000000000000000"),
                            new BigInteger("100000000000000000000"))),
            () ->
                assertThat(row.getMap("map_decimal_column", BigDecimal.class, BigDecimal.class))
                    .isEqualTo(
                        Map.of(
                            new BigDecimal("12345.6789"),
                            new BigDecimal("99999999999999999999999999999.999999999"))),
            () ->
                assertThat(row.getMap("map_ascii_column", String.class, String.class))
                    .isEqualTo(Map.of("example1", "max_str1", "example2", "max_str2")),
            () ->
                assertThat(row.getMap("map_varchar_column", String.class, String.class))
                    .isEqualTo(Map.of("key1", "max_value1", "key2", "max_value2")),
            () -> {
              byte[] keyBytes = Base64.getDecoder().decode("R29vZ2xl");
              byte[] valueBytes = Base64.getDecoder().decode("U29tZURhdGE=");
              Map<ByteBuffer, ByteBuffer> expected = new HashMap<>();
              expected.put(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
              Map<ByteBuffer, ByteBuffer> actual =
                  row.getMap("map_blob_column", ByteBuffer.class, ByteBuffer.class);
              Assertions.assertThat(actual)
                  .allSatisfy(
                      (key, value) -> {
                        ByteBuffer expectedKey =
                            expected.keySet().stream()
                                .filter(k -> compareByteBuffers(k, key))
                                .findAny()
                                .orElse(null);
                        Assertions.assertThat(expectedKey)
                            .withFailMessage("Unexpected key: %s", keyToString(key))
                            .isNotNull();
                        ByteBuffer expectedValue = expected.get(expectedKey);
                        Assertions.assertThat(expectedValue)
                            .withFailMessage("Unexpected value for key %s", keyToString(key))
                            .satisfies(v -> compareByteBuffers(v, value));
                      });
            },
            () ->
                assertThat(row.getMap("map_date_column", LocalDate.class, LocalDate.class))
                    .isEqualTo(
                        Map.of(LocalDate.parse("2025-01-27"), LocalDate.parse("9999-12-31"))),
            () ->
                assertThat(row.getMap("map_time_column", LocalTime.class, LocalTime.class))
                    .isEqualTo(Map.of(LocalTime.parse("12:30:00"), LocalTime.parse("23:59:59"))),
            () ->
                assertThat(row.getMap("map_timestamp_column", Instant.class, Instant.class))
                    .isEqualTo(
                        Map.of(
                            java.time.Instant.parse("2025-01-01T00:00:00Z"),
                            java.time.Instant.parse("9999-12-31T23:59:59.999Z"))),
            () ->
                assertThat(row.getMap("map_duration_column", String.class, CqlDuration.class))
                    .isEqualTo(Map.of("P4DT1H", CqlDuration.from("P10675199DT2H48M5S"))),
            () ->
                assertThat(row.getMap("map_uuid_column", UUID.class, UUID.class))
                    .isEqualTo(
                        Map.of(
                            UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                            UUID.fromString("ffffffff-ffff-4fff-9fff-ffffffffffff"))),
            () ->
                assertThat(row.getMap("map_timeuuid_column", UUID.class, UUID.class))
                    .isEqualTo(
                        Map.of(
                            UUID.fromString("321e4567-e89b-12d3-a456-426614174000"),
                            UUID.fromString("ffffffff-ffff-1fff-9fff-ffffffffffff"))),
            () -> {
              try {
                Map<InetAddress, InetAddress> expected =
                    Map.of(
                        InetAddress.getByName("255.255.255.255"),
                        InetAddress.getByName("::1"),
                        InetAddress.getByName("3031:3233:3435:3637:3839:4041:4243:4445"),
                        InetAddress.getByName("::ffff:192.0.2.128"));
                Map<InetAddress, InetAddress> actual =
                    row.getMap("map_inet_column", InetAddress.class, InetAddress.class);
                Assertions.assertThat(actual)
                    .as(
                        "Checking the mapping of IP addresses between Cassandra and the expected output")
                    .isEqualTo(expected);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to convert String to InetAddress, possibly due to an invalid IP format.",
                    e);
              }
            });
      } else if (Objects.equals(varcharColumn, "MinBoundaryVarchar")) {
        assertAll(
            () -> assertThat(row.getByte("tinyint_column")).isEqualTo(Byte.MIN_VALUE),
            () -> assertThat(row.getShort("smallint_column")).isEqualTo(Short.MIN_VALUE),
            () -> assertThat(row.getInt("int_column")).isEqualTo(Integer.MIN_VALUE),
            () -> assertThat(row.getLong("bigint_column")).isEqualTo(Long.MIN_VALUE),
            () -> assertThat(row.getFloat("float_column")).isEqualTo(Float.NEGATIVE_INFINITY),
            () -> assertThat(row.getDouble("double_column")).isEqualTo(Double.NEGATIVE_INFINITY),
            () ->
                assertThat(row.getBigDecimal("decimal_column"))
                    .isEqualTo(new BigDecimal("-99999999999999999999999999999.999999999")),
            () -> assertThat(row.getBoolean("bool_column")).isFalse(),
            () -> assertThat(row.isNull("ascii_column")).isTrue(),
            () -> assertThat(row.isNull("text_column")).isTrue(),
            () ->
                assertThat(row.getCqlDuration("duration_column"))
                    .isEqualTo(CqlDuration.from("-PT0S")),
            () ->
                assertThat(row.getUuid("timeuuid_column"))
                    .isEqualTo(UUID.fromString("00000000-0000-1000-9000-000000000000")),
            () ->
                assertThat(row.getUuid("uuid_column"))
                    .isEqualTo(UUID.fromString("00000000-0000-0000-0000-000000000000")),
            () -> {
              byte[] expectedMinBytes = Base64.getDecoder().decode("AAAAAAAAAAA=");
              ByteBuffer actualMinBytes = row.getBytesUnsafe("bytes_column");
              assertThat(actualMinBytes).isEqualTo(ByteBuffer.wrap(expectedMinBytes));
            },
            () ->
                assertThat(row.getLocalDate("date_column"))
                    .isEqualTo(LocalDate.parse("0001-01-01")),
            () ->
                assertThat(row.getLocalTime("time_column"))
                    .isEqualTo(java.time.LocalTime.parse("00:00:00.000000")),
            () ->
                assertThat(row.getInstant("timestamp_column"))
                    .isEqualTo(java.time.Instant.parse("0001-01-01T00:00:00.000Z")),
            // Maps
            () ->
                assertThat(row.getMap("map_bool_column", Boolean.class, Boolean.class))
                    .isEqualTo(Map.of(false, true)),
            () -> {
              Map<Float, Float> expected =
                  Map.of(-1.4E-45f, Float.NEGATIVE_INFINITY, Float.NaN, Float.NaN);
              Map<Float, Float> actual = row.getMap("map_float_column", Float.class, Float.class);
              expected.forEach(
                  (key, expectedValue) -> {
                    Assertions.assertThat(actual.containsKey(key))
                        .withFailMessage("Actual map is missing key: %s", key)
                        .isTrue();
                    Float actualValue = actual.get(key);
                    if (Float.isNaN(expectedValue)) {
                      Assertions.assertThat(Float.isNaN(actualValue))
                          .withFailMessage("Value for key %s should be NaN", key)
                          .isTrue();
                    } else if (Float.isInfinite(expectedValue)) {
                      Assertions.assertThat(actualValue)
                          .withFailMessage("Value for key %s should be %s", key, expectedValue)
                          .isEqualTo(expectedValue);
                    } else {
                      Assertions.assertThat(actualValue)
                          .withFailMessage("Value for key %s is incorrect", key)
                          .isEqualTo(expectedValue);
                    }
                  });
              Set<Float> unexpectedKeys =
                  actual.keySet().stream()
                      .filter(key -> !expected.containsKey(key))
                      .collect(Collectors.toSet());
              Assertions.assertThat(unexpectedKeys)
                  .withFailMessage("Actual map has unexpected keys: %s", unexpectedKeys)
                  .isEmpty();
            },
            () ->
                assertThat(row.getMap("map_double_column", Double.class, Double.class))
                    .isEqualTo(Map.of(-2.718281828459045, Double.NEGATIVE_INFINITY)),
            () ->
                assertThat(row.getMap("map_tinyint_column", Byte.class, Byte.class))
                    .isEqualTo(Map.of((byte) -128, (byte) -128)),
            () ->
                assertThat(row.getMap("map_smallint_column", Short.class, Short.class))
                    .isEqualTo(Map.of((short) -32768, (short) -32768)),
            () ->
                assertThat(row.getMap("map_int_column", Integer.class, Integer.class))
                    .isEqualTo(Map.of(-2147483648, -2147483648)),
            () ->
                assertThat(row.getMap("map_bigint_column", Long.class, Long.class))
                    .isEqualTo(Map.of(-9223372036854775808L, -9223372036854775808L)),
            () ->
                assertThat(row.getMap("map_varint_column", BigInteger.class, BigInteger.class))
                    .isEqualTo(
                        Map.of(
                            new BigInteger("-100000000000000000000"),
                            new BigInteger("-100000000000000000000"))),
            () ->
                assertThat(row.getMap("map_decimal_column", BigDecimal.class, BigDecimal.class))
                    .isEqualTo(
                        Map.of(
                            new BigDecimal("-98765.4321"),
                            new BigDecimal("-99999999999999999999999999999.999999999"))),
            () ->
                assertThat(row.getMap("map_ascii_column", String.class, String.class))
                    .isEqualTo(Map.of("exampleMin1", "min_str1", "exampleMin2", "min_str2")),
            () ->
                assertThat(row.getMap("map_varchar_column", String.class, String.class))
                    .isEqualTo(Map.of("keyMin1", "min_value1", "keyMin2", "min_value2")),
            () -> {
              byte[] keyBytes = Base64.getDecoder().decode("U29tZU5ld2RhdGE=");
              byte[] valueBytes = Base64.getDecoder().decode("Q29tcGFueQ==");
              Map<ByteBuffer, ByteBuffer> expected = new HashMap<>();
              expected.put(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
              Map<ByteBuffer, ByteBuffer> actual =
                  row.getMap("map_blob_column", ByteBuffer.class, ByteBuffer.class);
              Assertions.assertThat(actual)
                  .allSatisfy(
                      (key, value) -> {
                        ByteBuffer expectedKey =
                            expected.keySet().stream()
                                .filter(k -> compareByteBuffers(k, key))
                                .findAny()
                                .orElse(null);
                        Assertions.assertThat(expectedKey)
                            .withFailMessage("Unexpected key: %s", keyToString(key))
                            .isNotNull();
                        ByteBuffer expectedValue = expected.get(expectedKey);
                        Assertions.assertThat(expectedValue)
                            .withFailMessage("Unexpected value for key %s", keyToString(key))
                            .satisfies(v -> compareByteBuffers(v, value));
                      });
            },
            () ->
                assertThat(row.getMap("map_date_column", LocalDate.class, LocalDate.class))
                    .isEqualTo(
                        Map.of(LocalDate.parse("0001-01-01"), LocalDate.parse("1800-01-01"))),
            () ->
                assertThat(row.getMap("map_time_column", LocalTime.class, LocalTime.class))
                    .isEqualTo(Map.of(LocalTime.parse("00:00:00"), LocalTime.parse("01:00:00"))),
            () ->
                assertThat(row.getMap("map_timestamp_column", Instant.class, Instant.class))
                    .isEqualTo(
                        Map.of(
                            java.time.Instant.parse("0001-01-01T00:00:00Z"),
                            java.time.Instant.parse("1900-01-01T00:00:00Z"))),
            () ->
                assertThat(row.getMap("map_duration_column", String.class, CqlDuration.class))
                    .isEqualTo(Map.of("-P4DT1H", CqlDuration.from("-P10675199DT2H48M5S"))),
            () ->
                assertThat(row.getMap("map_uuid_column", UUID.class, UUID.class))
                    .isEqualTo(
                        Map.of(
                            UUID.fromString("00000000-0000-0000-0000-000000000000"),
                            UUID.fromString("11111111-1111-1111-1111-111111111111"))),
            () ->
                assertThat(row.getMap("map_timeuuid_column", UUID.class, UUID.class))
                    .isEqualTo(
                        Map.of(
                            UUID.fromString("00000000-0000-1000-9000-000000000000"),
                            UUID.fromString("10000000-0000-1000-9000-111111111111"))),
            () -> {
              try {
                Map<InetAddress, InetAddress> expected =
                    Map.of(
                        InetAddress.getByName("0.0.0.0"), InetAddress.getByName("0:0:0:0:0:0:0:0"));
                Map<InetAddress, InetAddress> actual =
                    row.getMap("map_inet_column", InetAddress.class, InetAddress.class);
                Assertions.assertThat(actual)
                    .as(
                        "Checking the mapping of IP addresses between Cassandra and the expected output")
                    .isEqualTo(expected);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to convert String to InetAddress, possibly due to an invalid IP format.",
                    e);
              }
            });
      }
    }
  }

  private void updateBoundaryValuesInSpanner() {
    // Update max boundary record to null (use mutation strategy that doesn't affect primary key)
    Mutation updateMaxToNull =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CONVERSION_TABLE)
            .set("varchar_column")
            .to("MaxBoundaryVarchar")
            .set("tinyint_column")
            .to(Value.int64(null))
            .set("smallint_column")
            .to(Value.int64(null))
            .set("int_column")
            .to(Value.int64(null))
            .set("bigint_column")
            .to(Value.int64(null))
            .set("float_column")
            .to((Float) null)
            .set("double_column")
            .to((Double) null)
            .set("decimal_column")
            .to((BigDecimal) null)
            .set("bool_column")
            .to((Boolean) null)
            .set("ascii_column")
            .to((String) null)
            .set("text_column")
            .to((String) null)
            .set("bytes_column")
            .to(Value.bytes(null))
            .set("date_column")
            .to((Date) null)
            .set("time_column")
            .to((String) null)
            .set("timestamp_column")
            .to((Timestamp) null)
            .set("duration_column")
            .to((String) null)
            .set("uuid_column")
            .to((String) null)
            .set("timeuuid_column")
            .to((String) null)
            .set("inet_column")
            .to((String) null)
            .set("map_bool_column")
            .to((String) null)
            .set("map_float_column")
            .to((String) null)
            .set("map_double_column")
            .to((String) null)
            .set("map_tinyint_column")
            .to((String) null)
            .set("map_smallint_column")
            .to((String) null)
            .set("map_int_column")
            .to((String) null)
            .set("map_bigint_column")
            .to((String) null)
            .set("map_varint_column")
            .to((String) null)
            .set("map_decimal_column")
            .to((String) null)
            .set("map_ascii_column")
            .to((String) null)
            .set("map_varchar_column")
            .to((String) null)
            .set("map_blob_column")
            .to((String) null)
            .set("map_date_column")
            .to((String) null)
            .set("map_time_column")
            .to((String) null)
            .set("map_timestamp_column")
            .to((String) null)
            .set("map_duration_column")
            .to((String) null)
            .set("map_uuid_column")
            .to((String) null)
            .set("map_timeuuid_column")
            .to((String) null)
            .set("map_inet_column")
            .to((String) null)
            .build();
    spannerResourceManager.write(updateMaxToNull);
  }

  private void assertCassandraAfterUpdate() throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(BOUNDARY_CONVERSION_TABLE) == 1);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(BOUNDARY_CONVERSION_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to read from Cassandra table: " + BOUNDARY_CONVERSION_TABLE, e);
    }
    assertThat(rows).hasSize(1);
    for (Row row : rows) {
      String varcharColumn = row.getString("varchar_column");
      if (Objects.equals(varcharColumn, "MaxBoundaryVarchar")) {
        assertAll(
            () -> assertThat(row.isNull("tinyint_column")).isTrue(),
            () -> assertThat(row.isNull("smallint_column")).isTrue(),
            () -> assertThat(row.isNull("int_column")).isTrue(),
            () -> assertThat(row.isNull("bigint_column")).isTrue(),
            () -> assertThat(row.isNull("float_column")).isTrue(),
            () -> assertThat(row.isNull("double_column")).isTrue(),
            () -> assertThat(row.isNull("decimal_column")).isTrue(),
            () -> assertThat(row.isNull("bool_column")).isTrue(),
            () -> assertThat(row.isNull("ascii_column")).isTrue(),
            () -> assertThat(row.isNull("text_column")).isTrue(),
            () -> assertThat(row.isNull("bytes_column")).isTrue(),
            () -> assertThat(row.isNull("date_column")).isTrue(),
            () -> assertThat(row.isNull("time_column")).isTrue(),
            () -> assertThat(row.isNull("timestamp_column")).isTrue(),
            // Maps
            () -> assertThat(row.isNull("map_bool_column")).isTrue(),
            () -> assertThat(row.isNull("map_float_column")).isTrue(),
            () -> assertThat(row.isNull("map_double_column")).isTrue(),
            () -> assertThat(row.isNull("map_tinyint_column")).isTrue(),
            () -> assertThat(row.isNull("map_smallint_column")).isTrue(),
            () -> assertThat(row.isNull("map_int_column")).isTrue(),
            () -> assertThat(row.isNull("map_bigint_column")).isTrue(),
            () -> assertThat(row.isNull("map_varint_column")).isTrue(),
            () -> assertThat(row.isNull("map_decimal_column")).isTrue(),
            () -> assertThat(row.isNull("map_ascii_column")).isTrue(),
            () -> assertThat(row.isNull("map_varchar_column")).isTrue(),
            () -> assertThat(row.isNull("map_blob_column")).isTrue(),
            () -> assertThat(row.isNull("map_date_column")).isTrue(),
            () -> assertThat(row.isNull("map_time_column")).isTrue(),
            () -> assertThat(row.isNull("map_timestamp_column")).isTrue(),
            () -> assertThat(row.isNull("map_duration_column")).isTrue(),
            () -> assertThat(row.isNull("map_uuid_column")).isTrue(),
            () -> assertThat(row.isNull("map_timeuuid_column")).isTrue(),
            () -> assertThat(row.isNull("map_inet_column")).isTrue());
      }
    }
  }

  private void deleteBoundaryValuesInSpanner() {
    KeySet allRows = KeySet.all();
    Mutation deleteAllMutation = Mutation.delete(BOUNDARY_CONVERSION_TABLE, allRows);
    spannerResourceManager.write(deleteAllMutation);
  }

  private void assertCassandraAfterDelete() throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(BOUNDARY_CONVERSION_TABLE) == 0);
    assertThatResult(result).meetsConditions();
  }

  // Helper function to compare two ByteBuffers byte-by-byte
  private boolean compareByteBuffers(ByteBuffer buffer1, ByteBuffer buffer2) {
    if (buffer1.remaining() != buffer2.remaining()) {
      return false;
    }

    for (int i = 0; i < buffer1.remaining(); i++) {
      if (buffer1.get(i) != buffer2.get(i)) {
        return false;
      }
    }
    return true;
  }

  // Utility for debugging, converting ByteBuffer to readable string
  private String keyToString(ByteBuffer buffer) {
    int oldPosition = buffer.position();
    StringBuilder hex = new StringBuilder();
    while (buffer.hasRemaining()) {
      hex.append(String.format("%02x", buffer.get()));
    }
    buffer.position(oldPosition); // reset to original position
    return hex.toString();
  }
}
