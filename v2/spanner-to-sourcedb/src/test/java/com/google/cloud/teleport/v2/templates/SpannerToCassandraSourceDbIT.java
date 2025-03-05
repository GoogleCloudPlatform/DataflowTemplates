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
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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

  private static final String USER_TABLE = "Users";
  private static final String USER_TABLE_2 = "Users2";
  private static final String ALL_DATA_TYPES_TABLE = "AllDatatypeColumns";
  private static final String ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE = "AllDatatypeTransformation";
  private static final String BOUNDARY_CONVERSION_TABLE = "BoundaryConversionTestTable";
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
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadCassandraConfigToGcs(
            gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
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
    assertThatPipeline(jobInfo).isRunning();
    insertBoundaryValuesIntoSpanner();
    assertCassandraBoundaryData();
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
            .build();

    spannerResourceManager.write(mutation);

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
                    .isEqualTo(ByteBuffer.wrap(ByteArray.copyFrom("Hello world").toByteArray())));
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
            () -> assertThat(row.isNull("inet_column")).isTrue());
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
                .isEqualTo(java.math.BigInteger.valueOf(123456789L)));
  }

  private void insertBoundaryValuesIntoSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CONVERSION_TABLE)
            .set("varchar_column")
            .to("SampleVarchar")
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
            .to("R29vZ2xl")
            .set("date_column")
            .to(Date.parseDate("9999-12-31"))
            .set("time_column")
            .to("23:59:59.999999")
            .set("timestamp_column")
            .to(String.valueOf(Timestamp.parseTimestamp("9999-12-31T23:59:59.999999Z")))
            .set("duration_column")
            .to("P4DT1H")
            .set("uuid_column")
            .to("123e4567-e89b-12d3-a456-426614174000")
            .set("timeuuid_column")
            .to("123e4567-e89b-12d3-a456-426614174000")
            .set("inet_column")
            .to("192.168.0.1")
            .set("map_bool_column")
            .to(Value.json("{\"true\": false}"))
            .set("map_float_column")
            .to(
                Value.json(
                    "{\"3.4028235E38\": 1.4E-45, \"Infinity\": \"Infinity\", \"NaN\": \"NaN\"}"))
            .set("map_double_column")
            .to(Value.json("{\"2.718281828459045\": \"2.718281828459045\"}"))
            .set("map_tinyint_column")
            .to(Value.json("{\"127\": \"-128\"}"))
            .set("map_smallint_column")
            .to(Value.json("{\"32767\": \"-32768\"}"))
            .set("map_int_column")
            .to(Value.json("{\"2147483647\": \"-2147483648\"}"))
            .set("map_bigint_column")
            .to(Value.json("{\"9223372036854775807\": 9007199254740993}"))
            .set("map_varint_column")
            .to(Value.json("{\"100000000000000000000\": \"-100000000000000000000\"}"))
            .set("map_decimal_column")
            .to(Value.json("{\"12345.6789\": \"98765.4321\"}"))
            .set("map_ascii_column")
            .to(Value.json("{\"example1\": \"string1\", \"example2\": \"string2\"}"))
            .set("map_varchar_column")
            .to(Value.json("{\"key1\": \"value1\", \"key2\": \"value2\"}"))
            .set("map_blob_column")
            .to(Value.json("{\"R29vZ2xl\": \"Q29tcGFueQ==\"}"))
            .set("map_date_column")
            .to(Value.json("{\"2025-01-27\": \"1995-01-29\"}"))
            .set("map_time_column")
            .to(Value.json("{\"12:30:00\": \"02:45:00\"}"))
            .set("map_timestamp_column")
            .to(Value.json("{\"2025-01-01T00:00:00Z\": \"9999-12-31T23:59:59.999999Z\"}"))
            .set("map_duration_column")
            .to(Value.json("{\"P4DT1H\": \"P4DT1H\"}"))
            .set("map_uuid_column")
            .to(
                Value.json(
                    "{\"123e4567-e89b-12d3-a456-426614174000\": \"321e4567-e89b-12d3-a456-426614174000\"}"))
            .set("map_timeuuid_column")
            .to(
                Value.json(
                    "{\"321e4567-e89b-12d3-a456-426614174000\": \"123e4567-e89b-12d3-a456-426614174000\"}"))
            .set("map_inet_column")
            .to(
                Value.json(
                    "{\"48.49.50.51\": \"::1\",\"3031:3233:3435:3637:3839:4041:4243:4445\": \"::ffff:192.0.2.128\" }"))
            .build();

    spannerResourceManager.write(mutation);
  }

  private void assertCassandraBoundaryData() throws InterruptedException, MultipleFailureException {
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
    // Convert hexadecimal string to a byte array
    String hexString = "476f6f676c65"; // "Google" without "0x" prefix
    byte[] expectedBytes = new byte[hexString.length() / 2];

    for (int i = 0; i < expectedBytes.length; i++) {
      int index = i * 2;
      int j = Integer.parseInt(hexString.substring(index, index + 2), 16);
      expectedBytes[i] = (byte) j;
    }

    assertThat(rows).hasSize(1);
    Row row = rows.iterator().next();
    assertAll(
        // Basic Data Types
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar"),
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
        () -> assertThat(row.getCqlDuration("duration_column").toString()).isEqualTo("4d"),
        () ->
            assertThat(row.getBytesUnsafe("bytes_column"))
                .isEqualTo(ByteBuffer.wrap(expectedBytes)),
        () -> assertThat(row.getLocalDate("date_column")).isEqualTo(LocalDate.parse("9999-12-31")),
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
          Map<Float, Float> expected =
              Map.of(
                  3.4028235E38f,
                  1.4E-45f,
                  Float.POSITIVE_INFINITY,
                  Float.POSITIVE_INFINITY,
                  Float.NaN,
                  Float.NaN);

          Map<Float, Float> actual = row.getMap("map_float_column", Float.class, Float.class);

          // Check if all expected keys exist in the actual map, and assert their values
          expected.forEach(
              (key, expectedValue) -> {
                Assertions.assertThat(actual.containsKey(key))
                    .withFailMessage("Actual map is missing key: %s", key)
                    .isTrue();

                Float actualValue = actual.get(key);

                if (Float.isNaN(expectedValue)) {
                  // Handle NaN separately because NaN is not equal to itself
                  Assertions.assertThat(Float.isNaN(actualValue))
                      .withFailMessage("Value for key %s should be NaN", key)
                      .isTrue();
                } else if (Float.isInfinite(expectedValue)) {
                  // Handle Infinity separately
                  Assertions.assertThat(actualValue)
                      .withFailMessage("Value for key %s should be Infinity", key)
                      .isEqualTo(Float.POSITIVE_INFINITY);
                } else {
                  // Regular comparison
                  Assertions.assertThat(actualValue)
                      .withFailMessage("Value for key %s is incorrect", key)
                      .isEqualTo(expectedValue);
                }
              });

          // Check if the actual map does not have extra keys that are not expected
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
                .isEqualTo(Map.of(2.718281828459045, 2.718281828459045)),
        () ->
            assertThat(row.getMap("map_tinyint_column", Byte.class, Byte.class))
                .isEqualTo(Map.of((byte) 127, (byte) -128)),
        () ->
            assertThat(row.getMap("map_smallint_column", Short.class, Short.class))
                .isEqualTo(Map.of((short) 32767, (short) -32768)),
        () ->
            assertThat(row.getMap("map_int_column", Integer.class, Integer.class))
                .isEqualTo(Map.of(2147483647, -2147483648)),
        () ->
            assertThat(row.getMap("map_bigint_column", Long.class, Long.class))
                .isEqualTo(Map.of(9223372036854775807L, 9007199254740993L)),
        () ->
            assertThat(row.getMap("map_varint_column", BigInteger.class, BigInteger.class))
                .isEqualTo(
                    Map.of(
                        new BigInteger("100000000000000000000"),
                        new BigInteger("-100000000000000000000"))),
        () ->
            assertThat(row.getMap("map_decimal_column", BigDecimal.class, BigDecimal.class))
                .isEqualTo(Map.of(new BigDecimal("12345.6789"), new BigDecimal("98765.4321"))),
        () ->
            assertThat(row.getMap("map_ascii_column", String.class, String.class))
                .isEqualTo(Map.of("example1", "string1", "example2", "string2")),
        () ->
            assertThat(row.getMap("map_varchar_column", String.class, String.class))
                .isEqualTo(Map.of("key1", "value1", "key2", "value2")),
        () -> {
          // Decode base64 to raw byte arrays
          byte[] keyBytes = Base64.getDecoder().decode("R29vZ2xl");
          byte[] valueBytes = Base64.getDecoder().decode("Q29tcGFueQ==");

          // Create expected map
          Map<ByteBuffer, ByteBuffer> expected = new HashMap<>();
          expected.put(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));

          // Fetch actual map from Cassandra
          Map<ByteBuffer, ByteBuffer> actual =
              row.getMap("map_blob_column", ByteBuffer.class, ByteBuffer.class);

          // Iterate and assert equality based on byte content instead of ByteBuffer object
          // references
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
                .isEqualTo(Map.of(LocalDate.parse("2025-01-27"), LocalDate.parse("1995-01-29"))),
        () ->
            assertThat(row.getMap("map_time_column", LocalTime.class, LocalTime.class))
                .isEqualTo(Map.of(LocalTime.parse("12:30:00"), LocalTime.parse("02:45:00"))),
        () ->
            assertThat(row.getMap("map_timestamp_column", Instant.class, Instant.class))
                .isEqualTo(
                    Map.of(
                        java.time.Instant.parse("2025-01-01T00:00:00Z"),
                        java.time.Instant.parse("9999-12-31T23:59:59.999Z"))),
        () ->
            assertThat(row.getMap("map_duration_column", String.class, CqlDuration.class))
                .isEqualTo(Map.of("P4DT1H", CqlDuration.from("4d"))),
        () ->
            assertThat(row.getMap("map_uuid_column", UUID.class, UUID.class))
                .isEqualTo(
                    Map.of(
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                        UUID.fromString("321e4567-e89b-12d3-a456-426614174000"))),
        () ->
            assertThat(row.getMap("map_timeuuid_column", UUID.class, UUID.class))
                .isEqualTo(
                    Map.of(
                        UUID.fromString("321e4567-e89b-12d3-a456-426614174000"),
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))),
        () -> {
          try {
            Map<InetAddress, InetAddress> expected =
                Map.of(
                    InetAddress.getByName("48.49.50.51"), InetAddress.getByName("::1"),
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
