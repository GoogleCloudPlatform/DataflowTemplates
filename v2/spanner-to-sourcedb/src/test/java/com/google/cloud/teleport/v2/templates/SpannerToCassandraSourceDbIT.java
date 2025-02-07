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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
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
  private static final String ALL_DATA_TYPES_TABLE = "AllDatatypeColumns";
  private static final String ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE = "AllDatatypeTransformation";
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
                "cassandra");
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
    Mutation m = Mutation.delete(USER_TABLE, Key.of(1, 3));
    spannerResourceManager.write(m);
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
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount(USER_TABLE) == 0);
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

  /**
   * Writes basic rows to multiple tables in Google Cloud Spanner.
   *
   * <p>This method performs the following operations:
   *
   * <ul>
   *   <li>Inserts or updates a row in the "users" table with an ID of 1.
   *   <li>Inserts or updates a row in the "users2" table with an ID of 2.
   *   <li>Executes a transactionally buffered insert/update operation in the "users" table with an
   *       ID of 3, using a transaction tag for tracking.
   * </ul>
   *
   * The transaction uses a Spanner client with a specific transaction tag
   * ("txBy=forwardMigration").
   */
  private void writeBasicRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder("users")
            .set("id")
            .to(1)
            .set("full_name")
            .to("A")
            .set("from")
            .to("B")
            .build();
    spannerResourceManager.write(m1);

    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder("users2")
            .set("id")
            .to(2)
            .set("full_name")
            .to("BB")
            .build();
    spannerResourceManager.write(m2);

    // Write a single record to Spanner for the given logical shard
    // Add the record with the transaction tag as txBy=
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(PROJECT)
            .withInstanceId(spannerResourceManager.getInstanceId())
            .withDatabaseId(spannerResourceManager.getDatabaseId());
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag("txBy=forwardMigration"),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                transaction -> {
                  Mutation m3 =
                      Mutation.newInsertOrUpdateBuilder("users")
                          .set("id")
                          .to(3)
                          .set("full_name")
                          .to("GG")
                          .set("from")
                          .to("BB")
                          .build();
                  transaction.buffer(m3);
                  return null;
                });
  }

  /**
   * Asserts that a basic row exists in the Cassandra database.
   *
   * <p>This method performs the following steps:
   *
   * <ul>
   *   <li>Waits for the condition that ensures one row exists in the Cassandra table {@code
   *       USER_TABLE}.
   *   <li>Retrieves and logs rows from the Cassandra table.
   *   <li>Checks if exactly one row is present in the table.
   *   <li>Verifies that the row contains expected values for columns: {@code id}, {@code
   *       full_name}, and {@code from}.
   * </ul>
   *
   * @throws InterruptedException if the thread is interrupted while waiting for the row count
   *     condition.
   * @throws RuntimeException if reading from the Cassandra table fails.
   */
  private void assertBasicRowInCassandraDB() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount(USER_TABLE) == 1);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      LOG.info("Reading from Cassandra table: {}", USER_TABLE);
      rows = cassandraResourceManager.readTable(USER_TABLE);
      LOG.info("Cassandra Rows: {}", rows.toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + USER_TABLE, e);
    }

    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    LOG.info("Cassandra Row to Assert: {}", row.toString());
    assertThat(row.getInt("id")).isEqualTo(1);
    assertThat(row.getString("full_name")).isEqualTo("A");
    assertThat(row.getString("from")).isEqualTo("B");
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

  /**
   * Validates that all data type rows inserted in Spanner have been correctly migrated and stored
   * in Cassandra.
   *
   * <p>This method ensures that the data in the Cassandra table {@code ALL_DATA_TYPES_TABLE}
   * matches the expected values after migration. It waits for the pipeline to process the data,
   * reads the data from Cassandra, and asserts all column values.
   *
   * <p><b>Assertions:</b>
   *
   * <ul>
   *   <li>Basic Data Types - Ensures correct values for varchar, bigint, bool, char, date,
   *       datetime, decimal, double, float.
   *   <li>Collections - Validates frozen lists, sets, and maps including nested structures.
   *   <li>Lists and Sets - Ensures list and set columns contain expected elements.
   *   <li>Maps - Validates various map column structures including text-to-int, date-to-text, and
   *       list/set mappings.
   * </ul>
   *
   * @throws InterruptedException if the thread is interrupted while waiting for pipeline execution.
   * @throws MultipleFailureException if multiple assertion failures occur.
   */
  private void assertAllDataTypeRowsInCassandraDB()
      throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(ALL_DATA_TYPES_TABLE) == 1);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(ALL_DATA_TYPES_TABLE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + ALL_DATA_TYPES_TABLE, e);
    }

    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();

    assertThat(rows).hasSize(1);
    assertAll(
        // Basic Data Types
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar"),
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
                .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00.123456Z")),
        () ->
            assertThat(row.getBigInteger("varint_column"))
                .isEqualTo(java.math.BigInteger.valueOf(123456789L)),
        () ->
            assertThat(row.getBytesUnsafe("bytes_column"))
                .isEqualTo(ByteBuffer.wrap(ByteArray.copyFrom("Hello world").toByteArray())));
  }

  /**
   * Inserts multiple rows into the Spanner table {@code ALL_DATA_TYPES_CUSTOM_CONVERSION_TABLE},
   * ensuring that all values are stored as strings, regardless of their original data type.
   *
   * <p>This method writes sample data to the Spanner table, converting all numerical, boolean, and
   * date/time values to their string representations. This ensures compatibility for scenarios
   * requiring string-based storage.
   *
   * <p><b>Columns and Data Mapping:</b>
   *
   * <ul>
   *   <li><b>Basic Types:</b> Strings, numbers (converted to strings), booleans.
   *   <li><b>Complex Types:</b> JSON representations for lists, sets, and maps.
   *   <li><b>Temporal Types:</b> Date, datetime, timestamp values stored as strings.
   * </ul>
   */
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

  /**
   * Validates that string-based data stored in Spanner is correctly converted to its actual data
   * types when retrieved from Cassandra.
   *
   * <p>This method ensures that values stored as strings in Spanner are properly transformed into
   * their expected data types in Cassandra. It performs the following:
   *
   * <ul>
   *   <li>Waits for the migration process to complete.
   *   <li>Reads and verifies that two rows are present in Cassandra.
   *   <li>Checks specific column values to confirm correct data type conversion.
   * </ul>
   *
   * <p><b>Assertions Performed:</b>
   *
   * <ul>
   *   <li>Verifies that {@code varchar_column} retains its expected string value.
   *   <li>Confirms that {@code tinyint_column} is correctly converted to a {@code byte}.
   * </ul>
   *
   * @throws MultipleFailureException if multiple assertions fail during validation.
   */
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
    assertAll(
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar"),
        () -> assertThat(row.getByte("tinyint_column")).isEqualTo((byte) 122));
  }
}
