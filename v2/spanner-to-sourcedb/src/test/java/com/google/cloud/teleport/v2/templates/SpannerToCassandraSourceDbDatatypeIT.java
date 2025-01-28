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
import com.google.cloud.spanner.Mutation;
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
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDbDatatypeIT extends SpannerToCassandraDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToCassandraSourceDbDatatypeIT/spanner-schema.sql";
  private static final String CASSANDRA_SCHEMA_FILE_RESOURCE =
      "SpannerToCassandraSourceDbDatatypeIT/cassandra-schema.sql";

  private static final String TABLE = "AllDatatypeColumns";
  private static final HashSet<SpannerToCassandraSourceDbDatatypeIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraSharedResourceManager cassandraResourceManager;
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
    synchronized (SpannerToCassandraSourceDbDatatypeIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        createAndUploadCassandraConfigToGcs(gcsResourceManager, cassandraResourceManager);
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
                null);
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
    for (SpannerToCassandraSourceDbDatatypeIT instance : testInstances) {
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
  public void spannerToCassandraSourceDataTypeConversionTest()
      throws InterruptedException, IOException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowInSpanner();
    assertRowInCassandraDB();
  }

  private long getRowCount() {
    String query = String.format("SELECT COUNT(*) FROM %s", TABLE);
    ResultSet resultSet = cassandraResourceManager.executeStatement(query);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + TABLE);
    }
  }

  private void writeRowInSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(TABLE)
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
            .to(Value.bytes(ByteArray.copyFrom("SGVsbG8gd29ybGQ=".getBytes())))
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
            .to(Value.bytes(ByteArray.copyFrom("b3f5ed4f".getBytes())))
            .build();

    spannerResourceManager.write(mutation);
  }

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

  private void assertRowInCassandraDB() throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount() == 1);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(TABLE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + TABLE, e);
    }

    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    System.out.println(row.getFormattedContents());

    assertThat(rows).hasSize(1);
    assertAll(
        // Basic Data Types
        () -> assertThat(row.getString("varchar_column")).isEqualTo("SampleVarchar"),
        () -> assertThat(row.getLong("bigint_column")).isEqualTo(9223372036854775807L),
        () -> assertThat(row.getBoolean("bool_column")).isTrue(),
        () -> {
          String hexString = "5347567362473867643239796247513d";
          byte[] byteArray;
          try {
            byteArray = Hex.decodeHex(hexString);
          } catch (DecoderException e) {
            byteArray = new byte[0];
          }
          ByteBuffer expectedBuffer = ByteBuffer.wrap(byteArray);
          assertThat(row.getByteBuffer("bytes_column")).isEqualTo(expectedBuffer);
        },
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
                .isEqualTo(java.time.Instant.parse("2025-01-27T10:30:00.000Z")),
        () ->
            assertThat(row.getBigInteger("varint_column"))
                .isEqualTo(java.math.BigInteger.valueOf(7076111819049546854L)));
  }
}
