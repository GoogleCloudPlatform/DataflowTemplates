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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template with custom transformation jar
 * supplied.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbCustomTransformationIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDbCustomTransformationIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbCustomTransformationIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToSourceDbCustomTransformationIT/session.json";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbCustomTransformationIT/mysql-schema.sql";

  private static final String TABLE = "Users1";

  private static final String TABLE2 = "AllDatatypeTransformation";
  private static final HashSet<SpannerToSourceDbCustomTransformationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbCustomTransformationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDbCustomTransformationIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager,
            SpannerToSourceDbCustomTransformationIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.CustomTransformationWithShardForLiveIT")
                .build();
        createAndUploadJarToGcs(gcsResourceManager);
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
                customTransformation,
                MYSQL_SOURCE_TYPE);
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
    for (SpannerToSourceDbCustomTransformationIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testCustomTransformation() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();
    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void writeRowInSpanner() {
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Users1").set("id").to(1).set("name").to("AA BB").build();
    spannerResourceManager.write(m);
    m =
        Mutation.newInsertOrUpdateBuilder("AllDatatypeTransformation")
            .set("varchar_column")
            .to("example2")
            .set("bigint_column")
            .to(1000)
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("bin_column")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("1")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("blob_column")))
            .set("bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 01, 01)))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("decimal_column")
            .to(new BigDecimal("99999.99"))
            .set("double_column")
            .to(123456.123)
            .set("enum_column")
            .to("1")
            .set("float_column")
            .to(12345.67)
            .set("int_column")
            .to(100)
            .set("text_column")
            .to("Sample text for entry 2")
            .set("time_column")
            .to("14:30:00")
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("tinyint_column")
            .to(2)
            .set("year_column")
            .to("2024")
            .build();
    spannerResourceManager.write(m);
    m =
        Mutation.newUpdateBuilder("AllDatatypeTransformation")
            .set("varchar_column")
            .to("example2")
            .set("bigint_column")
            .to(1000)
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("bin_column")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("1")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("blob_column")))
            .set("bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 01, 01)))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("decimal_column")
            .to(new BigDecimal("99999.99"))
            .set("double_column")
            .to(123456.123)
            .set("enum_column")
            .to("1")
            .set("float_column")
            .to(12345.67)
            .set("int_column")
            .to(100)
            .set("text_column")
            .to("Sample text for entry 2")
            .set("time_column")
            .to("14:30:00")
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("tinyint_column")
            .to(2)
            .set("year_column")
            .to("2024")
            .build();
    spannerResourceManager.write(m);
    m = Mutation.delete("AllDatatypeTransformation", Key.of("example2"));
    spannerResourceManager.write(m);
    m =
        Mutation.newInsertBuilder("AllDatatypeTransformation")
            .set("varchar_column")
            .to("example1")
            .set("bigint_column")
            .to(1000)
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("examplebinary1")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("1")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("exampleblob1")))
            .set("bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 01, 01)))
            .set("datetime_column")
            .to(Timestamp.parseTimestamp("2024-01-01T12:34:56Z"))
            .set("decimal_column")
            .to(new BigDecimal("99999.99"))
            .set("double_column")
            .to(123456.123)
            .set("enum_column")
            .to("1")
            .set("float_column")
            .to(12345.67)
            .set("int_column")
            .to(100)
            .set("text_column")
            .to("Sample text for entry 1")
            .set("time_column")
            .to("14:30:00")
            .set("timestamp_column")
            .to(Timestamp.parseTimestamp("2024-01-01T12:34:56Z"))
            .set("tinyint_column")
            .to(1)
            .set("year_column")
            .to("2024")
            .build();
    spannerResourceManager.write(m);
    m =
        Mutation.newInsertBuilder("AllDatatypeTransformation")
            .set("varchar_column")
            .to("example")
            .set("bigint_column")
            .to(12345)
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("Some binary data")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("1")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("Some blob data")))
            .set("bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 01, 01)))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("decimal_column")
            .to(new BigDecimal("12345.67"))
            .set("double_column")
            .to(123.456)
            .set("enum_column")
            .to("1")
            .set("float_column")
            .to(123.45)
            .set("int_column")
            .to(123)
            .set("text_column")
            .to("Sample text")
            .set("time_column")
            .to("14:30:00")
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("tinyint_column")
            .to(1)
            .set("year_column")
            .to("2024")
            .build();
    spannerResourceManager.write(m);
  }

  private void assertRowInMySQL() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                this::assertUsersTable);

    assertThatResult(result).meetsConditions();

    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                this::assertAllDatatypeTransformationTable);

    assertThatResult(result).meetsConditions();
  }

  private boolean assertAllDatatypeTransformationTable() {
    List<Map<String, Object>>  rows =
        jdbcResourceManager.runSQLQuery(
            String.format("select * from %s order by %s", TABLE2, "varchar_column"));
    if (rows.size() != 2) {
      return false;
    }

    Map<String, Object> row1 = rows.get(1);
    if (!row1.get("varchar_column").equals("example2")) {
      return false;
    }
    if (!row1.get("bigint_column").equals(1000)) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row1.get("binary_column"), "bin_column".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row1.get("bit_column"), "1".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row1.get("blob_column"), "blob_column".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!row1.get("bool_column").equals(true)) {
      return false;
    }
    if (!row1.get("date_column").equals(java.sql.Date.valueOf("2024-01-01"))) {
      return false;
    }
    if (!row1.get("datetime_column").equals(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 56))) {
      return false;
    }
    if (!row1.get("decimal_column").equals(new BigDecimal("99999.99"))) {
      return false;
    }
    if (!row1.get("double_column").equals(123456.123)) {
      return false;
    }
    if (!row1.get("enum_column").equals("1")) {
      return false;
    }
    if (!row1.get("float_column").equals(12345.67f)) {
      return false;
    }
    if (!row1.get("int_column").equals(100)) {
      return false;
    }
    if (!row1.get("text_column").equals("Sample text for entry 2")) {
      return false;
    }
    if (!row1.get("time_column").equals(java.sql.Time.valueOf("14:30:00"))) {
      return false;
    }
    if (!row1.get("timestamp_column").equals(java.sql.Timestamp.valueOf("2024-01-01 12:34:56.0"))) {
      return false;
    }
    if (!row1.get("tinyint_column").equals(2)) {
      return false;
    }
    if (!row1.get("year_column").equals(java.sql.Date.valueOf("2024-01-01"))) {
      return false;
    }

    Map<String, Object> row0 = rows.get(0);
    if (!row0.get("varchar_column").equals("example")) {
      return false;
    }
    if (!row0.get("bigint_column").equals(12346)) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row0.get("binary_column"), "binary_column_appended".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row0.get("bit_column"), "5".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!java.util.Arrays.equals((byte[])row0.get("blob_column"), "blob_column_appended".getBytes(StandardCharsets.UTF_8))) {
      return false;
    }
    if (!row0.get("bool_column").equals(false)) {
      return false;
    }
    if (!row0.get("date_column").equals(java.sql.Date.valueOf("2024-01-02"))) {
      return false;
    }
    if (!row0.get("datetime_column").equals(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 55))) {
      return false;
    }
    if (!row0.get("decimal_column").equals(new BigDecimal("12344.67"))) {
      return false;
    }
    if (!row0.get("double_column").equals(124.456)) {
      return false;
    }
    if (!row0.get("enum_column").equals("3")) {
      return false;
    }
    if (!row0.get("float_column").equals(124.45f)) {
      return false;
    }
    if (!row0.get("int_column").equals(124)) {
      return false;
    }
    if (!row0.get("text_column").equals("Sample text append")) {
      return false;
    }
    if (!row0.get("time_column").equals(java.sql.Time.valueOf("14:40:00"))) {
      return false;
    }
    if (!row0.get("timestamp_column").equals(java.sql.Timestamp.valueOf("2024-01-01 12:34:55.0"))) {
      return false;
    }
    if (!row0.get("tinyint_column").equals(2)) {
      return false;
    }
    if (!row0.get("year_column").equals(java.sql.Date.valueOf("2025-01-01"))) {
      return false;
    }

    List<Map<String, Object>> example1Rows =
        jdbcResourceManager.runSQLQuery(
            String.format(
                "select * from %s where %s like '%s'", TABLE2, "varchar_column", "example1"));
    return example1Rows.isEmpty();
  }

  private boolean assertUsersTable() {
    try {
      List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
      return rows.size() == 1
          && rows.get(0).get("id").equals(1)
          && rows.get(0).get("first_name").equals("AA")
          && rows.get(0).get("last_name").equals("BB");
    } catch (Exception e) {
      LOG.error("Error while asserting Users table", e);
      return false;
    }
  }
}
