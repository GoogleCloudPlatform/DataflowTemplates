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

import static com.google.common.truth.Truth.assertThat;
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
import org.junit.Ignore;
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

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
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
                customTransformation);
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
  @Ignore("This test seems flaky, ignoring it for now to unblock other tests")
  public void spannerToSourceDbWithCustomTransformation() throws InterruptedException {
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

  private void assertRowInMySQL() {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE) == 1);
    assertThatResult(result).meetsConditions();

    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE2) == 2);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("first_name")).isEqualTo("AA");
    assertThat(rows.get(0).get("last_name")).isEqualTo("BB");

    rows =
        jdbcResourceManager.runSQLQuery(
            String.format("select * from %s order by %s", TABLE2, "varchar_column"));
    assertThat(rows).hasSize(2);
    assertThat(rows.get(1).get("varchar_column")).isEqualTo("example2");
    assertThat(rows.get(1).get("bigint_column")).isEqualTo(1000);
    assertThat(rows.get(1).get("binary_column"))
        .isEqualTo("bin_column".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(1).get("bit_column")).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(1).get("blob_column"))
        .isEqualTo("blob_column".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(1).get("bool_column")).isEqualTo(true);
    assertThat(rows.get(1).get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-01-01"));
    assertThat(rows.get(1).get("datetime_column"))
        .isEqualTo(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 56));
    assertThat(rows.get(1).get("decimal_column")).isEqualTo(new BigDecimal("99999.99"));
    assertThat(rows.get(1).get("double_column")).isEqualTo(123456.123);
    assertThat(rows.get(1).get("enum_column")).isEqualTo("1");
    assertThat(rows.get(1).get("float_column")).isEqualTo(12345.67f);
    assertThat(rows.get(1).get("int_column")).isEqualTo(100);
    assertThat(rows.get(1).get("text_column")).isEqualTo("Sample text for entry 2");
    assertThat(rows.get(1).get("time_column")).isEqualTo(java.sql.Time.valueOf("14:30:00"));
    assertThat(rows.get(1).get("timestamp_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:56.0"));
    assertThat(rows.get(1).get("tinyint_column")).isEqualTo(2);
    assertThat(rows.get(1).get("year_column")).isEqualTo(java.sql.Date.valueOf("2024-01-01"));

    assertThat(rows.get(0).get("varchar_column")).isEqualTo("example");
    assertThat(rows.get(0).get("bigint_column")).isEqualTo(12346);
    assertThat(rows.get(0).get("binary_column"))
        .isEqualTo("binary_column_appended".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(0).get("bit_column")).isEqualTo("5".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(0).get("blob_column"))
        .isEqualTo("blob_column_appended".getBytes(StandardCharsets.UTF_8));
    assertThat(rows.get(0).get("bool_column")).isEqualTo(false);
    assertThat(rows.get(0).get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-01-02"));
    assertThat(rows.get(0).get("datetime_column"))
        .isEqualTo(java.time.LocalDateTime.of(2024, 1, 1, 12, 34, 55));
    assertThat(rows.get(0).get("decimal_column")).isEqualTo(new BigDecimal("12344.67"));
    assertThat(rows.get(0).get("double_column")).isEqualTo(124.456);
    assertThat(rows.get(0).get("enum_column")).isEqualTo("3");
    assertThat(rows.get(0).get("float_column")).isEqualTo(124.45f);
    assertThat(rows.get(0).get("int_column")).isEqualTo(124);
    assertThat(rows.get(0).get("text_column")).isEqualTo("Sample text append");
    assertThat(rows.get(0).get("time_column")).isEqualTo(java.sql.Time.valueOf("14:40:00"));
    assertThat(rows.get(0).get("timestamp_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:55.0"));
    assertThat(rows.get(0).get("tinyint_column")).isEqualTo(2);
    assertThat(rows.get(0).get("year_column")).isEqualTo(java.sql.Date.valueOf("2025-01-01"));

    rows =
        jdbcResourceManager.runSQLQuery(
            String.format(
                "select * from %s where %s like '%s'", TABLE2, "varchar_column", "example1"));
    assertThat(rows).hasSize(0);
  }
}
