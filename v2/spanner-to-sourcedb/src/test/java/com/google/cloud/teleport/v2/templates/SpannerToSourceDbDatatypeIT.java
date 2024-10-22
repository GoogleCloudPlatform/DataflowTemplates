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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
import org.junit.runners.model.MultipleFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link SpannerToSourceDb} Flex template for all data types. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbDatatypeIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbDatatypeIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbDatatypeIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToSourceDbDatatypeIT/session.json";
  private static final String TABLE1 = "AllDatatypeColumns";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbDatatypeIT/mysql-schema.sql";

  private static HashSet<SpannerToSourceDbDatatypeIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MySQLResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbDatatypeIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDbDatatypeIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager, SpannerToSourceDbDatatypeIT.MYSQL_SCHEMA_FILE_RESOURCE);

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
    for (SpannerToSourceDbDatatypeIT instance : testInstances) {
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
  public void spannerToSourceDataTypeConversionTest()
      throws IOException, InterruptedException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeRowsInSpanner();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> jdbcResourceManager.getRowCount(TABLE1) == 1); // only one row is inserted
    assertThatResult(result).meetsConditions();

    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void writeRowsInSpanner() {
    // Write a single record to Spanner
    Mutation m =
        Mutation.newInsertOrUpdateBuilder(TABLE1)
            .set("varchar_column")
            .to("value1")
            .set("tinyint_column")
            .to(10)
            .set("text_column")
            .to("text_column_value")
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 05, 24)))
            .set("smallint_column")
            .to(50)
            .set("mediumint_column")
            .to(1000)
            .set("int_column")
            .to(50000)
            .set("bigint_column")
            .to(987654321)
            .set("float_column")
            .to(45.67)
            .set("double_column")
            .to(123.789)
            .set("decimal_column")
            .to(new BigDecimal("1234.56"))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-02-08T08:15:30Z")))
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-02-08T08:15:30Z")))
            .set("time_column")
            .to("14:30:00")
            .set("year_column")
            .to("2022")
            .set("char_column")
            .to("char_col")
            .set("tinytext_column")
            .to("tinytext_column_value")
            .set("mediumtext_column")
            .to("mediumtext_column_value")
            .set("longtext_column")
            .to("longtext_column_value")
            .set("tinyblob_column")
            .to(Value.bytes(ByteArray.copyFrom("tinyblob_column_value")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("blob_column_value")))
            .set("mediumblob_column")
            .to(Value.bytes(ByteArray.copyFrom("mediumblob_column_value")))
            .set("longblob_column")
            .to(Value.bytes(ByteArray.copyFrom("longblob_column_value")))
            .set("enum_column")
            .to("2")
            .set("bool_column")
            .to(Value.bool(Boolean.FALSE))
            .set("other_bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("binary_col")))
            .set("varbinary_column")
            .to(Value.bytes(ByteArray.copyFrom("varbinary")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("a")))
            .build();
    spannerResourceManager.write(m);
  }

  private List<Throwable> assertionErrors = new ArrayList<>();

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

  private void assertRowInMySQL() throws InterruptedException, MultipleFailureException {
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE1);
    assertThat(rows).hasSize(1);
    Map<String, Object> row = rows.get(0);
    assertAll(
        () -> assertThat(row.get("varchar_column")).isEqualTo("value1"),
        () -> assertThat(row.get("tinyint_column")).isEqualTo(10),
        () -> assertThat(row.get("text_column")).isEqualTo("text_column_value"),
        () -> assertThat(row.get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-05-24")),
        () -> assertThat(row.get("smallint_column")).isEqualTo(50),
        () -> assertThat(row.get("mediumint_column")).isEqualTo(1000),
        () -> assertThat(row.get("int_column")).isEqualTo(50000),
        () -> assertThat(row.get("bigint_column")).isEqualTo(987654321),
        () -> assertThat(row.get("float_column")).isEqualTo(45.67f),
        () -> assertThat(row.get("double_column")).isEqualTo(123.789),
        () -> assertThat(row.get("decimal_column")).isEqualTo(new BigDecimal("1234.56")),
        () ->
            assertThat(row.get("datetime_column"))
                .isEqualTo(java.time.LocalDateTime.of(2024, 2, 8, 8, 15, 30)),
        () ->
            assertThat(row.get("timestamp_column"))
                .isEqualTo(java.sql.Timestamp.valueOf("2024-02-08 08:15:30.0")),
        () -> assertThat(row.get("time_column")).isEqualTo(java.sql.Time.valueOf("14:30:00")),
        () -> assertThat(row.get("year_column")).isEqualTo(java.sql.Date.valueOf("2022-01-01")),
        () -> assertThat(row.get("char_column")).isEqualTo("char_col"),
        () -> assertThat(row.get("tinytext_column")).isEqualTo("tinytext_column_value"),
        () -> assertThat(row.get("mediumtext_column")).isEqualTo("mediumtext_column_value"),
        () -> assertThat(row.get("longtext_column")).isEqualTo("longtext_column_value"),
        () ->
            assertThat(row.get("tinyblob_column"))
                .isEqualTo("tinyblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.get("blob_column"))
                .isEqualTo("blob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.get("mediumblob_column"))
                .isEqualTo("mediumblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.get("longblob_column"))
                .isEqualTo("longblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () -> assertThat(row.get("enum_column")).isEqualTo("2"),
        () -> assertThat(row.get("bool_column")).isEqualTo(false),
        () -> assertThat(row.get("other_bool_column")).isEqualTo(true),
        () ->
            assertThat(row.get("binary_column"))
                .isEqualTo("binary_col".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.get("varbinary_column"))
                .isEqualTo("varbinary".getBytes(StandardCharsets.UTF_8)),
        () -> assertThat(row.get("bit_column")).isEqualTo("a".getBytes(StandardCharsets.UTF_8)));
  }
}
