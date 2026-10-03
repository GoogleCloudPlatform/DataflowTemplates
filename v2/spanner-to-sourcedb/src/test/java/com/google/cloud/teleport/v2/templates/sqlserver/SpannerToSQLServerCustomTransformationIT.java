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

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
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
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
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
 * supplied targeting SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
@Ignore("This test is disabled currently")
public class SpannerToSQLServerCustomTransformationIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSQLServerCustomTransformationIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerCustomTransformationIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerCustomTransformationIT/session.json";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerCustomTransformationIT/sqlserver-schema.sql";

  private static final String TABLE = "Users1";
  private static final String TABLE2 = "AllDatatypeTransformation";

  private static final HashSet<SpannerToSQLServerCustomTransformationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MSSQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSQLServerCustomTransformationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSQLServerCustomTransformationIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = setUpMSSQLResourceManager(testName);
        createSQLServerSchema(
            jdbcResourceManager,
            SpannerToSQLServerCustomTransformationIT.SQLSERVER_SCHEMA_FILE_RESOURCE);

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
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
              }
            };
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
                SOURCE_SQLSERVER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSQLServerCustomTransformationIT instance : testInstances) {
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
    writeRowInSpanner();
    assertRowInSQLServer();
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
            .to(Value.numeric(new BigDecimal("123.45")))
            .set("double_column")
            .to(Value.float64(123.456))
            .set("enum_column")
            .to(Value.string("1"))
            .set("float_column")
            .to(Value.float64(123.45))
            .set("int_column")
            .to(Value.int64(100))
            .set("text_column")
            .to(Value.string("sample_text"))
            .set("time_column")
            .to(Value.string("12:34:56"))
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-01-01T12:34:56Z")))
            .set("tinyint_column")
            .to(Value.int64(1))
            .set("year_column")
            .to(Value.string("2024"))
            .build();
    spannerResourceManager.write(m);
  }

  private void assertRowInSQLServer() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    jdbcResourceManager.getRowCount(TABLE) == 1
                        && jdbcResourceManager.getRowCount(TABLE2) == 1);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("first_name")).isEqualTo("AA");
    assertThat(rows.get(0).get("last_name")).isEqualTo("BB");

    rows = jdbcResourceManager.readTable(TABLE2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("varchar_column")).isEqualTo("example2");
    assertThat(rows.get(0).get("source_only_pk")).isEqualTo(1);
    assertThat(rows.get(0).get("bigint_column")).isEqualTo(1000L);
    assertThat(new String((byte[]) rows.get(0).get("binary_column"), StandardCharsets.UTF_8).trim())
        .isEqualTo("bin_column");
    assertThat(rows.get(0).get("bit_column")).isEqualTo(true);
    assertThat(new String((byte[]) rows.get(0).get("blob_column"), StandardCharsets.UTF_8).trim())
        .isEqualTo("blob_column");
    assertThat(rows.get(0).get("bool_column")).isEqualTo(true);
    assertThat(rows.get(0).get("date_column")).isEqualTo(java.sql.Date.valueOf("2024-01-01"));
    assertThat(rows.get(0).get("datetime_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:56.0"));
    assertThat(rows.get(0).get("decimal_column")).isEqualTo(new BigDecimal("123.45"));
    assertThat(rows.get(0).get("double_column")).isEqualTo(123.456);
    assertThat(rows.get(0).get("enum_column")).isEqualTo("1");
    assertThat(rows.get(0).get("float_column")).isEqualTo(123.45);
    assertThat(rows.get(0).get("int_column")).isEqualTo(100);
    assertThat(rows.get(0).get("text_column")).isEqualTo("sample_text");
    assertThat(rows.get(0).get("time_column")).isEqualTo(java.sql.Time.valueOf("12:34:56"));
    assertThat(rows.get(0).get("timestamp_column"))
        .isEqualTo(java.sql.Timestamp.valueOf("2024-01-01 12:34:56.0"));
    assertThat(rows.get(0).get("tinyint_column")).isEqualTo((short) 1);
    assertThat(rows.get(0).get("year_column")).isEqualTo(2024);
  }
}
