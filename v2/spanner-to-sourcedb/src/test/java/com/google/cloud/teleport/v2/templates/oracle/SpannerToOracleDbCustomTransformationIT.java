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
package com.google.cloud.teleport.v2.templates.oracle;

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
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
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
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleDbCustomTransformationIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleDbCustomTransformationIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleDbCustomTransformationIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/SpannerToOracleDbCustomTransformationIT/session.json";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleDbCustomTransformationIT/oracle-schema.sql";

  private static final String TABLE = "Users1";
  private static final String TABLE2 = "AllDatatypeTransformation";
  private static final HashSet<SpannerToOracleDbCustomTransformationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static OracleResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleDbCustomTransformationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToOracleDbCustomTransformationIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
        testUsername = setupOracleIsolatedUser(jdbcResourceManager);

        createOracleSchema(
            jdbcResourceManager,
            SpannerToOracleDbCustomTransformationIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsername);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        createAndUploadJarToGcs(gcsResourceManager);
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
                    "input/customShard.jar", "com.custom.CustomTransformationWithOracleForIT")
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
                "oracle",
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToOracleDbCustomTransformationIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testCustomTransformation() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
    writeRowInSpanner();
    assertRowInOracle();
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

  private String readClob(Object clobObj) throws Exception {
    if (clobObj == null) {
      return null;
    }
    if (clobObj instanceof java.sql.Clob) {
      java.sql.Clob clob = (java.sql.Clob) clobObj;
      return clob.getSubString(1, (int) clob.length());
    }
    return clobObj.toString();
  }

  private void assertRowInOracle() throws Exception {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                () ->
                    runIsolatedGetRowCount(jdbcResourceManager, testUsername, "\"" + TABLE + "\"")
                        == 1);

    assertThatResult(result).meetsConditions();

    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                () ->
                    runIsolatedGetRowCount(jdbcResourceManager, testUsername, "\"" + TABLE2 + "\"")
                        == 2);

    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows =
        runIsolatedReadTable(jdbcResourceManager, testUsername, "\"" + TABLE + "\"");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("id")).longValue()).isEqualTo(1L);
    assertThat(rows.get(0).get("first_name")).isEqualTo("AA");
    assertThat(rows.get(0).get("last_name")).isEqualTo("BB");

    rows =
        jdbcResourceManager.runSQLQuery(
            String.format("select * from \"%s\" order by \"%s\"", TABLE2, "varchar_column"));
    assertThat(rows).hasSize(2);
    assertThat(rows.get(1).get("varchar_column")).isEqualTo("example2");
    assertThat(((Number) rows.get(1).get("bigint_column")).longValue()).isEqualTo(1001L);
    assertThat(((Number) rows.get(1).get("int_column")).intValue()).isEqualTo(101);
    assertThat(readClob(rows.get(1).get("text_column")))
        .isEqualTo("Sample text for entry 2 append");

    assertThat(rows.get(0).get("varchar_column")).isEqualTo("example");
    assertThat(((Number) rows.get(0).get("bigint_column")).longValue()).isEqualTo(12346L);
    assertThat(((Number) rows.get(0).get("int_column")).intValue()).isEqualTo(124);
    assertThat(readClob(rows.get(0).get("text_column"))).isEqualTo("Sample text append");

    rows =
        jdbcResourceManager.runSQLQuery(
            String.format(
                "select * from \"%s\" where \"%s\" like '%s'",
                TABLE2, "varchar_column", "example1"));
    assertThat(rows).hasSize(0);
  }

  @org.junit.AfterClass
  public static void flushRedo() {
    SpannerToSourceDbITBase.flushOracleRedoLogs(SharedOracleReverseITContainer.getInstance());
    SpannerToSourceDbITBase.clearIsolatedUser();
  }
}
