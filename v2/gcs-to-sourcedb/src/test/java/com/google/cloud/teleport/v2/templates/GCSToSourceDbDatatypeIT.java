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
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link GCSToSourceDb} Flex template with launching reader job. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class GCSToSourceDbDatatypeIT extends GCSToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbDatatypeIT.class);
  private static final String SPANNER_DDL_RESOURCE = "GCSToSourceDbDatatypeIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "GCSToSourceDbDatatypeIT/session.json";
  private static final String TABLE1 = "AllDatatypeColumns";

  private static HashSet<GCSToSourceDbDatatypeIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo writerJobInfo;
  private static PipelineLauncher.LaunchInfo readerJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static List<CustomMySQLResourceManager> jdbcResourceManagers;
  public static GcsResourceManager gcsResourceManager;
  public static FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (GCSToSourceDbDatatypeIT.class) {
      testInstances.add(this);
      if (writerJobInfo == null) {
        setupResourceManagers(SPANNER_DDL_RESOURCE, SESSION_FILE_RESOURSE, 1);

        createMySQLSchema(jdbcResourceManagers);
        readerJobInfo =
            launchReaderDataflowJob(
                gcsResourceManager, spannerResourceManager, spannerMetadataResourceManager);
        writerJobInfo = launchWriterDataflowJob(gcsResourceManager, spannerMetadataResourceManager);
      }
    }
  }

  public void setupResourceManagers(
      String spannerDdlResource, String sessionFileResource, int numShards) throws IOException {
    spannerResourceManager = createSpannerDatabase(spannerDdlResource);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    jdbcResourceManagers = new ArrayList<>();
    for (int i = 0; i < numShards; ++i) {
      jdbcResourceManagers.add(CustomMySQLResourceManager.builder(testName).build());
    }

    gcsResourceManager = createGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManagers);
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResource).getPath());
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (GCSToSourceDbDatatypeIT instance : testInstances) {
      instance.tearDownBase();
    }
    cleanupResourceManagers(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        flexTemplateDataflowJobResourceManager,
        jdbcResourceManagers);
  }

  @Test
  public void gcsToSourceDataTypeConversionTest()
      throws IOException, InterruptedException, MultipleFailureException {
    assertThatPipeline(readerJobInfo).isRunning();
    assertThatPipeline(writerJobInfo).isRunning();
    // Write row in Spanner
    writeRowsInSpanner();

    JDBCRowsCheck rowsCheck =
        JDBCRowsCheck.builder(jdbcResourceManagers.get(0), TABLE1)
            .setMinRows(1)
            .setMaxRows(1)
            .build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(writerJobInfo, Duration.ofMinutes(5)), rowsCheck);
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
    List<Map<String, Object>> rows = jdbcResourceManagers.get(0).readTable(TABLE1);
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
                .isEqualTo("binary_column_value1".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.get("varbinary_column"))
                .isEqualTo("varbinary".getBytes(StandardCharsets.UTF_8)),
        () -> assertThat(row.get("bit_column")).isEqualTo("a".getBytes(StandardCharsets.UTF_8)));
  }

  private void createMySQLSchema(List<CustomMySQLResourceManager> jdbcResourceManagers) {
    CustomMySQLResourceManager jdbcResourceManager = jdbcResourceManagers.get(0);
    HashMap<String, String> columns = new HashMap<>();
    columns.put("varchar_column", "varchar(20) NOT NULL");
    columns.put("tinyint_column", "tinyint");
    columns.put("text_column", "text");
    columns.put("date_column", "date");
    columns.put("smallint_column", "smallint");
    columns.put("mediumint_column", "mediumint");
    columns.put("int_column", "int");
    columns.put("bigint_column", "bigint");
    columns.put("float_column", "float(10,2)");
    columns.put("double_column", "double");
    columns.put("decimal_column", "decimal(10,2)");
    columns.put("datetime_column", "datetime");
    columns.put("timestamp_column", "timestamp");
    columns.put("time_column", "time");
    columns.put("year_column", "year");
    columns.put("char_column", "char(10)");
    columns.put("tinyblob_column", "tinyblob");
    columns.put("tinytext_column", "tinytext");
    columns.put("blob_column", "blob");
    columns.put("mediumblob_column", "mediumblob");
    columns.put("mediumtext_column", "mediumtext");
    columns.put("longblob_column", "longblob");
    columns.put("longtext_column", "longtext");
    columns.put("enum_column", "enum('1','2','3')");
    columns.put("bool_column", "tinyint(1)");
    columns.put("other_bool_column", "tinyint(1)");
    columns.put("binary_column", "binary(20)");
    columns.put("varbinary_column", "varbinary(20)");
    columns.put("bit_column", "bit(7)");
    JDBCResourceManager.JDBCSchema schema =
        new JDBCResourceManager.JDBCSchema(columns, "varchar_column");

    jdbcResourceManager.createTable("AllDatatypeColumns", schema);
  }
}
