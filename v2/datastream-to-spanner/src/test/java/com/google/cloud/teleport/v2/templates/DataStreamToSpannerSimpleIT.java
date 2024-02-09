/*
 * Copyright (C) 2023 Google LLC
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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Simple Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerSimpleIT extends DataStreamToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerSimpleIT.class);

  private static final String TABLE1 = "Users";
  private static final String TABLE2 = "Movie";
  private static final String TABLE3 = "Category";
  private static final String TABLE4 = "AllDatatypeColumns";
  private static final String TABLE5 = "AllDatatypeColumns2";

  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerSimpleIT/mysql-session.json";

  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerSimpleIT/spanner-schema.sql";

  private static HashSet<DataStreamToSpannerSimpleIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerSimpleIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                "SimpleIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "json");
                  }
                });
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
    for (DataStreamToSpannerSimpleIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void migrationTestWithUpdatesAndDeletes() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    // 3. Send second wave of events
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Users.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE1,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-cdc-Users.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertUsersTableContents();
  }

  @Test
  public void migrationTestWithInsertsOnly() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE2,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Movie.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertMovieTableContents();
  }

  @Test
  public void interleavedAndFKAndIndexTest() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        "Articles",
                        "mysql-Articles.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-Articles.jsonl"),
                    uploadDataStreamFile(
                        jobInfo,
                        "Authors",
                        "mysql-Authors.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-Authors.jsonl"),
                    uploadDataStreamFile(
                        jobInfo,
                        "Books",
                        "mysql-Books.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-Books.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, "Articles")
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Books")
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Authors")
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(12)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertAuthorsTable();
    assertBooksTable();
    assertArticlesTable();
  }

  @Test
  public void migrationTestWithAllDatatypeConversionsWithInsertsOnly() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE4,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-AllDatatypeColumns.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE4)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertAllDatatypeColumnsTableBackfillContents();
  }

  @Test
  public void migrationTestWithAllDatatypeConversionsWithUpdatesAndDeletes() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    // 3. Send second wave of events
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE4,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-AllDatatypeColumns.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE4)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE4,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-cdc-AllDatatypeColumns.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE4)
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertAllDatatypeColumnsTableCdcContents();
  }

  @Test
  public void migrationTestWithAllDatatypeMappingsWithInsertsOnly() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE5,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-AllDatatypeColumns2.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE5)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertAllDatatypeColumns2TableBackfillContents();
  }

  @Test
  public void migrationTestWithAllDatatypeMappingsWithUpdatesAndDeletes() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    // 3. Send second wave of events
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE5,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-AllDatatypeColumns2.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE5)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE5,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-cdc-AllDatatypeColumns2.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE5)
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertAllDatatypeColumns2TableCdcContents();
  }

  @Test
  public void migrationTestWithRenameAndDropColumnWithInsertsOnly() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE3,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Category.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE3)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertCategoryTableBackfillContents();
  }

  @Test
  public void migrationTestWithRenameAndDropColumnWithUpdatesAndDeletes() {
    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    // 3. Send second wave of events
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE3,
                        "backfill.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-backfill-Category.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE3)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    uploadDataStreamFile(
                        jobInfo,
                        TABLE3,
                        "cdc1.jsonl",
                        "DataStreamToSpannerSimpleIT/mysql-cdc-Category.jsonl"),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE3)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertCategoryTableCdcContents();
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("name", "Tester Kumar");
    row1.put("age_spanner", 30);
    row1.put("subscribed", false);
    row1.put("plan", "A");
    row1.put("startDate", Date.parseDate("2023-01-01"));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 3);
    row2.put("name", "Tester Gupta");
    row2.put("age_spanner", 50);
    row2.put("subscribed", false);
    row2.put("plan", "Z");
    row2.put("startDate", Date.parseDate("2023-06-07"));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 4);
    row3.put("name", "Tester");
    row3.put("age_spanner", 38);
    row3.put("subscribed", true);
    row3.put("plan", "D");
    row3.put("startDate", Date.parseDate("2023-09-10"));
    events.add(row1);
    events.add(row2);
    events.add(row3);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Users where id in (1, 3, 4)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertMovieTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("name", "movie1");
    row1.put("startTime", Timestamp.parseTimestamp("2023-01-01T12:12:12.000"));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2);
    row2.put("name", "movie2");
    row2.put("startTime", Timestamp.parseTimestamp("2023-11-25T17:10:12.000"));

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, name, startTime from Movie where id in (1, 2)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);

    ImmutableList<Struct> numericVals =
        spannerResourceManager.runQuery("select actor from Movie order by id");
    Assert.assertEquals(123.098, numericVals.get(0).getBigDecimal(0).doubleValue(), 0.001);
    Assert.assertEquals(931.512, numericVals.get(1).getBigDecimal(0).doubleValue(), 0.001);
  }

  private void assertAllDatatypeColumnsTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", "10");
    row.put("text_column", "dGV4dF9kYXRhXzEK");
    row.put("date_column", "2024-02-08T00:00:00.000Z");
    row.put("smallint_column", "50");
    row.put("mediumint_column", "1000");
    row.put("int_column", "50000");
    row.put("bigint_column", "987654321");
    row.put("float_column", "45.67");
    row.put("double_column", "123.789");
    row.put("datetime_column", "2024-02-08T08:15:30.000Z");
    row.put("timestamp_column", "2024-02-08T08:15:30.000Z");
    row.put("time_column", "29730000000");
    row.put("year_column", "2022");
    row.put("char_column", "Y2hhcjEK");
    row.put("tinyblob_column", "74696e79626c6f625f646174615f31");
    row.put("tinytext_column", "dGlueXRleHRfZGF0YV8xCg==");
    row.put("blob_column", "626c6f625f646174615f31");
    row.put("mediumblob_column", "6d656469756d626c6f625f646174615f31");
    row.put("mediumtext_column", "bWVkaXVtdGV4dF9kYXRhXzE=");
    row.put("longblob_column", "6c6f6e67626c6f625f646174615f31");
    row.put("longtext_column", "bG9uZ3RleHRfZGF0YV8x");
    row.put("enum_column", "2");
    row.put("bool_column", 0);
    row.put("other_bool_column", "1");
    row.put("binary_column", "62696e6172795f31");
    row.put("varbinary_column", "76617262696e6172795f646174615f31");
    row.put("bit_column", "102");
    events.add(row);

    row.put("varchar_column", "value2");
    row.put("tinyint_column", "5");
    row.put("text_column", "dGV4dF9kYXRhXzIK");
    row.put("date_column", "2024-02-09T00:00:00.000Z");
    row.put("smallint_column", "25");
    row.put("mediumint_column", "500");
    row.put("int_column", "25000");
    row.put("bigint_column", "987654");
    row.put("float_column", "12.34");
    row.put("double_column", "56.789");
    row.put("datetime_column", "2024-02-09T15:30:45.000Z");
    row.put("timestamp_column", "2024-02-09T15:30:45.000Z");
    row.put("time_column", "55845000000");
    row.put("year_column", "2023");
    row.put("char_column", "Y2hhcjIK");
    row.put("tinyblob_column", "74696e79626c6f625f646174615f32");
    row.put("tinytext_column", "dGlueXRleHRfZGF0YV8yCg==");
    row.put("blob_column", "626c6f625f646174615f32");
    row.put("mediumblob_column", "6d656469756d626c6f625f646174615f32");
    row.put("mediumtext_column", "bWVkaXVtdGV4dF9kYXRhXzI=");
    row.put("longblob_column", "6c6f6e67626c6f625f646174615f32");
    row.put("longtext_column", "bG9uZ3RleHRfZGF0YV8y");
    row.put("enum_column", "3");
    row.put("bool_column", 1);
    row.put("other_bool_column", "0");
    row.put("binary_column", "62696e6172795f32");
    row.put("varbinary_column", "76617262696e6172795f646174615f32");
    row.put("bit_column", "25");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column, "
                    + " longblob_column, longtext_column, enum_column, bool_column, other_bool_column, binary_column"
                    + ", varbinary_column, bit_column from AllDatatypeColumns"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeColumnsTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", "15");
    row.put("text_column", "dGV4dF9kYXRhXzEK");
    row.put("date_column", "2024-02-08T00:00:00.000Z");
    row.put("smallint_column", "50");
    row.put("mediumint_column", "1000");
    row.put("int_column", "50000");
    row.put("bigint_column", "987654321");
    row.put("float_column", "45.67");
    row.put("double_column", "123.789");
    row.put("datetime_column", "2024-02-08T08:15:30.000Z");
    row.put("timestamp_column", "2024-02-08T08:15:30.000Z");
    row.put("time_column", "29730000000");
    row.put("year_column", "2022");
    row.put("char_column", "Y2hhcjEK");
    row.put("tinyblob_column", "74696e79626c6f625f646174615f31");
    row.put("tinytext_column", "dGlueXRleHRfZGF0YV8xCg==");
    row.put("blob_column", "626c6f625f646174615f31");
    row.put("mediumblob_column", "6d656469756d626c6f625f646174615f31");
    row.put("mediumtext_column", "bWVkaXVtdGV4dF9kYXRhXzE=");
    row.put("longblob_column", "6c6f6e67626c6f625f646174615f31");
    row.put("longtext_column", "bG9uZ3RleHRfZGF0YV8x");
    row.put("enum_column", "2");
    row.put("bool_column", 0);
    row.put("other_bool_column", "1");
    row.put("binary_column", "62696e6172795f31");
    row.put("varbinary_column", "76617262696e6172795f646174615f31");
    row.put("bit_column", "102");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column, "
                    + " longblob_column, longtext_column, enum_column, bool_column, other_bool_column, binary_column"
                    + ", varbinary_column, bit_column from AllDatatypeColumns"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeColumns2TableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", 10);
    row.put("text_column", "text1");
    row.put("date_column", "2024-02-08");
    row.put("smallint_column", 50);
    row.put("mediumint_column", 1000);
    row.put("int_column", 50000);
    row.put("bigint_column", 987654321);
    row.put("float_column", 45.67);
    row.put("double_column", 123.789);
    row.put("datetime_column", "2024-02-08T08:15:30Z");
    row.put("timestamp_column", "2024-02-08T08:15:30Z");
    row.put("time_column", "29730000000");
    row.put("year_column", "2022");
    row.put("char_column", "char_1");
    // Source column value: 74696e79626c6f625f646174615f31 ( in BYTES, "tinyblob_data_1" in STRING)
    // results in dGlueWJsb2JfZGF0YV8x base64 encoded string
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8x");
    row.put("tinytext_column", "tinytext_data_1");
    row.put("blob_column", "YmxvYl9kYXRhXzE=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzE=");
    row.put("mediumtext_column", "mediumtext_data_1");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8x");
    row.put("longtext_column", "longtext_data_1");
    row.put("enum_column", "2");
    row.put("bool_column", false);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMQAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMQ==");
    row.put("bit_column", "AQI=");
    events.add(row);

    row.put("varchar_column", "value2");
    row.put("tinyint_column", 5);
    row.put("text_column", "text2");
    row.put("date_column", "2024-02-09");
    row.put("smallint_column", 25);
    row.put("mediumint_column", 500);
    row.put("int_column", 25000);
    row.put("bigint_column", 987654);
    row.put("float_column", 12.34);
    row.put("double_column", 56.789);
    row.put("datetime_column", "2024-02-09T15:30:45Z");
    row.put("timestamp_column", "2024-02-09T15:30:45Z");
    row.put("time_column", "55845000000");
    row.put("year_column", "2023");
    row.put("char_column", "char_2");
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8y");
    row.put("tinytext_column", "tinytext_data_2");
    row.put("blob_column", "YmxvYl9kYXRhXzI=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzI=");
    row.put("mediumtext_column", "mediumtext_data_2");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8y");
    row.put("longtext_column", "longtext_data_2");
    row.put("enum_column", "3");
    row.put("bool_column", true);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", "JQ==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column, "
                    + " longblob_column, longtext_column, enum_column, bool_column, binary_column"
                    + ", varbinary_column, bit_column from AllDatatypeColumns2"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeColumns2TableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", 15);
    row.put("text_column", "text1");
    row.put("date_column", "2024-02-08");
    row.put("smallint_column", 50);
    row.put("mediumint_column", 1000);
    row.put("int_column", 50000);
    row.put("bigint_column", 987654321);
    row.put("float_column", 45.67);
    row.put("double_column", 123.789);
    row.put("datetime_column", "2024-02-08T08:15:30Z");
    row.put("timestamp_column", "2024-02-08T08:15:30Z");
    row.put("time_column", "29730000000");
    row.put("year_column", "2022");
    row.put("char_column", "char_1");
    // Source column value: 74696e79626c6f625f646174615f31 ( in BYTES, "tinyblob_data_1" in STRING)
    // results in dGlueWJsb2JfZGF0YV8x base64 encoded string
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8x");
    row.put("tinytext_column", "tinytext_data_1");
    row.put("blob_column", "YmxvYl9kYXRhXzE=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzE=");
    row.put("mediumtext_column", "mediumtext_data_1");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8x");
    row.put("longtext_column", "longtext_data_1");
    row.put("enum_column", "2");
    row.put("bool_column", false);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMQAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMQ==");
    row.put("bit_column", "AQI=");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column, "
                    + " longblob_column, longtext_column, enum_column, bool_column, binary_column"
                    + ", varbinary_column, bit_column from AllDatatypeColumns2"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertCategoryTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 1);
    row1.put("full_name", "xyz");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 2);
    row2.put("full_name", "abc");

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select category_id, full_name from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertCategoryTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 2);
    row1.put("full_name", "abc1");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 3);
    row2.put("full_name", "def");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("category_id", 4);
    row3.put("full_name", "ghi");

    events.add(row1);
    events.add(row2);
    events.add(row3);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select category_id, full_name from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAuthorsTable() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("author_id", 1);
    row.put("name", "a1");
    events.add(row);

    row = new HashMap<>();
    row.put("author_id", 3);
    row.put("name", "a003");
    events.add(row);

    row = new HashMap<>();
    row.put("author_id", 4);
    row.put("name", "a4");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertBooksTable() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("title", "Book005");
    row.put("author_id", 3);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("title", "Book002");
    row.put("author_id", 3);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("title", "Book004");
    row.put("author_id", 4);
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Books@{FORCE_INDEX=author_id_6}"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertArticlesTable() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "Article001");
    row.put("published_date", Date.parseDate("2024-01-01"));
    row.put("author_id", 1);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("name", "Article002");
    row.put("published_date", Date.parseDate("2024-01-01"));
    row.put("author_id", 1);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("name", "Article003");
    row.put("published_date", Date.parseDate("2024-01-01"));
    row.put("author_id", 4);
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Articles@{FORCE_INDEX=author_id}"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
