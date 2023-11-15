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
package com.google.cloud.teleport.v2.sqllauncher;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.IOException;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BigQuery write options. */
@RunWith(JUnit4.class)
public class BigQueryWriteIT {

  private static final Schema SCHEMA =
      Schema.of(
          Field.of("str", FieldType.STRING),
          Field.of("dbl", FieldType.DOUBLE),
          Field.of("bool", FieldType.BOOLEAN));
  @Rule public final TestBigQuery table = TestBigQuery.create(SCHEMA);
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void writeEmpty_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true as bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeEmpty_emptyTable_writesData_forceStreaming() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true as bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    options.as(StreamingOptions.class).setStreaming(true);
    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeEmptyWithNamedParameters_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT @x AS str, @y AS dbl, @z as bool");
    options.setQueryParameters(
        "[{\"parameterType\": {\"type\": \"STRING\"}, \"parameterValue\": {\"value\": \"foo\"},"
            + " \"name\": \"x\"}, {\"parameterType\": {\"type\": \"FLOAT64\"}, \"parameterValue\":"
            + " {\"value\": \"1.0\"}, \"name\": \"y\"}, {\"parameterType\": {\"type\": \"BOOL\"},"
            + " \"parameterValue\": {\"value\":true}, \"name\": \"z\"}]");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeEmptyWithPositionalParameters_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT ? AS str, ? AS dbl, ? AS bool");
    options.setQueryParameters(
        "[{\"parameterType\": {\"type\": \"STRING\"}, \"parameterValue\": {\"value\": \"foo\"}},"
            + " {\"parameterType\": {\"type\": \"FLOAT64\"}, \"parameterValue\": {\"value\":"
            + " \"1.0\"}}, {\"parameterType\": {\"type\": \"BOOL\"}, \"parameterValue\":"
            + " {\"value\": true}}]");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeEmptyWithStructParameter_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString(
        "SELECT @struct_value.a AS str, @struct_value.b AS dbl, @struct_value.c AS bool");
    options.setQueryParameters(
        "[{\"name\": \"struct_value\", \"parameterType\": {\"structTypes\": [{\"name\": \"a\","
            + " \"type\": {\"type\": \"STRING\"}}, {\"name\": \"b\", \"type\": {\"type\":"
            + " \"FLOAT64\"}},{\"name\": \"c\", \"type\": {\"type\": \"BOOL\"}}], \"type\":"
            + " \"STRUCT\"}, \"parameterValue\": {\"structValues\": {\"a\": {\"value\": \"foo\"},"
            + " \"b\": {\"value\": 1.0}, \"c\": {\"value\": true}}}}]");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeEmpty_nonEmptyTable_throwsOnCreation() throws Exception {
    Row existingRow = Row.withSchema(SCHEMA).addValues("existing row", 1.414, false).build();

    insertRowAndWait(table, existingRow);

    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true AS bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_EMPTY\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    thrown.expectMessage(containsString("not empty"));
    DataflowSqlLauncher.buildPipeline(options).run();
  }

  private static void insertRowAndWait(TestBigQuery table, Row row) throws IOException {
    table.insertRows(row.getSchema(), row);

    // Block until we know pipeline construction will observe a non-empty table
    assertEventuallyNonempty(table, Duration.standardSeconds(30));
  }

  private static void assertEventuallyNonempty(TestBigQuery table, Duration duration)
      throws IOException {
    BigQuery client = BigQueryOptions.getDefaultInstance().getService();

    boolean tableEmpty = true;
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(30))
            .backoff();
    while (true) {
      try {
        if (client
                .query(
                    QueryJobConfiguration.newBuilder(
                            "SELECT false FROM (SELECT AS STRUCT * FROM `"
                                + table.tableReference().getProjectId()
                                + "`.`"
                                + table.tableReference().getDatasetId()
                                + "`.`"
                                + table.tableReference().getTableId()
                                + "` LIMIT 1) AS i WHERE i IS NOT NULL")
                        .setUseLegacySql(false)
                        .build())
                .getTotalRows()
            > 0) {
          tableEmpty = false;
          break;
        }
        BackOffUtils.next(Thread::sleep, backoff);
      } catch (InterruptedException ignored) {
      }
    }

    assertThat("Table did not contain data after " + duration, tableEmpty, is(false));
  }

  @Test
  public void writeAppend_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true AS bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_APPEND\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  @Ignore("b/251298302 flakes on read of existing row")
  public void writeAppend_nonEmptyTable_appendsData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    insertRowAndWait(table, Row.withSchema(SCHEMA).addValues("existing row", 1.414, false).build());

    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true AS bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_APPEND\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(
            Row.withSchema(SCHEMA).addValues("existing row", 1.414, false).build(),
            Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeTruncate_emptyTable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true as bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_APPEND\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }

  @Test
  public void writeTruncate_nonEmptyTable_overwritesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    insertRowAndWait(table, Row.withSchema(SCHEMA).addValues("existing row", 1.414, false).build());

    TableReference ref = table.tableReference();
    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true as bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"bigquery\", \"table\": {\"projectId\": \"%s\", \"datasetId\": \"%s\","
                + " \"tableId\": \"%s\"}, \"writeDisposition\": \"WRITE_TRUNCATE\"}]",
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId()));

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        DataflowSqlLauncher.buildPipeline(options).run().waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    assertThat(
        table.getFlatJsonRows(SCHEMA),
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues("foo", 1.0, true).build()));
  }
}
