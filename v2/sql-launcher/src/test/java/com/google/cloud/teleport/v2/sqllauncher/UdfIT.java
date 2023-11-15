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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Java UDFs. Requires system property "udfProviderJar" to be set. */
@RunWith(JUnit4.class)
public class UdfIT {

  private static final Schema SCHEMA =
      Schema.builder().addNullableField("col", Schema.FieldType.INT64).build();
  @Rule public final TestBigQuery table = TestBigQuery.create(SCHEMA);
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void udf() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString(
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT increment(increment(0) + 1) AS col;",
            System.getProperty("udfProviderJar")));
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
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues(3L).build()));
  }

  @Test
  public void udaf() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);
    TableReference ref = table.tableReference();
    options.setQueryString(
        String.format(
            "CREATE AGGREGATE FUNCTION my_sum(f INT64) RETURNS INT64 LANGUAGE java OPTIONS"
                + " (path='%s'); SELECT my_sum(k) AS col FROM UNNEST([1, 2, 3]) k;",
            System.getProperty("udfProviderJar")));
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
        containsInAnyOrder(Row.withSchema(SCHEMA).addValues(6L).build()));
  }
}
