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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryPubsubJoinIT {
  private static final Schema TABLE_SCHEMA =
      Schema.of(Field.of("id", Schema.FieldType.INT64), Field.of("name", Schema.FieldType.STRING));

  @Rule public final TestBigQuery table = TestBigQuery.create(TABLE_SCHEMA);
  @Rule public final TestPubsub inputPubsub = TestPubsub.create();
  @Rule public final TestPubsub outputPubsub = TestPubsub.create();
  @Rule public final TestPubsubSignal startSignal = TestPubsubSignal.create();

  // TODO: Add a test verifying a join with struct fields
  @Test
  public void simpleJoin_outputsExpectedData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    table.insertRows(
        TABLE_SCHEMA,
        Row.withSchema(TABLE_SCHEMA).addValues(1L, "foo").build(),
        Row.withSchema(TABLE_SCHEMA).addValues(2L, "bar").build(),
        Row.withSchema(TABLE_SCHEMA).addValues(3L, "baz").build());

    try (DataCatalogTableProvider dataCatalogClient =
        DataCatalogTableProvider.create(
            TestPipeline.testingPipelineOptions().as(DataCatalogPipelineOptions.class))) {

      String inputPubsubResource =
          String.format(
              "pubsub.topic.`%s`.`%s`",
              options.as(GcpOptions.class).getProject(), inputPubsub.topicPath().getName());
      dataCatalogClient.setSchemaIfNotPresent(
          inputPubsubResource,
          Schema.of(
              Field.nullable("event_timestamp", Schema.FieldType.DATETIME),
              Field.of("id", Schema.FieldType.INT64)));
      dataCatalogClient.setSchemaIfNotPresent(
          String.format(
              "pubsub.topic.`%s`.`%s`",
              options.as(GcpOptions.class).getProject(), outputPubsub.topicPath().getName()),
          Schema.of(
              Field.nullable("event_timestamp", Schema.FieldType.DATETIME),
              Field.of("name", Schema.FieldType.STRING)));

      String bqTableResource =
          String.format(
              "bigquery.table.`%s`.`%s`.`%s`",
              options.as(GcpOptions.class).getProject(),
              table.tableReference().getDatasetId(),
              table.tableReference().getTableId());
      options.setQueryString(
          String.format(
              "SELECT bq.name AS name FROM %s p INNER JOIN %s bq ON p.id = bq.id",
              inputPubsubResource, bqTableResource));
      options.setOutputs(
          String.format(
              "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                  + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
              options.as(GcpOptions.class).getProject(), outputPubsub.topicPath().getName()));

      // Verify that we can parse this query and build a pipeline
      Pipeline pipeline = DataflowSqlLauncher.buildPipeline(options);
      // TODO(bhulette): Actually run the pipeline and verify the output
      // Supplier<Void> waitForStart = startSignal.waitForStart(Timeouts.STARTUP);
      // pipeline.apply(startSignal.signalStart());
      // PipelineResult result = pipeline.run();

      //// Wait for the signal that the pipeline has started and is reading data
      // waitForStart.get();

      //// Inject some test data
      // inputPubsub.publish(
      //    ImmutableList.of(
      //        new PubsubMessage("{\"id\": 3}".getBytes(StandardCharsets.UTF_8),
      // ImmutableMap.of()),
      //        new PubsubMessage("{\"id\": 3}".getBytes(StandardCharsets.UTF_8),
      // ImmutableMap.of()),
      //        new PubsubMessage(
      //            "{\"id\": 2}".getBytes(StandardCharsets.UTF_8), ImmutableMap.of())));

      // outputPubsub
      //    .assertThatTopicEventuallyReceives(
      //        hasProperty(
      //            "payload", equalTo("{\"name\":\"baz\"}".getBytes(StandardCharsets.UTF_8))),
      //        hasProperty(
      //            "payload", equalTo("{\"name\":\"baz\"}".getBytes(StandardCharsets.UTF_8))),
      //        hasProperty(
      //            "payload", equalTo("{\"name\":\"bar\"}".getBytes(StandardCharsets.UTF_8))))
      //    .waitForUpTo(Duration.standardSeconds(30));}}
    }
  }
}
