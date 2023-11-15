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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertThat;

import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.DataCatalogSettings;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubGrpcClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Pubsub write options. */
@RunWith(JUnit4.class)
public class PubsubWriteIT {

  @Rule public final TestPubsub pubsub = TestPubsub.create();
  private DataCatalogClient dataCatalogClient;
  private DataCatalogTableProvider dataCatalogTableProvider;
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() throws IOException {
    DataCatalogPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(DataCatalogPipelineOptions.class);
    dataCatalogClient =
        DataCatalogClient.create(
            DataCatalogSettings.newBuilder()
                .setCredentialsProvider(() -> options.as(GcpOptions.class).getGcpCredential())
                .setEndpoint(options.getDataCatalogEndpoint())
                .build());
    dataCatalogTableProvider = DataCatalogTableProvider.create(options);
  }

  @After
  public void cleanup() {
    dataCatalogClient.close();
  }

  @Test
  public void failNotFound_noDCSchema_setsSchemaAndWritesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl, true as bool");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    Entry entry =
        dataCatalogClient.lookupEntry(
            LookupEntryRequest.newBuilder().setSqlResource(resource).build());
    assertThat(
        String.format(
            "Data Catalog entry <%s> for topic \"%s\" does not have expected schema",
            entry, pubsub.topicPath().getName()),
        entry.getSchema().getColumnsList(),
        containsInAnyOrder(
            allOf(
                hasProperty("column", equalTo("event_timestamp")),
                hasProperty("type", equalTo("TIMESTAMP"))),
            allOf(hasProperty("column", equalTo("str")), hasProperty("type", equalTo("STRING"))),
            allOf(hasProperty("column", equalTo("dbl")), hasProperty("type", equalTo("DOUBLE"))),
            allOf(hasProperty("column", equalTo("bool")), hasProperty("type", equalTo("BOOL")))));
    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty(
                "payload",
                equalTo(
                    "{\"str\":\"foo\",\"dbl\":1.0,\"bool\":true}"
                        .getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
  }

  @Test
  public void failNotFound_matchingDCSchema_sendsMessages() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    dataCatalogTableProvider.setSchemaIfNotPresent(
        resource,
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addStringField("str")
            .addDoubleField("dbl")
            .build());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty(
                "payload",
                equalTo("{\"str\":\"foo\",\"dbl\":1.0}".getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
  }

  @Test
  public void falilNotFound_nonMatchingDCSchema_throwsOnCreation() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    dataCatalogTableProvider.setSchemaIfNotPresent(
        resource,
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addDoubleField("str")
            .addStringField("dbl")
            .build());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    thrown.expectMessage(containsString("schema"));
    thrown.expectMessage(containsString(options.as(GcpOptions.class).getProject()));
    thrown.expectMessage(containsString(pubsub.topicPath().getName()));
    DataflowSqlLauncher.buildPipeline(options);
  }

  @Test
  public void failNotFound_differentNullabilityButAssignable_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    dataCatalogTableProvider.setSchemaIfNotPresent(
        resource,
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addNullableField("str", FieldType.STRING)
            .addNullableField("dbl", FieldType.DOUBLE)
            .build());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();
    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty(
                "payload",
                equalTo("{\"str\":\"foo\",\"dbl\":1.0}".getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
  }

  @Test
  public void failNotFound_schemaFieldsOutOfOrder_writesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    dataCatalogTableProvider.setSchemaIfNotPresent(
        resource,
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addDoubleField("dbl")
            .addStringField("str")
            .build());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();
    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty(
                "payload",
                equalTo("{\"str\":\"foo\",\"dbl\":1.0}".getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
  }

  @Test
  public void createNotFound_noTopic_createsTopic() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String topic = randomTopic();

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"CREATE_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), topic));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    PubsubClient pubsubClient =
        PubsubGrpcClient.FACTORY.newClient(null, null, options.as(PubsubOptions.class));

    // check that the topic exists, and grab schema
    boolean topicExists =
        pubsubClient
            .listTopics(PubsubClient.projectPathFromId(options.as(GcpOptions.class).getProject()))
            .stream()
            .anyMatch(topicPath -> topicPath.getName().equals(topic));

    String resource =
        String.format("pubsub.topic.`%s`.`%s`", options.as(GcpOptions.class).getProject(), topic);
    Entry entry =
        dataCatalogClient.lookupEntry(
            LookupEntryRequest.newBuilder().setSqlResource(resource).build());

    // TODO(bhulette): Assert that topic eventually receives messages
    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    // clean up
    pubsubClient.deleteTopic(
        PubsubClient.topicPathFromName(options.as(GcpOptions.class).getProject(), topic));
    pubsubClient.close();

    assertThat(
        String.format("SQL Launcher did not create topic \"%s\"", topic),
        topicExists,
        equalTo(true));
    assertThat(
        String.format(
            "Data Catalog entry <%s> for topic \"%s\" does not have expected schema", entry, topic),
        entry.getSchema().getColumnsList(),
        containsInAnyOrder(
            allOf(
                hasProperty("column", equalTo("event_timestamp")),
                hasProperty("type", equalTo("TIMESTAMP"))),
            allOf(hasProperty("column", equalTo("str")), hasProperty("type", equalTo("STRING"))),
            allOf(hasProperty("column", equalTo("dbl")), hasProperty("type", equalTo("DOUBLE")))));
  }

  @Test
  public void createNotFound_topicExistsNoDCSchema_setsSchemaAndWritesData() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    options.setQueryString("SELECT \"foo\" as str, 1.0 as dbl");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"CREATE_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    Entry entry =
        dataCatalogClient.lookupEntry(
            LookupEntryRequest.newBuilder().setSqlResource(resource).build());
    assertThat(
        String.format(
            "Data Catalog entry <%s> for topic \"%s\" does not have expected schema",
            entry, pubsub.topicPath().getName()),
        entry.getSchema().getColumnsList(),
        containsInAnyOrder(
            allOf(
                hasProperty("column", equalTo("event_timestamp")),
                hasProperty("type", equalTo("TIMESTAMP"))),
            allOf(hasProperty("column", equalTo("str")), hasProperty("type", equalTo("STRING"))),
            allOf(hasProperty("column", equalTo("dbl")), hasProperty("type", equalTo("DOUBLE")))));
    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty(
                "payload",
                equalTo("{\"str\":\"foo\",\"dbl\":1.0}".getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);

    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
  }

  @Test
  public void createNotFound_complexTypes_publishesCorrectSchema() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    // TODO(bhulette): Test struct output as well. Currently throws:
    // https://github.com/apache/beam/blob/1f64ba3aeb093c77c4d931fb6791b8b239be3f85/sdks/java/extensions/sql/zetasql/src/main/java/org/apache/beam/sdk/extensions/sql/zetasql/translation/ExpressionConverter.java#L566
    String topic = randomTopic();

    options.setQueryString("SELECT [\"foo\",\"bar\",\"baz\"] as arr");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"CREATE_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), topic));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    PubsubClient pubsubClient =
        PubsubGrpcClient.FACTORY.newClient(null, null, options.as(PubsubOptions.class));

    // check that the topic exists, and grab schema
    boolean topicExists =
        pubsubClient
            .listTopics(PubsubClient.projectPathFromId(options.as(GcpOptions.class).getProject()))
            .stream()
            .anyMatch(topicPath -> topicPath.getName().equals(topic));

    String resource =
        String.format("pubsub.topic.`%s`.`%s`", options.as(GcpOptions.class).getProject(), topic);
    Entry entry =
        dataCatalogClient.lookupEntry(
            LookupEntryRequest.newBuilder().setSqlResource(resource).build());

    // TODO(bhulette): Assert that topic eventually receives messages
    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.OVERHEAD),
        equalTo(State.DONE));

    // clean up
    pubsubClient.deleteTopic(
        PubsubClient.topicPathFromName(options.as(GcpOptions.class).getProject(), topic));
    pubsubClient.close();

    assertThat(
        String.format("SQL Launcher did not create topic \"%s\"", topic),
        topicExists,
        equalTo(true));
    assertThat(
        String.format(
            "Data Catalog entry <%s> for topic \"%s\" does not have expected schema", entry, topic),
        entry.getSchema().getColumnsList(),
        containsInAnyOrder(
            allOf(
                hasProperty("column", equalTo("event_timestamp")),
                hasProperty("type", equalTo("TIMESTAMP"))),
            allOf(
                hasProperty("column", equalTo("arr")),
                hasProperty("type", equalTo("STRING")),
                hasProperty("mode", equalTo("REPEATED")))));
  }

  @Test
  public void eventTimestampInOutput_droppedFromPayload() throws Exception {
    DataflowSqlLauncherOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowSqlLauncherOptions.class);

    String resource =
        String.format(
            "pubsub.topic.`%s`.`%s`",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName());

    options.setQueryString("SELECT CURRENT_TIMESTAMP() as event_timestamp, \"foo\" as str");
    options.setOutputs(
        String.format(
            "[{\"type\": \"pubsub\", \"projectId\": \"%s\", \"topic\": \"%s\","
                + " \"createDisposition\": \"FAIL_IF_NOT_FOUND\"}]",
            options.as(GcpOptions.class).getProject(), pubsub.topicPath().getName()));

    PipelineResult result = DataflowSqlLauncher.buildPipeline(options).run();

    Entry entry =
        dataCatalogClient.lookupEntry(
            LookupEntryRequest.newBuilder().setSqlResource(resource).build());
    assertThat(
        String.format(
            "Data Catalog entry <%s> for topic \"%s\" does not have expected schema",
            entry, pubsub.topicPath().getName()),
        entry.getSchema().getColumnsList(),
        containsInAnyOrder(
            allOf(
                hasProperty("column", equalTo("event_timestamp")),
                hasProperty("type", equalTo("TIMESTAMP"))),
            allOf(hasProperty("column", equalTo("str")), hasProperty("type", equalTo("STRING")))));
    assertThat(
        "Pipeline did not finish with state DONE (null indicates a timeout).",
        result.waitUntilFinish(Timeouts.SHUTDOWN),
        equalTo(State.DONE));
    pubsub
        .assertThatTopicEventuallyReceives(
            hasProperty("payload", equalTo("{\"str\":\"foo\"}".getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Timeouts.STARTUP);
  }

  private String randomTopic() {
    return this.getClass().getSimpleName() + "_" + UUID.randomUUID();
  }
}
