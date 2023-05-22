/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.datadog.DatadogEvent;
import com.google.cloud.teleport.datadog.DatadogEventCoder;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Unit tests for {@link com.google.cloud.teleport.templates.common.DatadogConverters} class. */
public class DatadogConvertersTest {

  private static final TupleTag<DatadogEvent> DATADOG_EVENT_OUT = new TupleTag<DatadogEvent>() {};
  private static final TupleTag<FailsafeElement<String, String>> DATADOG_EVENT_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test successful conversion of simple String payloads. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventSimpleStrings() {

    FailsafeElement<String, String> hello = FailsafeElement.of("hello", "hello");
    FailsafeElement<String, String> world = FailsafeElement.of("world", "world");

    pipeline.getCoderRegistry().registerCoderForClass(DatadogEvent.class, DatadogEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(hello, world)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                DatadogConverters.failsafeStringToDatadogEvent(
                    DATADOG_EVENT_OUT, DATADOG_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(DATADOG_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(DATADOG_EVENT_OUT))
        .containsInAnyOrder(
            DatadogEvent.newBuilder().withMessage("hello").withSource("gcp").build(),
            DatadogEvent.newBuilder().withMessage("world").withSource("gcp").build());

    pipeline.run();
  }

  /** Test successful conversion of invalid JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventInvalidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\",\n" + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withMessage("{\n" + "\t\"name\": \"Jim\",\n" + "}").withSource("gcp").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\"\n" + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withMessage("{\n" + "\t\"name\": \"Jim\"\n" + "}").withSource("gcp").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of partial Pubsub (data element only) JSON messages with a resource type. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_DataOnly_WithResourceTypeSource() {
    FailsafeElement<String, String> input = FailsafeElement.of(
        "",
        "{\n" +
            "  \"name\": \"Jim\",\n" +
            "  \"resource\": {\n" +
            "    \"type\": \"resource_type_source\"\n" +
            "  }\n" +
            "}"
    );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage("{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"type\": \"resource_type_source\"\n" +
                "  }\n" +
                "}")
            .withSource("gcp.resource.type.source")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of full PubSub JSON messages with a resource type. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_FullPubsubMessage_WithResourceTypeSource() {
    FailsafeElement<String, String> input = FailsafeElement.of(
        "",
        "{\n" +
            "  \"data\": {\n" +
            "    \"name\": \"Jim\",\n" +
            "    \"resource\": {\n" +
            "      \"type\": \"resource_type_source\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"attributes\": {\n" +
            "    \"test-key\": \"test-value\"\n" +
            "  }\n" +
            "}"
    );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage("{\n" +
                "  \"data\": {\n" +
                "    \"name\": \"Jim\",\n" +
                "    \"resource\": {\n" +
                "      \"type\": \"resource_type_source\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"attributes\": {\n" +
                "    \"test-key\": \"test-value\"\n" +
                "  }\n" +
                "}")
            .withSource("gcp.resource.type.source")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of partial Pubsub (data element only) JSON messages with valid resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_DataOnly_WithResourceLabels_Valid() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"labels\": {\n" +
                "      \"location\": \"us-east1-c\",\n" +
                "      \"project_id\": \"sample-project\",\n" +
                "      \"instance_id\": \"\"\n" +
                "    }\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"name\": \"Jim\",\n" +
                    "  \"resource\": {\n" +
                    "    \"labels\": {\n" +
                    "      \"location\": \"us-east1-c\",\n" +
                    "      \"project_id\": \"sample-project\",\n" +
                    "      \"instance_id\": \"\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .withTags("project_id:sample-project,location:us-east1-c")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of full Pubsub JSON messages with valid resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_FullPubsubMessage_WithResourceLabels_Valid() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"data\": {\n" +
                "    \"name\": \"Jim\",\n" +
                "    \"resource\": {\n" +
                "      \"labels\": {\n" +
                "        \"location\": \"us-east1-c\",\n" +
                "        \"project_id\": \"sample-project\",\n" +
                "        \"instance_id\": \"\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"attributes\": {\n" +
                "    \"test-key\": \"test-value\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"data\": {\n" +
                    "    \"name\": \"Jim\",\n" +
                    "    \"resource\": {\n" +
                    "      \"labels\": {\n" +
                    "        \"location\": \"us-east1-c\",\n" +
                    "        \"project_id\": \"sample-project\",\n" +
                    "        \"instance_id\": \"\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"attributes\": {\n" +
                    "    \"test-key\": \"test-value\"\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .withTags("project_id:sample-project,location:us-east1-c")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of partial Pubsub (data element only) JSON messages with empty resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_DataOnly_WithResourceLabels_Empty() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"labels\": {\n" +
                "    }\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"name\": \"Jim\",\n" +
                    "  \"resource\": {\n" +
                    "    \"labels\": {\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of full Pubsub JSON messages with empty resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_FullPubsubMessage_WithResourceLabels_Empty() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"data\": {\n" +
                "    \"name\": \"Jim\",\n" +
                "    \"resource\": {\n" +
                "      \"labels\": {\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"attributes\": {\n" +
                "    \"test-key\": \"test-value\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"data\": {\n" +
                    "    \"name\": \"Jim\",\n" +
                    "    \"resource\": {\n" +
                    "      \"labels\": {\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"attributes\": {\n" +
                    "    \"test-key\": \"test-value\"\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of partial Pubsub (data element only) JSON messages with invalid resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_DataOnly_WithResourceLabels_Invalid() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"labels\": \"invalid-labels\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"name\": \"Jim\",\n" +
                    "  \"resource\": {\n" +
                    "    \"labels\": \"invalid-labels\"\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of full Pubsub JSON messages with invalid resource labels. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_FullPubsub_WithResourceLabels_Invalid() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"data\": {\n" +
                "    \"name\": \"Jim\",\n" +
                "    \"resource\": {\n" +
                "      \"labels\": \"invalid-labels\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"attributes\": {\n" +
                "    \"test-key\": \"test-value\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\n" +
                    "  \"data\": {\n" +
                    "    \"name\": \"Jim\",\n" +
                    "    \"resource\": {\n" +
                    "      \"labels\": \"invalid-labels\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"attributes\": {\n" +
                    "    \"test-key\": \"test-value\"\n" +
                    "  }\n" +
                    "}"
            )
            .withSource("gcp")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata source. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_WithMetadataSource() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"type\": \"resource_type_source\"\n" +
                "  },\n" +
                "  \"_metadata\": {\n" +
                "    \"ddsource\": \"_metadata_source\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage("{\"resource\":{\"type\":\"resource_type_source\"},\"name\":\"Jim\"}")
            .withSource("_metadata_source")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata tags. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withMetadataTags() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n" +
                "  \"name\": \"Jim\",\n" +
                "  \"resource\": {\n" +
                "    \"labels\": {\n" +
                "      \"location\": \"us-east1-c\",\n" +
                "      \"project_id\": \"sample-project\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"_metadata\": {\n" +
                "    \"ddtags\": \"_metadata-tags\"\n" +
                "  }\n" +
                "}"
        );

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withMessage(
                "{\"resource\":{\"labels\":{\"project_id\":\"sample-project\",\"location\":\"us-east1-c\"}},\"name\":\"Jim\"}"
            )
            .withSource("gcp")
            .withTags("_metadata-tags")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata hostname. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withMetadataHostname() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"hostname\": \"test-host\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withMessage("{\"name\":\"Jim\"}").withHostname("test-host").withSource("gcp").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata service. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withMetadataService() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"service\": \"test-service\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withMessage("{\"name\":\"Jim\"}").withService("test-service").withSource("gcp").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata message. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withMetadataMessage() {
    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"message\": \"test-message\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withMessage("test-message").withSource("gcp").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  @Category(NeedsRunner.class)
  private void matchesDatadogEvent(
      FailsafeElement<String, String> input, DatadogEvent expectedDatadogEvent) {
    pipeline.getCoderRegistry().registerCoderForClass(DatadogEvent.class, DatadogEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                DatadogConverters.failsafeStringToDatadogEvent(
                    DATADOG_EVENT_OUT, DATADOG_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(DATADOG_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(DATADOG_EVENT_OUT)).containsInAnyOrder(expectedDatadogEvent);

    pipeline.run();
  }
}
