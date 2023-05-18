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

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.datadog.DatadogEvent;
import com.google.cloud.teleport.datadog.DatadogEventCoder;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
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

  private static final Gson GSON = new Gson();

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
            DatadogEvent.newBuilder().withEvent("hello").build(),
            DatadogEvent.newBuilder().withEvent("world").build());

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
        DatadogEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\",\n" + "}").build();

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
        DatadogEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\"\n" + "}").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a valid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent(
                "{\n"
                    + "\t\"name\": \"Jim\",\n"
                    + "\t\"logName\": \"test-log-name\",\n"
                    + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                    + "}")
            .withTime(DateTime.parseRfc3339("2019-10-15T11:32:26.553Z").getValue())
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with an invalid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventInValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent(
                "{\n"
                    + "\t\"name\": \"Jim\",\n"
                    + "\t\"logName\": \"test-log-name\",\n"
                    + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                    + "}")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidSource() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"source\": \"test-log-name\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withSource("test-log-name")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided host. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidHost() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withEvent("{\"name\":\"Jim\"}").withHost("test-host").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with a user provided index. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidIndex() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\","
                + "\"index\":\"test-index\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withHost("test-host")
            .withIndex("test-index")
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with user provided index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withValidFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"fields\":{\"test-key\":\"test-value\"}}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withFields(GSON.fromJson("{\"test-key\":\"test-value\"}", JsonObject.class))
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with user provided empty index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withEmptyFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "", "{\n" + "\t\"name\": \"Jim\",\n" + "\t\"_metadata\": {\"fields\":{}}\n" + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withFields(GSON.fromJson("{}", JsonObject.class))
            .build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with user provided invalid index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEvent_withInvalidFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"fields\":\"invalid-json-fields\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder().withEvent("{\"name\":\"Jim\"}").build();

    matchesDatadogEvent(input, expectedDatadogEvent);
  }

  /** Test successful conversion of JSON messages with provided overrides for time and source. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToDatadogEventValidTimeOverride() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\",\n"
                + "\t\"_metadata\": {\"time\": \"2019-11-22T11:32:26.553Z\", "
                + "\"source\": \"test-source-name\"}\n"
                + "}");

    DatadogEvent expectedDatadogEvent =
        DatadogEvent.newBuilder()
            .withEvent("{" + "\"timestamp\":\"2019-10-15T11:32:26.553Z\"" + "}")
            .withSource("test-source-name")
            .withTime(DateTime.parseRfc3339("2019-11-22T11:32:26.553Z").getValue())
            .build();

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
