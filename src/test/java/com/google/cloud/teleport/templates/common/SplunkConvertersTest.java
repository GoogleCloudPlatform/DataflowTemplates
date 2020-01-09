/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.templates.common;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.splunk.SplunkEvent;
import com.google.cloud.teleport.splunk.SplunkEventCoder;
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

/** Unit tests for {@link com.google.cloud.teleport.templates.common.SplunkConverters} class. */
public class SplunkConvertersTest {

  private static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};
  private static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test successful conversion of simple String payloads. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventSimpleStrings() {

    FailsafeElement<String, String> hello = FailsafeElement.of("hello", "hello");
    FailsafeElement<String, String> world = FailsafeElement.of("world", "world");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(hello, world)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder().withEvent("hello").build(),
            SplunkEvent.newBuilder().withEvent("world").build());

    pipeline.run();
  }

  /** Test successful conversion of invalid JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventInvalidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\",\n" + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\",\n" + "}").build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\"\n" + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\"\n" + "}").build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with a valid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder()
                .withEvent(
                    "{\n"
                        + "\t\"name\": \"Jim\",\n"
                        + "\t\"logName\": \"test-log-name\",\n"
                        + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                        + "}")
                .withTime(DateTime.parseRfc3339("2019-10-15T11:32:26.553Z").getValue())
                .build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with an invalid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventInValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder()
                .withEvent(
                    "{\n"
                        + "\t\"name\": \"Jim\",\n"
                        + "\t\"logName\": \"test-log-name\",\n"
                        + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                        + "}")
                .build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with a user provided _metadata. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidSource() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"source\": \"test-log-name\"}\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder()
                .withEvent("{\"name\":\"Jim\"}")
                .withSource("test-log-name")
                .build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with a user provided host. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidHost() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\"}\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder().withEvent("{\"name\":\"Jim\"}").withHost("test-host").build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with a user provided index. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidIndex() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\","
                + "\"index\":\"test-index\"}\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder()
                .withEvent("{\"name\":\"Jim\"}")
                .withHost("test-host")
                .withIndex("test-index")
                .build());

    pipeline.run();
  }

  /** Test successful conversion of JSON messages with provided overrides for time and source. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSplunkEventValidTimeOverride() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\",\n"
                + "\t\"_metadata\": {\"time\": \"2019-11-22T11:32:26.553Z\", "
                + "\"source\": \"test-source-name\"}\n"
                + "}");

    pipeline.getCoderRegistry().registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(SPLUNK_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(
            SplunkEvent.newBuilder()
                .withEvent(
                    "{" + "\"timestamp\":\"2019-10-15T11:32:26.553Z\"" + "}")
                .withSource("test-source-name")
                .withTime(DateTime.parseRfc3339("2019-11-22T11:32:26.553Z").getValue())
                .build());

    pipeline.run();
  }
}
