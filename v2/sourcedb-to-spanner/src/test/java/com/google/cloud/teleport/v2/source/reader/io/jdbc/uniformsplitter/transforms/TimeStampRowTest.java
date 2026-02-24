/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link TimeStampRow}. */
@RunWith(MockitoJUnitRunner.class)
public class TimeStampRowTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testTimeStampRowResetsFutureTimestamp() {
    // 1. Create an element with a timestamp far in the future
    // (simulating GlobalWindow side-input inheritance).
    // We use a value slightly before the absolute end of time to avoid boundary issues in the test
    // runner.
    Instant futureTimestamp = GlobalWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardHours(24));
    PCollection<String> input =
        pipeline.apply(Create.timestamped(TimestampedValue.of("test-element", futureTimestamp)));

    // 2. Apply the TimeStampRow transform
    PCollection<String> output = input.apply(ParDo.of(new TimeStampRow<>()));

    // 3. Verify that the output timestamp is roughly "now" (not the end of time)
    PAssert.that(output)
        .satisfies(
            elements -> {
              for (String element : elements) {
                assertThat(element).isEqualTo("test-element");
              }
              return null;
            });

    // We also verify the timestamp more precisely using a specialized DoFn
    output.apply(
        "VerifyTimestamp",
        ParDo.of(
            new DoFn<String, Void>() {
              @ProcessElement
              public void process(ProcessContext c) {
                Instant timestamp = c.timestamp();
                // Verify that it moved back from the future
                assertThat(timestamp.isBefore(futureTimestamp)).isTrue();
                // Verify that it's close to current time (within a generous 10 minutes window for
                // test
                // stability)
                assertThat(timestamp.isAfter(Instant.now().minus(Duration.standardMinutes(10L))))
                    .isTrue();
              }
            }));

    pipeline.run();
  }

  @Test
  public void testTimeStampRowWithCurrentTimestamp() {
    // 1. Create an element with a current timestamp
    Instant now = Instant.now();
    PCollection<String> input =
        pipeline.apply(
            "CreateCurrent", Create.timestamped(TimestampedValue.of("current-element", now)));

    // 2. Apply the TimeStampRow transform
    PCollection<String> output = input.apply("ApplyTimeStampRow", ParDo.of(new TimeStampRow<>()));

    // 3. Verify that the output timestamp is still roughly "now"
    output.apply(
        "VerifyCurrentTimestamp",
        ParDo.of(
            new DoFn<String, Void>() {
              @ProcessElement
              public void process(ProcessContext c) {
                Instant timestamp = c.timestamp();
                assertThat(timestamp.isAfter(now.minus(Duration.standardSeconds(10)))).isTrue();
                assertThat(timestamp.isBefore(now.plus(Duration.standardHours(1)))).isTrue();
              }
            }));

    pipeline.run();
  }

  @Test
  public void testGetAllowedTimestampSkew() {
    TimeStampRow<String> doFn = new TimeStampRow<>();
    assertThat(doFn.getAllowedTimestampSkew()).isEqualTo(Duration.millis(Long.MAX_VALUE));
  }
}
