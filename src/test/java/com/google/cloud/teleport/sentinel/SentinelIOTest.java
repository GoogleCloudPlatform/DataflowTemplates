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
package com.google.cloud.teleport.sentinel;

import static org.junit.Assume.assumeNoException;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.sentinel.SentinelIO} class. */
public class SentinelIOTest {

  private static final String EXPECTED_PATH = "/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Sentinel HEC server.
  @Rule public MockServerRule mockServerRule;
  private MockServerClient mockServerClient;

  @Before
  public void setup() {
    try {
      mockServerRule = new MockServerRule(this);
    } catch (Exception e) {
      assumeNoException(e);
    }
  }

  /** Test successful multi-event POST request for SentinelIO without parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSentinelIOMultiBatchNoParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();

    List<SentinelEvent> testEvents =
        ImmutableList.of(
            SentinelEvent.newBuilder()
                .withEvent("test-event-1")
                .withHost("test-host-1")
                .withIndex("test-index-1")
                .withSource("test-source-1")
                .withSourceType("test-source-type-1")
                .withTime(12345L)
                .build(),
            SentinelEvent.newBuilder()
                .withEvent("test-event-2")
                .withHost("test-host-2")
                .withIndex("test-index-2")
                .withSource("test-source-2")
                .withSourceType("test-source-type-2")
                .withTime(12345L)
                .build());

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents).withCoder(SentinelEventCoder.of()))
            .apply(
                "SentinelIO",
                SentinelIO.writeBuilder()
                    .withParallelism(1)
                    .withBatchCount(testEvents.size())
                    .withToken("test-token")
                    .withCustomerId("test-customerId")
                    .withLogTableName("test-logTableName")                    
                    .withUrl(Joiner.on(':').join("http://localhost", testPort))
                    .build())
            .setCoder(SentinelWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test successful multi-event POST request for SentinelIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSentinelIOMultiBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();
    int testParallelism = 2;

    List<SentinelEvent> testEvents =
        ImmutableList.of(
            SentinelEvent.newBuilder()
                .withEvent("test-event-1")
                .withHost("test-host-1")
                .withIndex("test-index-1")
                .withSource("test-source-1")
                .withSourceType("test-source-type-1")
                .withTime(12345L)
                .build(),
            SentinelEvent.newBuilder()
                .withEvent("test-event-2")
                .withHost("test-host-2")
                .withIndex("test-index-2")
                .withSource("test-source-2")
                .withSourceType("test-source-type-2")
                .withTime(12345L)
                .build());

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents).withCoder(SentinelEventCoder.of()))
            .apply(
                "SentinelIO",
                SentinelIO.writeBuilder()
                    .withParallelism(testParallelism)
                    .withBatchCount(testEvents.size())
                    .withCustomerId("test-customerId")
                    .withLogTableName("test-logTableName")
                    .withToken("test-token")
                    .withUrl(Joiner.on(':').join("http://localhost", testPort))
                    .build())
            .setCoder(SentinelWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request per parallelism
    mockServerClient.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testParallelism));
  }

  /** Test successful multi-event POST request for SentinelIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSentinelIOSingleBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();
    int testParallelism = 2;

    List<SentinelEvent> testEvents =
        ImmutableList.of(
            SentinelEvent.newBuilder()
                .withEvent("test-event-1")
                .withHost("test-host-1")
                .withIndex("test-index-1")
                .withSource("test-source-1")
                .withSourceType("test-source-type-1")
                .withTime(12345L)
                .build(),
            SentinelEvent.newBuilder()
                .withEvent("test-event-2")
                .withHost("test-host-2")
                .withIndex("test-index-2")
                .withSource("test-source-2")
                .withSourceType("test-source-type-2")
                .withTime(12345L)
                .build());

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents).withCoder(SentinelEventCoder.of()))
            .apply(
                "SentinelIO",
                SentinelIO.writeBuilder()
                    .withParallelism(testParallelism)
                    .withBatchCount(1)
                    .withToken("test-token")
                    .withCustomerId("test-customerId")
                    .withLogTableName("test-logTableName")
                    .withUrl(Joiner.on(':').join("http://localhost", testPort))
                    .build())
            .setCoder(SentinelWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly 1 post request per SentinelEvent
    mockServerClient.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testEvents.size()));
  }

  private void mockServerListening(int statusCode) {
    try {
      mockServerClient
          .when(HttpRequest.request(EXPECTED_PATH))
          .respond(HttpResponse.response().withStatusCode(statusCode));
    } catch (Exception e) {
      assumeNoException(e);
    }
  }
}
