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
package com.google.cloud.teleport.splunk;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.ServerSocket;
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
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.splunk.SplunkIO} class. */
public class SplunkIOTest {

  private static final SplunkEvent SPLUNK_TEST_EVENT_1 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-1")
          .withHost("test-host-1")
          .withIndex("test-index-1")
          .withSource("test-source-1")
          .withSourceType("test-source-type-1")
          .withTime(12345L)
          .build();

  private static final SplunkEvent SPLUNK_TEST_EVENT_2 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-2")
          .withHost("test-host-2")
          .withIndex("test-index-2")
          .withSource("test-source-2")
          .withSourceType("test-source-type-2")
          .withTime(12345L)
          .build();

  private static final List<SplunkEvent> SPLUNK_EVENTS =
      ImmutableList.of(SPLUNK_TEST_EVENT_1, SPLUNK_TEST_EVENT_2);

  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.HEC_URL_PATH;
  private static final int TEST_PARALLELISM = 2;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a mock server to simulate an actual Splunk HEC server.
  private ClientAndServer mockServer;

  @Before
  public void setup() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    mockServer = startClientAndServer(port);
  }

  /** Test successful multi-event POST request for SplunkIO without parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSplunkIOMultiBatchNoParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(SPLUNK_EVENTS).withCoder(SplunkEventCoder.of()))
            .apply(
                "SplunkIO",
                SplunkIO.writeBuilder()
                    .withParallelism(1)
                    .withBatchCount(SPLUNK_EVENTS.size())
                    .withToken("test-token")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(SplunkWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test successful multi-event POST request for SplunkIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSplunkIOMultiBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(SPLUNK_EVENTS).withCoder(SplunkEventCoder.of()))
            .apply(
                "SplunkIO",
                SplunkIO.writeBuilder()
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(SPLUNK_EVENTS.size())
                    .withToken("test-token")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(SplunkWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request per parallelism
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.atLeast(1));
  }

  /** Test successful multi-event POST request for SplunkIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSplunkIOSingleBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(SPLUNK_EVENTS).withCoder(SplunkEventCoder.of()))
            .apply(
                "SplunkIO",
                SplunkIO.writeBuilder()
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(1)
                    .withToken("test-token")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(SplunkWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly 1 post request per SplunkEvent
    mockServer.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(SPLUNK_EVENTS.size()));
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}
