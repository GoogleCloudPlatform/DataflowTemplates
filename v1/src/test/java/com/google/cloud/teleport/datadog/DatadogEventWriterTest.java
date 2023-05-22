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
package com.google.cloud.teleport.datadog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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

/** Unit tests for {@link com.google.cloud.teleport.datadog.DatadogEventWriter} class. */
public class DatadogEventWriterTest {

  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.DD_URL_PATH;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Datadog API server.
  private ClientAndServer mockServer;

  @Before
  public void setup() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    mockServer = startClientAndServer(port);
  }

  /** Test building {@link DatadogEventWriter} with missing URL. */
  @Test
  public void eventWriterMissingURL() {

    Exception thrown =
        assertThrows(NullPointerException.class, () -> DatadogEventWriter.newBuilder().build());

    assertThat(thrown).hasMessageThat().contains("url needs to be provided");
  }

  /** Test building {@link DatadogEventWriter} with missing URL protocol. */
  @Test
  public void eventWriterMissingURLProtocol() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("test-url").build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link DatadogEventWriter} with an invalid URL. */
  @Test
  public void eventWriterInvalidURL() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("http://1.2.3").build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /**
   * Test building {@link DatadogEventWriter} with the 'api/v2/logs' path appended to
   * the URL.
   */
  @Test
  public void eventWriterFullEndpoint() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatadogEventWriter.newBuilder()
                    .withUrl("http://test-url:8088/api/v2/logs")
                    .build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link DatadogEventWriter} with missing token. */
  @Test
  public void eventWriterMissingToken() {

    Exception thrown =
        assertThrows(
            NullPointerException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("http://test-url").build());

    assertThat(thrown).hasMessageThat().contains("apiKey needs to be provided");
  }

  /**
   * Test building {@link DatadogEventWriter} with default batchcount .
   */
  @Test
  public void eventWriterDefaultBatchCount() {

    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder().withUrl("http://test-url").withApiKey("test-api-key").build();

    assertThat(writer.inputBatchCount()).isNull();
  }

  /** Test building {@link DatadogEventWriter} with a batchCount greater than 1000. */
  @Test
  public void eventWriterBatchCountTooBig() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogEventWriter.newBuilder()
                .withUrl("http://test-url")
                .withApiKey("test-api-key")
                .withInputBatchCount(StaticValueProvider.of(1001))
                .build());

    assertThat(thrown).hasMessageThat().contains("inputBatchCount must be less than or equal to 1000");
  }

  /** Test building {@link DatadogEventWriter} with custom batchcount . */
  @Test
  public void eventWriterCustomBatchCountAndValidation() {

    Integer batchCount = 30;
    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withApiKey("test-api-key")
            .withInputBatchCount(StaticValueProvider.of(batchCount))
            .build();

    assertThat(writer.inputBatchCount().get()).isEqualTo(batchCount);
  }

  /** Test successful POST request for single batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogWriteSingleBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()),
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-2")
                    .withTags("test-tags-2")
                    .withHostname("test-hostname-2")
                    .withService("test-service-2")
                    .withMessage("test-message-2")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(1)) // Test one request per DatadogEvent
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly the expected number of POST requests.
    mockServer.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testEvents.size()));
  }

  /** Test successful POST request for multi batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogWriteMultiBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()),
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-2")
                    .withTags("test-tags-2")
                    .withHostname("test-hostname-2")
                    .withService("test-service-2")
                    .withMessage("test-message-2")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test failed POST request. */
  @Test
  @Category(NeedsRunner.class)
  public void failedDatadogWriteSingleBatchTest() {

    // Create server expectation for FAILURE.
    mockServerListening(404);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // Expect a single 404 Not found DatadogWriteError
    PAssert.that(actual)
        .containsInAnyOrder(
            DatadogWriteError.newBuilder()
                .withStatusCode(404)
                .withStatusMessage("Not Found")
                .withPayload(
                    "{\"ddsource\":\"test-source-1\"," +
                        "\"ddtags\":\"test-tags-1\",\"hostname\":\"test-hostname-1\"," +
                        "\"service\":\"test-service-1\",\"message\":\"test-message-1\"}")
                .build());

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}
