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
import static org.junit.Assert.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
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

/** Unit tests for {@link com.google.cloud.teleport.datadog.DatadogIO} class. */
public class DatadogIOTest {

  private static final DatadogEvent DATADOG_TEST_EVENT_1 =
      DatadogEvent.newBuilder()
          .withSource("test-source-1")
          .withTags("test-tags-1")
          .withHostname("test-hostname-1")
          .withService("test-service-1")
          .withMessage("test-message-1")
          .build();

  private static final DatadogEvent DATADOG_TEST_EVENT_2 =
      DatadogEvent.newBuilder()
          .withSource("test-source-2")
          .withTags("test-tags-2")
          .withHostname("test-hostname-2")
          .withService("test-service-2")
          .withMessage("test-message-2")
          .build();

  private static final List<DatadogEvent> DATADOG_EVENTS =
      ImmutableList.of(DATADOG_TEST_EVENT_1, DATADOG_TEST_EVENT_2);

  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.DD_URL_PATH;
  private static final int TEST_PARALLELISM = 2;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a mock server to simulate an actual Datadog API server.
  private ClientAndServer mockServer;

  @Before
  public void setup() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    mockServer = startClientAndServer(port);
  }

  /** Test the builder with an invalid site. */
  @Test
  public void builderInvalidSite() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogIO.writeBuilder()
                .withApiKey("test-api-key")
                .withSite("bad-site")
                .build()
        );

    assertThat(thrown).hasMessageThat().contains(DatadogIO.Write.Builder.INVALID_SITE_MESSAGE);
  }

  /** Test the builder with an invalid site provider. */
  @Test
  public void builderInvalidSiteProvider() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogIO.writeBuilder()
                .withApiKey("test-api-key")
                .withSite(ValueProvider.StaticValueProvider.of("bad-site"))
                .build()
        );

    assertThat(thrown).hasMessageThat().contains(DatadogIO.Write.Builder.INVALID_SITE_MESSAGE);
  }

  /** Test the builder with a valid site. */
  @Test
  public void builderValidSite() {

    DatadogIO.Write writer = DatadogIO.writeBuilder()
        .withApiKey("test-api-key")
        .withSite("datadoghq.com")
        .build();

    assertThat(writer.url().get()).isEqualTo("https://http-intake.logs.datadoghq.com");
  }

  /** Test the builder with a valid site provider. */
  @Test
  public void builderValidSiteProvider() {

    DatadogIO.Write writer = DatadogIO.writeBuilder()
        .withApiKey("test-api-key")
        .withSite(ValueProvider.StaticValueProvider.of("datadoghq.com"))
        .build();

    assertThat(writer.url().get()).isEqualTo("https://http-intake.logs.datadoghq.com");
  }

  /** Test successful multi-event POST request for DatadogIO without parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOMultiBatchNoParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder()
                    .withParallelism(1)
                    .withBatchCount(DATADOG_EVENTS.size())
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test successful multi-event POST request for DatadogIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOMultiBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder()
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(DATADOG_EVENTS.size())
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request per parallelism
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.atLeast(1));
  }

  /** Test successful multi-event POST request for DatadogIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOSingleBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder()
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(1)
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly 1 post request per DatadogEvent
    mockServer.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(DATADOG_EVENTS.size()));
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}
