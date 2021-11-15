/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic;

import com.google.cloud.teleport.newrelic.config.NewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.verify.VerificationTimes;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;

import static com.google.cloud.teleport.newrelic.utils.HttpClient.APPLICATION_GZIP;
import static com.google.cloud.teleport.newrelic.utils.HttpClient.APPLICATION_JSON;
import static com.google.cloud.teleport.templates.PubsubToNewRelic.PLUGIN_VERSION;
import static org.junit.Assume.assumeNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

/**
 * Unit tests for {@link NewRelicPipelineTest}.
 */
public class NewRelicPipelineTest {

  private static final String EXPECTED_PATH = "/log/v1";
  private static final String LICENSE_KEY = "a-license-key";
  private static final String PLAINTEXT_MESSAGE = "A PLAINTEXT log message";
  private static final JsonObject EXPECTED_PLAINTEXT_MESSAGE_JSON;
  private static final LocalDateTime SOME_DATETIME =
    LocalDateTime.of(2021, Month.DECEMBER, 25, 23, 0, 0, 900);
  private static final String JSON_MESSAGE =
    "{ \"message\": \"A JSON message\", \"timestamp\": \"" + SOME_DATETIME.toString() + "\"}";
  private static final JsonObject EXPECTED_JSON_MESSAGE_JSON;

  static {
    EXPECTED_PLAINTEXT_MESSAGE_JSON = new JsonObject();
    EXPECTED_PLAINTEXT_MESSAGE_JSON.addProperty("message", PLAINTEXT_MESSAGE);

    EXPECTED_JSON_MESSAGE_JSON = new JsonObject();
    EXPECTED_JSON_MESSAGE_JSON.addProperty("message", JSON_MESSAGE);
    EXPECTED_JSON_MESSAGE_JSON.addProperty(
      "timestamp", SOME_DATETIME.toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  @Rule
  public final TestPipeline testPipeline = TestPipeline.create();

  private MockServerClient mockServerClient;
  private String url;

  @Before
  public void setUp() throws Exception {
    try {
      mockServerClient = startClientAndServer();
      url = String.format("http://localhost:%d%s", mockServerClient.getPort(), EXPECTED_PATH);
    } catch (Exception e) {
      assumeNoException(e);
    }

    // By default, the mockserver will accept any input. We overwrite this only for these tests
    // requiring failures
    mockServerClient
      .when(HttpRequest.request(EXPECTED_PATH))
      .respond(HttpResponse.response().withStatusCode(202));
  }

  @After
  public void tearDown() {
    mockServerClient.stop();
  }

  @Test
  public void testPubSubMessagesAreSentToNewRelic() {
    // Given
    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        Create.of(PLAINTEXT_MESSAGE, JSON_MESSAGE),
        new NewRelicIO(getPipelineOptions(url, 2, 10, 1, false)));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(100));

    // Then
    // One single request has been performed
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.once());

    // Check the body contains the expected messages
    final String expectedBody =
      jsonArrayOf(EXPECTED_PLAINTEXT_MESSAGE_JSON, EXPECTED_JSON_MESSAGE_JSON);
    mockServerClient.verify(
      baseJsonRequest().withBody(JsonBody.json(expectedBody)), VerificationTimes.once());
  }

  @Test
  public void testMessagesAreBatchedCorrectly() {
    // Given
    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        Create.of(
          PLAINTEXT_MESSAGE,
          PLAINTEXT_MESSAGE,
          PLAINTEXT_MESSAGE,
          PLAINTEXT_MESSAGE,
          PLAINTEXT_MESSAGE),
        new NewRelicIO(getPipelineOptions(url, 2, 2, 1, false)));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(100));

    // Then
    // Three requests should have been performed: two with 2 messages and one with 1 messages
    // (batching = 2, total messages = 5)
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.exactly(3));

    // Check the bodies contain the expected messages for each batch
    final String body1 =
      jsonArrayOf(EXPECTED_PLAINTEXT_MESSAGE_JSON, EXPECTED_PLAINTEXT_MESSAGE_JSON);
    final String body2 = jsonArrayOf(EXPECTED_PLAINTEXT_MESSAGE_JSON);
    mockServerClient.verify(
      baseJsonRequest().withBody(JsonBody.json(body1)),
      baseJsonRequest().withBody(JsonBody.json(body1)),
      baseJsonRequest().withBody(JsonBody.json(body2)));
  }

  @Test
  public void testMessagesAreNotFlushedIfTimerDoesNotExpire() {
    // Given
    final int flushDelay = 2;
    final TestStream<String> logRecordLines =
      TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(new Instant(0))
        .addElements(PLAINTEXT_MESSAGE)
        .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(flushDelay - 1)))
        .addElements(JSON_MESSAGE)
        .advanceWatermarkToInfinity();

    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        logRecordLines,
        new NewRelicIO(getPipelineOptions(url, 10, flushDelay, 1, false)));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(100));

    // Then
    // One single request should have been performed with the 2 messages, as the timer hasn't
    // expired.
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.once());

    // Check the bodies contain the expected messages for each batch
    final String body = jsonArrayOf(EXPECTED_PLAINTEXT_MESSAGE_JSON, EXPECTED_JSON_MESSAGE_JSON);
    mockServerClient.verify(
      baseJsonRequest().withBody(JsonBody.json(body)), VerificationTimes.once());
  }

  @Test
  public void testMessagesAreFlushedIfTimerExpires() {
    // Given
    final int flushDelay = 2;
    final TestStream<String> logRecordLines =
      TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(new Instant(0))
        .addElements(PLAINTEXT_MESSAGE)
        .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(flushDelay + 1)))
        .addElements(JSON_MESSAGE)
        .advanceWatermarkToInfinity();

    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        logRecordLines,
        new NewRelicIO(getPipelineOptions(url, 10, flushDelay, 1, false)));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(100));

    // Then
    // One single request should have been performed with the 2 messages, as the timer hasn't
    // expired.
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.exactly(2));

    // Check the bodies contain the expected messages for each batch
    final String body1 = jsonArrayOf(EXPECTED_PLAINTEXT_MESSAGE_JSON);
    final String body2 = jsonArrayOf(EXPECTED_JSON_MESSAGE_JSON);
    mockServerClient.verify(
      baseJsonRequest().withBody(JsonBody.json(body1)),
      baseJsonRequest().withBody(JsonBody.json(body2)));
  }

  @Test
  public void testPubSubMessagesAreSentToNewRelicWithCompression() {
    // Given
    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        Create.of(JSON_MESSAGE),
        new NewRelicIO(getPipelineOptions(url, 1, 2, 1, true)));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(100));

    // Then
    mockServerClient.verify(baseGzipRequest(), VerificationTimes.once());
  }

  @Test
  public void testRetriesForRetryableErrors() {
    // Given
    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        Create.of(PLAINTEXT_MESSAGE),
        new NewRelicIO(getPipelineOptions(url, 1, 2, 1, false)));
    mockServerClient.reset();

    // First request is answered with a 429, rest of requests are answered with 202
    mockServerClient
      .when(HttpRequest.request(EXPECTED_PATH), Times.exactly(1))
      .respond(HttpResponse.response().withStatusCode(429));
    mockServerClient
      .when(HttpRequest.request(EXPECTED_PATH))
      .respond(HttpResponse.response().withStatusCode(202));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(50));

    // Then
    // Two identical requests were performed, as the Http Client retries sending the original
    // payload after receiving the first 429
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.exactly(2));
  }

  @Test
  public void testNoRetryForNonRetryableErrors() {
    // Given
    NewRelicPipeline pipeline =
      new NewRelicPipeline(
        testPipeline,
        Create.of(PLAINTEXT_MESSAGE),
        new NewRelicIO(getPipelineOptions(url, 1, 2, 1, false)));
    mockServerClient.reset();

    mockServerClient
      .when(HttpRequest.request(EXPECTED_PATH))
      .respond(HttpResponse.response().withStatusCode(413));

    // When
    pipeline.run().waitUntilFinish(Duration.millis(50));

    // Then
    // One single request has been performed
    mockServerClient.verify(baseJsonRequest(), VerificationTimes.exactly(1));
  }

  private NewRelicPipelineOptions getPipelineOptions(
    final String url,
    final Integer batchCount,
    final Integer flushDelay,
    final Integer parallelism,
    final Boolean useCompression) {
    final NewRelicPipelineOptions newRelicPipelineOptions = mock(NewRelicPipelineOptions.class);
    when(newRelicPipelineOptions.getLogsApiUrl()).thenReturn(StaticValueProvider.of(url));
    when(newRelicPipelineOptions.getLicenseKey()).thenReturn(StaticValueProvider.of(LICENSE_KEY));
    when(newRelicPipelineOptions.getBatchCount()).thenReturn(StaticValueProvider.of(batchCount));
    when(newRelicPipelineOptions.getFlushDelay()).thenReturn(StaticValueProvider.of(flushDelay));
    when(newRelicPipelineOptions.getParallelism()).thenReturn(StaticValueProvider.of(parallelism));
    when(newRelicPipelineOptions.getDisableCertificateValidation())
      .thenReturn(StaticValueProvider.of(false));
    when(newRelicPipelineOptions.getUseCompression()).thenReturn(StaticValueProvider.of(useCompression));

    return newRelicPipelineOptions;
  }

  private HttpRequest baseJsonRequest() {
    return HttpRequest.request(EXPECTED_PATH)
      .withMethod("POST")
      .withHeader(Header.header("Content-Type", APPLICATION_JSON))
      .withHeader(Header.header("api-key", LICENSE_KEY));
  }

  private HttpRequest baseGzipRequest() {
    return HttpRequest.request(EXPECTED_PATH)
      .withMethod("POST")
      .withHeader(Header.header("Content-Type", APPLICATION_GZIP))
      .withHeader(Header.header("api-key", LICENSE_KEY));
  }

  private String jsonArrayOf(final JsonObject... jsonObjects) {
    final JsonObject attributes = new JsonObject();
    attributes.addProperty("plugin.source", "gcp-dataflow-" + PLUGIN_VERSION);
    final JsonObject common = new JsonObject();
    common.add("attributes", attributes);

    final JsonArray logs = new JsonArray();
    for (JsonObject obj : jsonObjects) {
      logs.add(obj);
    }

    final JsonObject logsBlock = new JsonObject();
    logsBlock.add("common", common);
    logsBlock.add("logs", logs);

    final JsonArray payload = new JsonArray();
    payload.add(logsBlock);

    return payload.toString();
  }
}
