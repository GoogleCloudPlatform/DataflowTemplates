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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} to write {@link DatadogEvent}s to Datadog's HEC endpoint. */
@AutoValue
public abstract class DatadogEventWriter extends DoFn<KV<Integer, DatadogEvent>, DatadogWriteError> {

  private static final Integer DEFAULT_BATCH_COUNT = 10;
  private static final Integer MAX_BATCH_COUNT = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(DatadogEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Counter INPUT_COUNTER =
      Metrics.counter(DatadogEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(DatadogEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(DatadogEventWriter.class, "outbound-failed-events");
  private static final Counter INVALID_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-invalid-requests");
  private static final Counter SERVER_ERROR_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-server-error-requests");
  private static final Counter VALID_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-valid-requests");
  private static final Distribution SUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(DatadogEventWriter.class, "successful_write_to_datadog_latency_ms");
  private static final Distribution UNSUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(DatadogEventWriter.class, "unsuccessful_write_to_datadog_latency_ms");
  private static final Distribution SUCCESSFUL_WRITE_BATCH_SIZE =
      Metrics.distribution(DatadogEventWriter.class, "write_to_datadog_batch");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";
  private static final Pattern URL_PATTERN = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");

  @VisibleForTesting
  protected static final String INVALID_URL_FORMAT_MESSAGE =
      "Invalid url format. Url format should match PROTOCOL://HOST[:PORT], where PORT is optional. "
          + "Supported Protocols are http and https. eg: http://hostname:8088";

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<DatadogEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  private Integer batchCount;
  private HttpEventPublisher publisher;

  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  public static Builder newBuilder() {
    return new AutoValue_DatadogEventWriter.Builder();
  }

  @Nullable
  abstract ValueProvider<String> url();

  @Nullable
  abstract ValueProvider<String> token();

  @Nullable
  abstract ValueProvider<Integer> inputBatchCount();

  @Setup
  public void setup() {

    checkArgument(url().isAccessible(), "url is required for writing events.");
    checkArgument(isValidUrlFormat(url().get()), INVALID_URL_FORMAT_MESSAGE);
    checkArgument(token().isAccessible(), "Access token is required for writing events.");

    // Either user supplied or default batchCount.
    if (batchCount == null) {

      if (inputBatchCount() != null) {
        batchCount = inputBatchCount().get();
      }

      batchCount = MoreObjects.firstNonNull(batchCount, DEFAULT_BATCH_COUNT);
      LOG.info("Batch count set to: {}", batchCount);
    }
    checkArgument(batchCount <= MAX_BATCH_COUNT, "batchCount must be less than or equal to %s", MAX_BATCH_COUNT);

    try {
      HttpEventPublisher.Builder builder =
          HttpEventPublisher.newBuilder()
              .withUrl(url().get())
              .withToken(token().get());

      publisher = builder.build();
      LOG.info("Successfully created HttpEventPublisher");

    } catch (NoSuchAlgorithmException
        | KeyManagementException
        | IOException e) {
      LOG.error("Error creating HttpEventPublisher: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, DatadogEvent> input,
      OutputReceiver<DatadogWriteError> receiver,
      BoundedWindow window,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @TimerId(TIME_ID_NAME) Timer timer)
      throws IOException {

    Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    DatadogEvent event = input.getValue();
    INPUT_COUNTER.inc();
    bufferState.add(event);
    count += 1;
    countState.write(count);
    timer.offset(Duration.standardSeconds(DEFAULT_FLUSH_DELAY)).setRelative();

    if (count >= batchCount) {
      LOG.debug("Flushing batch of {} events", count);
      flush(receiver, bufferState, countState);
    }
  }

  @OnTimer(TIME_ID_NAME)
  public void onExpiry(
      OutputReceiver<DatadogWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (MoreObjects.<Long>firstNonNull(countState.read(), 0L) > 0) {
      LOG.debug("Flushing window with {} events", countState.read());
      flush(receiver, bufferState, countState);
    }
  }

  @Teardown
  public void tearDown() {
    if (this.publisher != null) {
      try {
        this.publisher.close();
        LOG.info("Successfully closed HttpEventPublisher");

      } catch (IOException e) {
        LOG.warn("Received exception while closing HttpEventPublisher: {}", e.getMessage());
      }
    }
  }

  /**
   * Utility method to flush a batch of events via {@link HttpEventPublisher}.
   *
   * @param receiver Receiver to write {@link DatadogWriteError}s to
   */
  private void flush(
      OutputReceiver<DatadogWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (!bufferState.isEmpty().read()) {

      HttpResponse response = null;
      List<DatadogEvent> events = Lists.newArrayList(bufferState.read());
      long startTime = System.nanoTime();
      try {
        // Important to close this response to avoid connection leak.
        response = publisher.execute(events);
        if (!response.isSuccessStatusCode()) {
          UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          FAILED_WRITES.inc(countState.read());
          int statusCode = response.getStatusCode();
          if (statusCode >= 400 && statusCode < 500) {
            INVALID_REQUESTS.inc();
          } else if (statusCode >= 500 && statusCode < 600) {
            SERVER_ERROR_REQUESTS.inc();
          }

          logWriteFailures(
              countState,
              response.getStatusCode(),
              response.parseAsString(),
              response.getStatusMessage());
          flushWriteFailures(
              events, response.getStatusMessage(), response.getStatusCode(), receiver);

        } else {
          SUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          SUCCESS_WRITES.inc(countState.read());
          VALID_REQUESTS.inc();
          SUCCESSFUL_WRITE_BATCH_SIZE.update(countState.read());

          LOG.debug("Successfully wrote {} events", countState.read());
        }

      } catch (HttpResponseException e) {
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(countState.read());
        int statusCode = e.getStatusCode();
        if (statusCode >= 400 && statusCode < 500) {
          INVALID_REQUESTS.inc();
        } else if (statusCode >= 500 && statusCode < 600) {
          SERVER_ERROR_REQUESTS.inc();
        }

        logWriteFailures(countState, e.getStatusCode(), e.getContent(), e.getStatusMessage());
        flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

      } catch (IOException ioe) {
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(countState.read());
        INVALID_REQUESTS.inc();

        logWriteFailures(countState, 0, ioe.getMessage(), null);
        flushWriteFailures(events, ioe.getMessage(), null, receiver);

      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed events to an output PCollection.
        bufferState.clear();
        countState.clear();

        // We've observed cases where errors at this point can cause the pipeline to keep retrying
        // the same events over and over (e.g. from Dataflow Runner's Pub/Sub implementation). Since
        // the events have either been published or wrapped for error handling, we can safely
        // ignore this error, though there may or may not be a leak of some type depending on
        // HttpResponse's implementation. However, any potential leak would still happen if we let
        // the exception fall through, so this isn't considered a major issue.
        try {
          if (response != null) {
            response.ignore();
          }
        } catch (IOException e) {
          LOG.warn(
              "Error ignoring response from Datadog. Messages should still have published, but there"
                  + " might be a connection leak.",
              e);
        }
      }
    }
  }

  /** Utility method to log write failures. */
  private void logWriteFailures(
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      int statusCode,
      String content,
      String statusMessage) {
    LOG.error("Failed to write {} events", countState.read());
    LOG.error(
        "Error writing to Datadog. StatusCode: {}, content: {}, StatusMessage: {}",
        statusCode,
        content,
        statusMessage);
  }

  /**
   * Utility method to un-batch and flush failed write events.
   *
   * @param events List of {@link DatadogEvent}s to un-batch
   * @param statusMessage Status message to be added to {@link DatadogWriteError}
   * @param statusCode Status code to be added to {@link DatadogWriteError}
   * @param receiver Receiver to write {@link DatadogWriteError}s to
   */
  private static void flushWriteFailures(
      List<DatadogEvent> events,
      String statusMessage,
      Integer statusCode,
      OutputReceiver<DatadogWriteError> receiver) {

    checkNotNull(events, "DatadogEvents cannot be null.");

    DatadogWriteError.Builder builder = DatadogWriteError.newBuilder();

    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }

    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }

    for (DatadogEvent event : events) {
      String payload = GSON.toJson(event);
      DatadogWriteError error = builder.withPayload(payload).build();

      receiver.output(error);
    }
  }

  /**
   * Checks whether the HEC URL matches the format PROTOCOL://HOST[:PORT].
   *
   * @param url for HEC event collector
   * @return true if the URL is valid
   */
  private static boolean isValidUrlFormat(String url) {
    Matcher matcher = URL_PATTERN.matcher(url);
    if (matcher.find()) {
      String host = matcher.group(2);
      return InetAddresses.isInetAddress(host) || InternetDomainName.isValid(host);
    }
    return false;
  }

  /**
   * Converts Nanoseconds to Milliseconds.
   *
   * @param ns time in nanoseconds
   * @return time in milliseconds
   */
  private static long nanosToMillis(long ns) {
    return Math.round(((double) ns) / 1e6);
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setUrl(ValueProvider<String> url);

    abstract ValueProvider<String> url();

    abstract Builder setToken(ValueProvider<String> token);

    abstract ValueProvider<String> token();

    abstract Builder setInputBatchCount(ValueProvider<Integer> inputBatchCount);

    abstract DatadogEventWriter autoBuild();

    /**
     * Method to set the url for HEC event collector.
     *
     * @param url for HEC event collector
     * @return {@link Builder}
     */
    public Builder withUrl(ValueProvider<String> url) {
      checkArgument(url != null, "withURL(url) called with null input.");
      if (url.isAccessible()) {
        checkArgument(isValidUrlFormat(url.get()), INVALID_URL_FORMAT_MESSAGE);
      }
      return setUrl(url);
    }

    /**
     * Same as {@link Builder#withUrl(ValueProvider)} but without {@link ValueProvider}.
     *
     * @param url for HEC event collector
     * @return {@link Builder}
     */
    public Builder withUrl(String url) {
      checkArgument(url != null, "withURL(url) called with null input.");
      checkArgument(isValidUrlFormat(url), INVALID_URL_FORMAT_MESSAGE);
      return setUrl(ValueProvider.StaticValueProvider.of(url));
    }

    /**
     * Method to set the authentication token for HEC.
     *
     * @param token Authentication token for HEC event collector
     * @return {@link Builder}
     */
    public Builder withToken(ValueProvider<String> token) {
      checkArgument(token != null, "withToken(token) called with null input.");
      return setToken(token);
    }

    /**
     * Same as {@link Builder#withToken(ValueProvider)} but without {@link ValueProvider}.
     *
     * @param token for HEC event collector
     * @return {@link Builder}
     */
    public Builder withToken(String token) {
      checkArgument(token != null, "withToken(token) called with null input.");
      return setToken(ValueProvider.StaticValueProvider.of(token));
    }

    /**
     * Method to set the inputBatchCount.
     *
     * @param inputBatchCount for batching post requests.
     * @return {@link Builder}
     */
    public Builder withInputBatchCount(ValueProvider<Integer> inputBatchCount) {
      if (inputBatchCount != null && inputBatchCount.isAccessible()) {
        Integer batchCount = inputBatchCount.get();
        if (batchCount != null) {
          checkArgument(batchCount <= MAX_BATCH_COUNT, "inputBatchCount must be less than or equal to %s", MAX_BATCH_COUNT);
        }
      }
      return setInputBatchCount(inputBatchCount);
    }

    /** Build a new {@link DatadogEventWriter} objects based on the configuration. */
    public DatadogEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(token(), "token needs to be provided.");

      return autoBuild();
    }
  }
}
