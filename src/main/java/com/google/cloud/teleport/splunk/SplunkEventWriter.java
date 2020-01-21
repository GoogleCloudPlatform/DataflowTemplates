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

package com.google.cloud.teleport.splunk;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
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

/**
 * A {@link DoFn} to write {@link SplunkEvent}s to Splunk's HEC endpoint.
 */
@AutoValue
public abstract class SplunkEventWriter extends DoFn<KV<Integer, SplunkEvent>, SplunkWriteError> {

  private static final Integer DEFAULT_BATCH_COUNT = 1;
  private static final Boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  private static final Logger LOG = LoggerFactory.getLogger(SplunkEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Counter INPUT_COUNTER = Metrics
      .counter(SplunkEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES = Metrics
      .counter(SplunkEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES = Metrics
      .counter(SplunkEventWriter.class, "outbound-failed-events");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";
  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<SplunkEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  private Integer batchCount;
  private Boolean disableValidation;
  private HttpEventPublisher publisher;

  public static Builder newBuilder() {
    return new AutoValue_SplunkEventWriter.Builder();
  }

  @Nullable
  abstract ValueProvider<String> url();

  @Nullable
  abstract ValueProvider<String> token();

  @Nullable
  abstract ValueProvider<Boolean> disableCertificateValidation();

  @Nullable
  abstract ValueProvider<Integer> inputBatchCount();

  @Setup
  public void setup() {

    checkArgument(url().isAccessible(), "url is required for writing events.");
    checkArgument(token().isAccessible(), "Access token is required for writing events.");

    // Either user supplied or default batchCount.
    if (batchCount == null) {

      if (inputBatchCount() != null && inputBatchCount().isAccessible()) {
        batchCount = inputBatchCount().get();
      } else {
        batchCount = DEFAULT_BATCH_COUNT;
      }
      LOG.info("Batch count set to: {}", batchCount);
    }

    // Either user supplied or default disableValidation.
    if (disableValidation == null) {
      if (disableCertificateValidation() != null && disableCertificateValidation().isAccessible()) {
        disableValidation = disableCertificateValidation().get();
      } else {
        disableValidation = DEFAULT_DISABLE_CERTIFICATE_VALIDATION;
      }
      LOG.info("Disable certificate validation set to: {}", disableValidation);
    }

    try {
      HttpEventPublisher.Builder builder = HttpEventPublisher.newBuilder()
          .withUrl(url().get())
          .withToken(token().get())
          .withDisableCertificateValidation(disableValidation);

      publisher = builder.build();
      LOG.info("Successfully created HttpEventPublisher");

    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException | UnsupportedEncodingException e) {
      LOG.error("Error creating HttpEventPublisher: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, SplunkEvent> input,
      OutputReceiver<SplunkWriteError> receiver,
      BoundedWindow window,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @TimerId(TIME_ID_NAME) Timer timer) throws IOException {

    Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    SplunkEvent event = input.getValue();
    INPUT_COUNTER.inc();
    bufferState.add(event);
    count += 1;
    countState.write(count);
    timer.offset(Duration.standardSeconds(DEFAULT_FLUSH_DELAY)).setRelative();

    if (count >= batchCount) {

      LOG.info("Flushing batch of {} events", count);
      flush(receiver, bufferState, countState);
    }
  }

  @OnTimer(TIME_ID_NAME)
  public void onExpiry(OutputReceiver<SplunkWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState) throws IOException {

    if (MoreObjects.<Long>firstNonNull(countState.read(), 0L) > 0) {
      LOG.info("Flushing window with {} events", countState.read());
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
   * Utility method to flush a batch of requests via {@link HttpEventPublisher}.
   *
   * @param receiver Receiver to write {@link SplunkWriteError}s to
   */
  private void flush(
      OutputReceiver<SplunkWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState) throws IOException {

    if (!bufferState.isEmpty().read()) {

      HttpResponse response = null;
      List<SplunkEvent> events = Lists.newArrayList(bufferState.read());
      try {
        // Important to close this response to avoid connection leak.
        response = publisher.execute(events);
        Optional<SplunkWriteError> maybeError = checkResponse(response);

        if (maybeError.isPresent()) {
          receiver.output(maybeError.get());
          logWriteFailures(countState);

        } else {
          LOG.info("Successfully wrote {} events", countState.read());
          SUCCESS_WRITES.inc(countState.read());

        }

      } catch (HttpResponseException e) {
        LOG.error(
            "Error writing to Splunk. StatusCode: {}, content: {}, StatusMessage: {}",
            e.getStatusCode(), e.getContent(), e.getStatusMessage());
        logWriteFailures(countState);

        String payload = publisher.getStringPayload(events);
        SplunkWriteError error = SplunkWriteError.newBuilder()
            .withStatusCode(e.getStatusCode())
            .withStatusMessage(e.getStatusMessage())
            .withPayload(payload)
            .build();
        receiver.output(error);

      } catch (IOException ioe) {
        LOG.error("Error writing to Splunk: {}", ioe.getMessage());
        logWriteFailures(countState);

        String payload = publisher.getStringPayload(events);
        SplunkWriteError error = SplunkWriteError.newBuilder()
            .withStatusMessage(ioe.getMessage())
            .withPayload(payload)
            .build();
        receiver.output(error);

      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed events to an output PCollection.
        bufferState.clear();
        countState.clear();

        if (response != null) {
          response.disconnect();
        }
      }
    }
  }

  /**
   * Utility method to log write failures and handle metrics.
   */
  private void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
    LOG.error("Failed to write {} events", countState.read());
    FAILED_WRITES.inc(countState.read());
  }

  /**
   * A helper method to check the {@link HttpResponse} object and wrap the response into an {@link
   * Optional} {@link SplunkWriteError} in case of an unsuccessful response.
   *
   * @param response {@link HttpResponse} object
   * @return {@link Optional} {@link SplunkWriteError}
   */
  private Optional<SplunkWriteError> checkResponse(HttpResponse response)
      throws IOException {
    if (!response.isSuccessStatusCode()) {
      SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();
      builder.withStatusCode(response.getStatusCode());

      if (response.getStatusMessage() != null) {
        builder.withStatusMessage(response.getStatusMessage());
      }

      if (response.parseAsString() != null) {
        builder.withPayload(response.parseAsString());
      }
      SplunkWriteError error = builder.build();
      return Optional.of(error);
    }
    return Optional.empty();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setUrl(ValueProvider<String> url);

    abstract ValueProvider<String> url();

    abstract Builder setToken(ValueProvider<String> token);

    abstract ValueProvider<String> token();

    abstract Builder setDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation);

    abstract Builder setInputBatchCount(ValueProvider<Integer> inputBatchCount);

    abstract SplunkEventWriter autoBuild();

    /**
     * Method to set the url for HEC event collector.
     *
     * @param url for HEC event collector
     * @return {@link Builder}
     */
    public Builder withUrl(ValueProvider<String> url) {
      checkArgument(url != null, "withURL(url) called with null input.");
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
      return setInputBatchCount(inputBatchCount);
    }

    /**
     * Method to disable certificate validation.
     *
     * @param disableCertificateValidation for disabling certificate validation.
     * @return {@link Builder}
     */
    public Builder withDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation) {
      return setDisableCertificateValidation(disableCertificateValidation);
    }

    /**
     * Build a new {@link SplunkEventWriter} objects based on the configuration.
     */
    public SplunkEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(token(), "token needs to be provided.");

      return autoBuild();
    }
  }

}
