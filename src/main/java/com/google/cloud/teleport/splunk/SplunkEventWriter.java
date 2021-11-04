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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.util.GCSUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

/** A {@link DoFn} to write {@link SplunkEvent}s to Splunk's HEC endpoint. */
@AutoValue
public abstract class SplunkEventWriter extends DoFn<KV<Integer, SplunkEvent>, SplunkWriteError> {

  private static final Integer DEFAULT_BATCH_COUNT = 1;
  private static final Boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  private static final Logger LOG = LoggerFactory.getLogger(SplunkEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Counter INPUT_COUNTER =
      Metrics.counter(SplunkEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-failed-events");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";
  private static final Pattern URL_PATTERN = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");

  @VisibleForTesting
  protected static final String INVALID_URL_FORMAT_MESSAGE =
      "Invalid url format. Url format should match PROTOCOL://HOST[:PORT], where PORT is optional. "
          + "Supported Protocols are http and https. eg: http://hostname:8088";

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<SplunkEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  private Integer batchCount;
  private Boolean disableValidation;
  private HttpEventPublisher publisher;

  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

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
  abstract ValueProvider<String> selfSignedCertificatePath();

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

    // Either user supplied or default disableValidation.
    if (disableValidation == null) {

      if (disableCertificateValidation() != null) {
        disableValidation = disableCertificateValidation().get();
      }

      disableValidation =
          MoreObjects.firstNonNull(disableValidation, DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
      LOG.info("Disable certificate validation set to: {}", disableValidation);
    }

    try {
      HttpEventPublisher.Builder builder =
          HttpEventPublisher.newBuilder()
              .withUrl(url().get())
              .withToken(token().get())
              .withDisableCertificateValidation(disableValidation);

      if (selfSignedCertificatePath() != null && selfSignedCertificatePath().get() != null) {
        builder.withSelfSignedCertificate(
            GCSUtils.getGcsFileAsBytes(selfSignedCertificatePath().get()));
      }

      publisher = builder.build();
      LOG.info("Successfully created HttpEventPublisher");

    } catch (CertificateException
        | NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | IOException e) {
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
      @TimerId(TIME_ID_NAME) Timer timer)
      throws IOException {

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
  public void onExpiry(
      OutputReceiver<SplunkWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

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
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (!bufferState.isEmpty().read()) {

      HttpResponse response = null;
      List<SplunkEvent> events = Lists.newArrayList(bufferState.read());
      try {
        // Important to close this response to avoid connection leak.
        response = publisher.execute(events);

        if (!response.isSuccessStatusCode()) {
          flushWriteFailures(
              events, response.getStatusMessage(), response.getStatusCode(), receiver);
          logWriteFailures(countState);

        } else {
          LOG.info("Successfully wrote {} events", countState.read());
          SUCCESS_WRITES.inc(countState.read());
        }

      } catch (HttpResponseException e) {
        LOG.error(
            "Error writing to Splunk. StatusCode: {}, content: {}, StatusMessage: {}",
            e.getStatusCode(),
            e.getContent(),
            e.getStatusMessage());
        logWriteFailures(countState);

        flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

      } catch (IOException ioe) {
        LOG.error("Error writing to Splunk: {}", ioe.getMessage());
        logWriteFailures(countState);

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
            response.disconnect();
          }
        } catch (IOException e) {
          LOG.warn(
              "Error trying to disconnect from Splunk: {}\n"
                  + "Messages should still have either been published or prepared for error"
                  + " handling, but there might be a connection leak.\nStack Trace: {}",
              e.getMessage(),
              e.getStackTrace());
        }
      }
    }
  }

  /** Utility method to log write failures and handle metrics. */
  private void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
    LOG.error("Failed to write {} events", countState.read());
    FAILED_WRITES.inc(countState.read());
  }

  /**
   * Utility method to un-batch and flush failed write events.
   *
   * @param events List of {@link SplunkEvent}s to un-batch
   * @param statusMessage Status message to be added to {@link SplunkWriteError}
   * @param statusCode Status code to be added to {@link SplunkWriteError}
   * @param receiver Receiver to write {@link SplunkWriteError}s to
   */
  private static void flushWriteFailures(
      List<SplunkEvent> events,
      String statusMessage,
      Integer statusCode,
      OutputReceiver<SplunkWriteError> receiver) {

    checkNotNull(events, "SplunkEvents cannot be null.");

    SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();

    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }

    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }

    for (SplunkEvent event : events) {
      String payload = GSON.toJson(event);
      SplunkWriteError error = builder.withPayload(payload).build();

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

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setUrl(ValueProvider<String> url);

    abstract ValueProvider<String> url();

    abstract Builder setToken(ValueProvider<String> token);

    abstract ValueProvider<String> token();

    abstract Builder setDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation);

    abstract Builder setSelfSignedCertificatePath(ValueProvider<String> selfSignedCertificatePath);

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
     * Method to set the self signed certificate path.
     *
     * @param selfSignedCertificatePath Path to self-signed certificate
     * @return {@link Builder}
     */
    public Builder withSelfSignedCertificatePath(ValueProvider<String> selfSignedCertificatePath) {
      return setSelfSignedCertificatePath(selfSignedCertificatePath);
    }

    /** Build a new {@link SplunkEventWriter} objects based on the configuration. */
    public SplunkEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(token(), "token needs to be provided.");

      return autoBuild();
    }
  }
}
