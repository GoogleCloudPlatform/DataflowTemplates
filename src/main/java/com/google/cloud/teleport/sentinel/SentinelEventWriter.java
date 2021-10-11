package com.google.cloud.teleport.sentinel;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

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
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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

/** A {@link DoFn} to write {@link SentinelEvent}s to Sentinel's http endpoint. */
@AutoValue
public abstract class SentinelEventWriter extends DoFn<KV<Integer, SentinelEvent>, SentinelWriteError>  {
    private static final Integer DEFAULT_BATCH_COUNT = 1;
    private static final Boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
    private static final Logger LOG = LoggerFactory.getLogger(SentinelEventWriter.class);
    private static final long DEFAULT_FLUSH_DELAY = 2;
    private static final Counter INPUT_COUNTER =
        Metrics.counter(SentinelEventWriter.class, "inbound-events");
    private static final Counter SUCCESS_WRITES =
        Metrics.counter(SentinelEventWriter.class, "outbound-successful-events");
    private static final Counter FAILED_WRITES =
        Metrics.counter(SentinelEventWriter.class, "outbound-failed-events");
    private static final String BUFFER_STATE_NAME = "buffer";
    private static final String COUNT_STATE_NAME = "count";
    private static final String TIME_ID_NAME = "expiry";
    private static final Pattern URL_PATTERN = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");
  
    @VisibleForTesting
    protected static final String INVALID_URL_FORMAT_MESSAGE =
        "Invalid url format. Url format should match PROTOCOL://HOST[:PORT], where PORT is optional. "
            + "Supported Protocols are http and https. eg: http://hostname:8088";
  
    @StateId(BUFFER_STATE_NAME)
    private final StateSpec<BagState<SentinelEvent>> buffer = StateSpecs.bag();
  
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
      return new AutoValue_SentinelEventWriter.Builder();
    }
  
    @Nullable
    abstract ValueProvider<String> url();
  
    @Nullable
    abstract ValueProvider<String> token();

    @Nullable
    abstract ValueProvider<String> customerId();

    @Nullable
    abstract ValueProvider<String> logTableName();
    
    @Nullable
    abstract ValueProvider<Boolean> disableCertificateValidation();
  
    @Nullable
    abstract ValueProvider<Integer> inputBatchCount();
  
    @Setup
    public void setup() {
  
      checkArgument(url().isAccessible(), "url is required for writing events.");
      //checkArgument(isValidUrlFormat(url().get()), INVALID_URL_FORMAT_MESSAGE);
      checkArgument(token().isAccessible(), "Access token is required for writing events.");
      checkArgument(customerId().isAccessible(), "Customer id is required for writing events.");
      checkArgument(logTableName().isAccessible(), "Log Table Name is required for writing events.");
  
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
                .withCustomerId(customerId().get())
                .withLogTableName(logTableName().get())
                .withDisableCertificateValidation(disableValidation);
  
        publisher = builder.build();
        LOG.info("Successfully created HttpEventPublisher");
  
      } catch (NoSuchAlgorithmException
          | KeyStoreException
          | KeyManagementException
          | UnsupportedEncodingException e) {
        LOG.error("Error creating HttpEventPublisher: {}", e.getMessage());
        throw new RuntimeException(e);
      }
    }
  
    @ProcessElement
    public void processElement(
        @Element KV<Integer, SentinelEvent> input,
        OutputReceiver<SentinelWriteError> receiver,
        BoundedWindow window,
        @StateId(BUFFER_STATE_NAME) BagState<SentinelEvent> bufferState,
        @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
        @TimerId(TIME_ID_NAME) Timer timer)
        throws IOException {
  
      Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
      SentinelEvent event = input.getValue();
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
        OutputReceiver<SentinelWriteError> receiver,
        @StateId(BUFFER_STATE_NAME) BagState<SentinelEvent> bufferState,
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
     * @param receiver Receiver to write {@link SentinelWriteError}s to
     */
    private void flush(
        OutputReceiver<SentinelWriteError> receiver,
        @StateId(BUFFER_STATE_NAME) BagState<SentinelEvent> bufferState,
        @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
        throws IOException {
  
      if (!bufferState.isEmpty().read()) {
  
        HttpResponse response = null;
        List<SentinelEvent> events = Lists.newArrayList(bufferState.read());
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
              "Error writing to Sentinel. StatusCode: {}, content: {}, StatusMessage: {}",
              e.getStatusCode(),
              e.getContent(),
              e.getStatusMessage());
          logWriteFailures(countState);
  
          flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);
  
        } catch (IOException ioe) {
          LOG.error("Error writing to Sentinel: {}", ioe.getMessage());
          logWriteFailures(countState);
  
          flushWriteFailures(events, ioe.getMessage(), null, receiver);
  
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
  
    /** Utility method to log write failures and handle metrics. */
    private void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
      LOG.error("Failed to write {} events", countState.read());
      FAILED_WRITES.inc(countState.read());
    }
  
    /**
     * Utility method to un-batch and flush failed write events.
     *
     * @param events List of {@link SentinelEvent}s to un-batch
     * @param statusMessage Status message to be added to {@link SentinelWriteError}
     * @param statusCode Status code to be added to {@link SentinelWriteError}
     * @param receiver Receiver to write {@link SentinelWriteError}s to
     */
    private static void flushWriteFailures(
        List<SentinelEvent> events,
        String statusMessage,
        Integer statusCode,
        OutputReceiver<SentinelWriteError> receiver) {
  
      checkNotNull(events, "SentinelEvents cannot be null.");
  
      SentinelWriteError.Builder builder = SentinelWriteError.newBuilder();
  
      if (statusMessage != null) {
        builder.withStatusMessage(statusMessage);
      }
  
      if (statusCode != null) {
        builder.withStatusCode(statusCode);
      }
  
      for (SentinelEvent event : events) {
        String payload = GSON.toJson(event);
        SentinelWriteError error = builder.withPayload(payload).build();
  
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

      abstract ValueProvider<String> customerId();

      abstract Builder setCustomerId(ValueProvider<String> customerId);      

      abstract ValueProvider<String> logTableName();

      abstract Builder setLogTableName(ValueProvider<String> logTableName);      

      abstract Builder setDisableCertificateValidation(
          ValueProvider<Boolean> disableCertificateValidation);
  
      abstract Builder setInputBatchCount(ValueProvider<Integer> inputBatchCount);
  
      abstract SentinelEventWriter autoBuild();
  
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
       * Same as {@link Builder#withCustomerId(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param customerId for event collector
       * @return {@link Builder}
       */
      public Builder withCustomerId(ValueProvider<String> customerId) {
        checkArgument(customerId != null, "withCustomerId(customerId) called with null input.");
        return setCustomerId(customerId);
      }
  
      /**
       * Same as {@link Builder#withLogTableName(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param logTableName for event collector
       * @return {@link Builder}
       */
      public Builder withLogTableName(ValueProvider<String> logTableName) {
        checkArgument(logTableName != null, "withLogTableName(logTableName) called with null input.");
        return setLogTableName(logTableName);
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
  
      /** Build a new {@link SentinelEventWriter} objects based on the configuration. */
      public SentinelEventWriter build() {
        checkNotNull(url(), "url needs to be provided.");
        checkNotNull(token(), "token needs to be provided.");
        checkNotNull(customerId(), "customerId needs to be provided.");
        checkNotNull(logTableName(), "logTableName needs to be provided.");
  
        return autoBuild();
      }
    }
  }
  