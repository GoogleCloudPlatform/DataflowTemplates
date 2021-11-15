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
package com.google.cloud.teleport.newrelic.dofns;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.teleport.newrelic.config.NewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.utils.HttpClient;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/** A {@link DoFn} to write {@link NewRelicLogRecord}s to New Relic's log API endpoint. */
public class NewRelicLogRecordWriterFn
    extends DoFn<KV<Integer, NewRelicLogRecord>, NewRelicLogApiSendError> {

  private static final Logger LOG = LoggerFactory.getLogger(NewRelicLogRecordWriterFn.class);

  private static final Counter INPUT_COUNTER =
      Metrics.counter(NewRelicLogRecordWriterFn.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(NewRelicLogRecordWriterFn.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(NewRelicLogRecordWriterFn.class, "outbound-failed-events");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";
  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<NewRelicLogRecord>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  // Non-serialized fields: these are set up once the DoFn has potentially been deserialized, in the
  // @Setup method.
  private HttpClient httpClient;

  // Serialized fields
  private final ValueProvider<String> logsApiUrl;
  private final ValueProvider<String> licenseKey;
  private final ValueProvider<Boolean> disableCertificateValidation;
  private final ValueProvider<Integer> batchCount;
  private final ValueProvider<Integer> flushDelay;
  private final ValueProvider<Boolean> useCompression;
  private final ValueProvider<String> tokenKmsEncryptionKey;

  public NewRelicLogRecordWriterFn(final NewRelicPipelineOptions pipelineOptions) {
    this.logsApiUrl = pipelineOptions.getLogsApiUrl();
    this.licenseKey = pipelineOptions.getLicenseKey();
    this.disableCertificateValidation = pipelineOptions.getDisableCertificateValidation();
    this.batchCount = pipelineOptions.getBatchCount();
    this.flushDelay = pipelineOptions.getFlushDelay();
    this.useCompression = pipelineOptions.getUseCompression();
    this.tokenKmsEncryptionKey = pipelineOptions.getTokenKMSEncryptionKey();
  }

  @Setup
  public void setup() {
    final ValueProvider<String> decryptedLicenseKey = tokenKmsEncryptionKey != null && tokenKmsEncryptionKey.isAccessible() && tokenKmsEncryptionKey.get() != null
      ? decryptLicenseKey(licenseKey, tokenKmsEncryptionKey)
      : licenseKey;
    checkArgument(
      decryptedLicenseKey != null && decryptedLicenseKey.isAccessible() && decryptedLicenseKey.get() != null,
      "New Relic License Key is mandatory for sending log records, and it was not provided.");

    logRuntimeConfiguration(decryptedLicenseKey);

    try {
      this.httpClient =
          HttpClient.init(
              new GenericUrl(logsApiUrl.get()),
              decryptedLicenseKey.get(),
              disableCertificateValidation.get(),
              useCompression.get());
      LOG.info("Successfully created HttpClient");
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
      LOG.error("Error creating HttpClient", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Utility method to decrypt a NewRelic API token.
   *
   * @param encryptedKey The NewRelic API token as a Base64 encoded {@link String} encrypted
   *     with a Cloud KMS Key.
   * @param kmsKey The Cloud KMS Encryption Key to decrypt the NewRelic API token.
   * @return Decrypted NewRelic API token.
   */
  private static ValueProvider<String> decryptLicenseKey(
    ValueProvider<String> encryptedKey, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(encryptedKey, kmsKey);
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, NewRelicLogRecord> input,
      OutputReceiver<NewRelicLogApiSendError> receiver,
      @AlwaysFetched @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
      @AlwaysFetched @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @TimerId(TIME_ID_NAME) Timer timer)
      throws IOException {

    Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    final NewRelicLogRecord logRecord = input.getValue();
    INPUT_COUNTER.inc();
    bufferState.add(logRecord);
    count += 1;
    countState.write(count);
    timer.offset(Duration.standardSeconds(flushDelay.get())).setRelative();

    if (count >= batchCount.get()) {
      LOG.debug("Flushing batch of {} events", count);
      flush(receiver, bufferState, countState);
    }
  }

  @OnTimer(TIME_ID_NAME)
  public void onExpiry(
      OutputReceiver<NewRelicLogApiSendError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (MoreObjects.<Long>firstNonNull(countState.read(), 0L) > 0) {
      LOG.debug("Timer expired: flushing window with {} events", countState.read());
      flush(receiver, bufferState, countState);
    }
  }

  /**
   * Utility method to flush a batch of requests via {@link HttpClient}.
   *
   * @param receiver Receiver to write {@link NewRelicLogApiSendError}s to
   */
  private void flush(
      OutputReceiver<NewRelicLogApiSendError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (!bufferState.isEmpty().read()) {
      // Important to close this response to avoid connection leak.
      HttpResponse response = null;
      final List<NewRelicLogRecord> logRecords = ImmutableList.copyOf(bufferState.read());
      try {
        final long startTime = System.currentTimeMillis();
        response = httpClient.send(logRecords);
        final long duration = System.currentTimeMillis() - startTime;

        if (!response.isSuccessStatusCode()) {
          LOG.error(
              "Error writing to New Relic. StatusCode: {}, Content: {}, StatusMessage: {}",
              response.getStatusCode(),
              response.getContent(),
              response.getStatusMessage());
          logWriteFailures(countState);
          flushWriteFailures(
              logRecords, response.getStatusMessage(), response.getStatusCode(), receiver);
        } else {
          LOG.debug(
              "Successfully wrote {} log records in {}ms. Response code {} and body: {}",
              countState.read(),
              duration,
              response.getStatusCode(),
              response.parseAsString());
          SUCCESS_WRITES.inc(countState.read());
        }
      } catch (HttpResponseException e) {
        LOG.error("Error writing to New Relic", e);
        logWriteFailures(countState);
        flushWriteFailures(logRecords, e.getStatusMessage(), e.getStatusCode(), receiver);
      } catch (IOException ioe) {
        LOG.error("Error writing to New Relic", ioe);
        logWriteFailures(countState);
        flushWriteFailures(logRecords, ioe.getMessage(), null, receiver);
      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed logRecords to an output PCollection.
        bufferState.clear();
        countState.clear();

        if (response != null) {
          response.disconnect();
        }
      }
    }
  }

  /**
   * Utility method to un-batch and flush failed write log records.
   *
   * @param logRecords List of {@link NewRelicLogRecord}s to un-batch
   * @param statusMessage Status message to be added to {@link NewRelicLogApiSendError}
   * @param statusCode Status code to be added to {@link NewRelicLogApiSendError}
   * @param receiver Receiver to write {@link NewRelicLogApiSendError}s to
   */
  private static void flushWriteFailures(
      List<NewRelicLogRecord> logRecords,
      String statusMessage,
      Integer statusCode,
      OutputReceiver<NewRelicLogApiSendError> receiver) {

    checkNotNull(logRecords, "New Relic logRecords cannot be null.");

    for (NewRelicLogRecord event : logRecords) {
      String payload = GSON.toJson(event);
      receiver.output(new NewRelicLogApiSendError(payload, statusMessage, statusCode));
    }
  }

  /** Utility method to log write failures and handle metrics. */
  private static void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
    LOG.error("Failed to write {} log records", countState.read());
    FAILED_WRITES.inc(countState.read());
  }

  @Teardown
  public void tearDown() {
    if (this.httpClient != null) {
      try {
        this.httpClient.close();
        LOG.debug("Successfully closed HttpClient");
      } catch (IOException e) {
        LOG.warn("Received exception while closing HttpClient", e);
      }
    }
  }

  private void logRuntimeConfiguration(final ValueProvider<String> decryptedLicenseKey) {
    final String licenseKey = decryptedLicenseKey.get();
    final String obfuscatedLicenseKey = licenseKey.length() < 20
      ? StringUtils.repeat("*", licenseKey.length())
      : StringUtils.overlay(licenseKey, StringUtils.repeat("*", licenseKey.length() - 8), 8, licenseKey.length());
    LOG.info("NewRelicLogRecordWriterFn runtime configuration: logsApiUrl={}, licenseKey={}, disableCertificateValidation={}, batchCount={}, flushDelay={}, useCompression={}",
      logsApiUrl, obfuscatedLicenseKey, disableCertificateValidation, batchCount, flushDelay, useCompression);
  }
}
