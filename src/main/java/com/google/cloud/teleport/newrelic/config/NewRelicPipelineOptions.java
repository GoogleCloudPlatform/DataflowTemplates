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
package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.newrelic.dofns.NewRelicLogRecordWriterFn;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The {@link NewRelicPipelineOptions} class provides the custom options passed by the executor at
 * the command line to configure the pipeline that process PubSub data and sends it to NR using
 * {@link NewRelicLogRecordWriterFn}. It also defines the default values for those configuration
 * options that are optional.
 */
public interface NewRelicPipelineOptions extends PipelineOptions {
  String DEFAULT_LOGS_API_URL = "https://log-api.newrelic.com/log/v1";
  int DEFAULT_BATCH_COUNT = 100;
  boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  boolean DEFAULT_USE_COMPRESSION = true;
  int DEFAULT_FLUSH_DELAY = 2;
  int DEFAULT_PARALLELISM = 1;

  @Description("New Relic license key.")
  ValueProvider<String> getLicenseKey();
  void setLicenseKey(ValueProvider<String> licenseKey);

  @Description(
      "New Relic Logs API url. This should be routable from the VPC in which the Dataflow pipeline runs.")
  @Default.String(DEFAULT_LOGS_API_URL)
  ValueProvider<String> getLogsApiUrl();
  void setLogsApiUrl(ValueProvider<String> logsApiUrl);

  @Description(
      "Maximum number of log records to aggregate into a batch before sending them to NewRelic in a single HTTP POST request.")
  @Default.Integer(DEFAULT_BATCH_COUNT)
  ValueProvider<Integer> getBatchCount();
  void setBatchCount(ValueProvider<Integer> batchCount);

  @Description(
      "Number of seconds to wait for additional logs (up to batchCount) since the reception of the last log record in non-full batch, before flushing them to New Relic Logs.")
  @Default.Integer(DEFAULT_FLUSH_DELAY)
  ValueProvider<Integer> getFlushDelay();
  void setFlushDelay(ValueProvider<Integer> flushDelay);

  @Description("Disable SSL certificate validation.")
  @Default.Boolean(DEFAULT_DISABLE_CERTIFICATE_VALIDATION)
  ValueProvider<Boolean> getDisableCertificateValidation();
  void setDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation);

  @Description("Maximum number of parallel requests.")
  @Default.Integer(DEFAULT_PARALLELISM)
  ValueProvider<Integer> getParallelism();
  void setParallelism(ValueProvider<Integer> parallelism);

  @Description(
      "KMS Encryption Key for the token. The Key should be in the format "
          + "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
  ValueProvider<String> getTokenKMSEncryptionKey();
  void setTokenKMSEncryptionKey(ValueProvider<String> keyName);

  @Description("True to compress (in GZIP) the payloads sent to the New Relic Logs API.")
  @Default.Boolean(DEFAULT_USE_COMPRESSION)
  ValueProvider<Boolean> getUseCompression();
  void setUseCompression(ValueProvider<Boolean> useCompression);
}
