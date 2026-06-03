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

import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * The NewRelicConfig contains the {@link NewRelicPipelineOptions} that were supplied when starting
 * the Apache Beam job, and which will be used by the {@link
 * com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO} transform to conveniently batch and
 * send the processed logs to New Relic.
 */
public class NewRelicConfig {
  private final ValueProvider<String> logsApiUrl;
  private final ValueProvider<String> licenseKey;
  private final ValueProvider<Integer> batchCount;
  private final ValueProvider<Integer> parallelism;
  private final ValueProvider<Boolean> disableCertificateValidation;
  private final ValueProvider<Boolean> useCompression;
  private final ValueProvider<Integer> flushDelay;

  private NewRelicConfig(
      final ValueProvider<String> logsApiUrl,
      final ValueProvider<String> licenseKey,
      final ValueProvider<Integer> batchCount,
      final ValueProvider<Integer> flushDelay,
      final ValueProvider<Integer> parallelism,
      final ValueProvider<Boolean> disableCertificateValidation,
      final ValueProvider<Boolean> useCompression) {
    this.logsApiUrl = logsApiUrl;
    this.licenseKey = licenseKey;
    this.batchCount = batchCount;
    this.flushDelay = flushDelay;
    this.parallelism = parallelism;
    this.disableCertificateValidation = disableCertificateValidation;
    this.useCompression = useCompression;
  }

  /**
   * Factory method to build a {@link NewRelicConfig} out of the supplied {@link
   * NewRelicPipelineOptions} supplied by the user. Default values are configured via @Default
   * annotations in {@link NewRelicPipelineOptions}.
   *
   * @param newRelicOptions The supplied options when executing the pipeline
   * @return The options to be used to execute the pipeline.
   */
  public static NewRelicConfig fromPipelineOptions(final NewRelicPipelineOptions newRelicOptions) {
    return new NewRelicConfig(
        newRelicOptions.getLogsApiUrl(),
        newRelicOptions.getTokenKMSEncryptionKey().isAccessible()
            ? maybeDecrypt(
                newRelicOptions.getLicenseKey(), newRelicOptions.getTokenKMSEncryptionKey())
            : newRelicOptions.getLicenseKey(),
        newRelicOptions.getBatchCount(),
        newRelicOptions.getFlushDelay(),
        newRelicOptions.getParallelism(),
        newRelicOptions.getDisableCertificateValidation(),
        newRelicOptions.getUseCompression());
  }

  /**
   * Utility method to decrypt a NewRelic API token.
   *
   * @param unencryptedToken The NewRelic API token as a Base64 encoded {@link String} encrypted
   *     with a Cloud KMS Key.
   * @param kmsKey The Cloud KMS Encryption Key to decrypt the NewRelic API token.
   * @return Decrypted NewRelic API token.
   */
  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedToken, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedToken, kmsKey);
  }

  public ValueProvider<String> getLogsApiUrl() {
    return logsApiUrl;
  }

  public ValueProvider<String> getLicenseKey() {
    return licenseKey;
  }

  public ValueProvider<Integer> getBatchCount() {
    return batchCount;
  }

  public ValueProvider<Integer> getFlushDelay() {
    return flushDelay;
  }

  public ValueProvider<Integer> getParallelism() {
    return parallelism;
  }

  public ValueProvider<Boolean> getDisableCertificateValidation() {
    return disableCertificateValidation;
  }

  public ValueProvider<Boolean> getUseCompression() {
    return useCompression;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("logsApiUrl", logsApiUrl)
        .append("licenseKey", licenseKey)
        .append("batchCount", batchCount)
        .append("parallelism", parallelism)
        .append("disableCertificateValidation", disableCertificateValidation)
        .append("useCompression", useCompression)
        .append("flushDelay", flushDelay)
        .toString();
  }

}
