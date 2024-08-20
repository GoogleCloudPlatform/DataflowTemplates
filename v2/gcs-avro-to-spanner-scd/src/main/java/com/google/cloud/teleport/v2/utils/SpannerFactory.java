/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.threeten.bp.Duration;

/** Creates Spanner and DatabaseClient instances to access Spanner data. */
public class SpannerFactory {

  private transient SpannerConfig spannerConfig;
  private transient Spanner spanner;

  SpannerFactory(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  /**
   * Initializes SpannerFactory with the given SpannerConfig.
   *
   * @param spannerConfig
   * @return SpannerFactory to create DatabaseClient(s).
   */
  public static SpannerFactory withSpannerConfig(SpannerConfig spannerConfig) {
    return new SpannerFactory(spannerConfig);
  }

  private Spanner getOrCreateSpannerService() {
    if (spanner == null) {
      SpannerOptions.Builder optionsBuilder =
          SpannerOptions.newBuilder()
              .setHeaderProvider(
                  FixedHeaderProvider.create(
                      "User-Agent", "cloud-solutions/gcs-avro-to-spanner-scd"))
              .setHost(spannerConfig.getHost().get())
              .setProjectId(spannerConfig.getProjectId().get());

      RetrySettings retrySettings =
          RetrySettings.newBuilder()
              .setInitialRpcTimeout(Duration.ofHours(2))
              .setMaxRpcTimeout(Duration.ofHours(2))
              .setTotalTimeout(Duration.ofHours(2))
              .setRpcTimeoutMultiplier(1.0)
              .setInitialRetryDelay(Duration.ofSeconds(2))
              .setMaxRetryDelay(Duration.ofSeconds(60))
              .setRetryDelayMultiplier(1.5)
              .setMaxAttempts(100)
              .build();

      optionsBuilder.getSpannerStubSettingsBuilder().readSettings().setRetrySettings(retrySettings);

      optionsBuilder
          .getSpannerStubSettingsBuilder()
          .batchWriteSettings()
          .setRetrySettings(retrySettings);

      // This property sets the default timeout between 2 response packets in the client library.
      System.setProperty("com.google.cloud.spanner.watchdogTimeoutSeconds", "7200");

      spanner = optionsBuilder.build().getService();
    }

    return spanner;
  }

  /**
   * Gets or creates a Spanner Database client.
   *
   * <p>The client is configured using SpannerConfig variables and with additional retry logic.
   *
   * @return DatabaseClient to connect to Spanner.
   */
  public DatabaseClient getDatabaseClient() {
    return getOrCreateSpannerService()
        .getDatabaseClient(
            DatabaseId.of(
                spannerConfig.getProjectId().get(),
                spannerConfig.getInstanceId().get(),
                spannerConfig.getDatabaseId().get()));
  }

  /** Closes Spanner client. */
  public void close() {
    if (spanner != null) {
      spanner.close();
    }
  }

  /**
   * Returns whether Spanner client is closed.
   *
   * @return True if the Spanner client is closed. False otherwise.
   */
  public boolean isClosed() {
    if (spanner == null) {
      return true;
    }
    return spanner.isClosed();
  }
}
