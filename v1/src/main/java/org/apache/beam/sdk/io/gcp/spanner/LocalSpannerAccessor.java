/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import org.threeten.bp.Duration;

/** Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class LocalSpannerAccessor implements AutoCloseable {
  /* A common user agent token that indicates that this request was originated from
   * Apache Beam. Setting the user-agent allows Cloud Spanner to detect that the
   * workload is coming from Dataflow and to potentially apply performance optimizations
   */
  private final SpannerAccessor originalAccessor;

  private LocalSpannerAccessor(SpannerAccessor originalAccessor) {
    this.originalAccessor = originalAccessor;
  }

  public static LocalSpannerAccessor getOrCreate(SpannerConfig spannerConfig) {
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
    spannerConfig = spannerConfig.withExecuteStreamingSqlRetrySettings(retrySettings);
    // This property sets the default timeout between 2 response packets in the client library.
    System.setProperty("com.google.cloud.spanner.watchdogTimeoutSeconds", "7200");
    return new LocalSpannerAccessor(SpannerAccessor.getOrCreate(spannerConfig));
  }

  public DatabaseClient getDatabaseClient() {
    return originalAccessor.getDatabaseClient();
  }

  public BatchClient getBatchClient() {
    return originalAccessor.getBatchClient();
  }

  public DatabaseAdminClient getDatabaseAdminClient() {
    return originalAccessor.getDatabaseAdminClient();
  }

  @Override
  public void close() {
    originalAccessor.close();
  }
}
