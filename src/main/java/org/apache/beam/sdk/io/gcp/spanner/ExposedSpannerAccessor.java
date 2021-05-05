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
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
public class ExposedSpannerAccessor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ExposedSpannerAccessor.class);

  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";

  // Only create one ExposedSpannerAccessor for each different SpannerConfig.
  private static final ConcurrentHashMap<SpannerConfig, ExposedSpannerAccessor>
      exposedSpannerAccessors = new ConcurrentHashMap<>();

  // Keep reference counts of each ExposedSpannerAccessor's usage so that we can close
  // it when it is no longer in use.
  private static final ConcurrentHashMap<SpannerConfig, AtomicInteger> refcounts =
      new ConcurrentHashMap<>();

  private final Spanner spanner;
  private final DatabaseClient databaseClient;
  private final BatchClient batchClient;
  private final DatabaseAdminClient databaseAdminClient;
  private final SpannerConfig spannerConfig;

  private ExposedSpannerAccessor(
      Spanner spanner,
      DatabaseClient databaseClient,
      DatabaseAdminClient databaseAdminClient,
      BatchClient batchClient,
      SpannerConfig spannerConfig) {
    this.spanner = spanner;
    this.databaseClient = databaseClient;
    this.databaseAdminClient = databaseAdminClient;
    this.batchClient = batchClient;
    this.spannerConfig = spannerConfig;
  }

  public static ExposedSpannerAccessor create(SpannerConfig spannerConfig) {
    return getOrCreate(spannerConfig);
  }

  static ExposedSpannerAccessor getOrCreate(SpannerConfig spannerConfig) {

    ExposedSpannerAccessor self = exposedSpannerAccessors.get(spannerConfig);
    if (self == null) {
      synchronized (exposedSpannerAccessors) {
        // Re-check that it has not been created before we got the lock.
        self = exposedSpannerAccessors.get(spannerConfig);
        if (self == null) {
          // Connect to spanner for this SpannerConfig.
          LOG.info("Connecting to {}", spannerConfig);
          self = ExposedSpannerAccessor.createAndConnect(spannerConfig);
          exposedSpannerAccessors.put(spannerConfig, self);
          refcounts.putIfAbsent(spannerConfig, new AtomicInteger(0));
        }
      }
    }
    // Add refcount for this spannerConfig.
    int refcount = refcounts.get(spannerConfig).incrementAndGet();
    LOG.debug("getOrCreate(): refcount={} for {}", refcount, spannerConfig);
    return self;
  }

  private static ExposedSpannerAccessor createAndConnect(SpannerConfig spannerConfig) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();

    ValueProvider<Duration> commitDeadline = spannerConfig.getCommitDeadline();
    if (commitDeadline != null && commitDeadline.get().getMillis() > 0) {

      // Set the GRPC deadline on the Commit API call.
      UnaryCallSettings.Builder<CommitRequest, CommitResponse> commitSettings =
          builder.getSpannerStubSettingsBuilder().commitSettings();
      RetrySettings.Builder commitRetrySettings = commitSettings.getRetrySettings().toBuilder();
      commitSettings.setRetrySettings(
          commitRetrySettings
              .setTotalTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setMaxRpcTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setInitialRpcTimeout(
                  org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .build());
    }

    // Setting the timeout for streaming read to 2 hours. This is 1 hour by default
    // after BEAM 2.20.
    ServerStreamingCallSettings.Builder<ExecuteSqlRequest, PartialResultSet>
        executeStreamingSqlSettings =
            builder.getSpannerStubSettingsBuilder().executeStreamingSqlSettings();
    RetrySettings.Builder executeSqlStreamingRetrySettings =
        executeStreamingSqlSettings.getRetrySettings().toBuilder();
    executeStreamingSqlSettings.setRetrySettings(
        executeSqlStreamingRetrySettings
            .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .build());

    ValueProvider<String> projectId = spannerConfig.getProjectId();
    if (projectId != null) {
      builder.setProjectId(projectId.get());
    }
    ServiceFactory<Spanner, SpannerOptions> serviceFactory = spannerConfig.getServiceFactory();
    if (serviceFactory != null) {
      builder.setServiceFactory(serviceFactory);
    }
    ValueProvider<String> host = spannerConfig.getHost();
    if (host != null) {
      builder.setHost(host.get());
    }
    ValueProvider<String> emulatorHost = spannerConfig.getEmulatorHost();
    if (emulatorHost != null) {
      builder.setEmulatorHost(emulatorHost.get());
      builder.setCredentials(NoCredentials.getInstance());
    }
    String userAgentString = USER_AGENT_PREFIX + "/" + ReleaseInfo.getReleaseInfo().getVersion();
    builder.setHeaderProvider(FixedHeaderProvider.create("user-agent", userAgentString));

    SessionPoolOptions.Builder sessionPoolOptions = SessionPoolOptions.newBuilder();
    sessionPoolOptions.setMinSessions(Runtime.getRuntime().availableProcessors());
    sessionPoolOptions.setMaxSessions(300);
    builder.setSessionPoolOption(sessionPoolOptions.build());

    SpannerOptions options = builder.build();

    Spanner spanner = options.getService();
    String instanceId = spannerConfig.getInstanceId().get();
    String databaseId = spannerConfig.getDatabaseId().get();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    BatchClient batchClient =
        spanner.getBatchClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    return new ExposedSpannerAccessor(
        spanner, databaseClient, databaseAdminClient, batchClient, spannerConfig);
  }

  public DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  public BatchClient getBatchClient() {
    return batchClient;
  }

  public DatabaseAdminClient getDatabaseAdminClient() {
    return databaseAdminClient;
  }

  @Override
  public void close() {
    // Only close Spanner when present in map and refcount == 0
    int refcount = refcounts.getOrDefault(spannerConfig, new AtomicInteger(0)).decrementAndGet();
    LOG.debug("close(): refcount={} for {}", refcount, spannerConfig);

    if (refcount == 0) {
      synchronized (exposedSpannerAccessors) {
        // Re-check refcount in case it has increased outside the lock.
        if (refcounts.get(spannerConfig).get() <= 0) {
          exposedSpannerAccessors.remove(spannerConfig);
          refcounts.remove(spannerConfig);
          LOG.info("Closing {} ", spannerConfig);
          spanner.close();
        }
      }
    }
  }
}
