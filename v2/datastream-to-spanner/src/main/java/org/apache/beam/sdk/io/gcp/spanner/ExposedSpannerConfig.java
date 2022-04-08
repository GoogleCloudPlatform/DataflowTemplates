/*
 * Copyright (C) 2018 Google LLC
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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

/** Exposed spanner config. */
public class ExposedSpannerConfig extends SpannerConfig {
  // A default host name for batch traffic.
  private static final String DEFAULT_HOST = "https://batch-spanner.googleapis.com/";
  // Deadline for Commit API call.
  private static final Duration DEFAULT_COMMIT_DEADLINE = Duration.standardSeconds(15);
  // Total allowable backoff time.
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardMinutes(15);

  private final ValueProvider<String> projectId;

  private final ValueProvider<String> instanceId;

  private final ValueProvider<String> databaseId;

  private final ValueProvider<String> host;

  private final ValueProvider<String> emulatorHost;

  private final ValueProvider<Duration> commitDeadline;

  private final ValueProvider<Duration> maxCumulativeBackoff;

  private final ValueProvider<RpcPriority> rpcPriority;

  private final ServiceFactory<Spanner, SpannerOptions> serviceFactory;

  private ExposedSpannerConfig(
      @Nullable ValueProvider<String> projectId,
      @Nullable ValueProvider<String> instanceId,
      @Nullable ValueProvider<String> databaseId,
      @Nullable ValueProvider<String> host,
      @Nullable ValueProvider<String> emulatorHost,
      @Nullable ValueProvider<Duration> commitDeadline,
      @Nullable ValueProvider<Duration> maxCumulativeBackoff,
      @Nullable ValueProvider<RpcPriority> rpcPriority,
      @Nullable ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.host = host;
    this.emulatorHost = emulatorHost;
    this.commitDeadline = commitDeadline;
    this.maxCumulativeBackoff = maxCumulativeBackoff;
    this.rpcPriority = rpcPriority;
    this.serviceFactory = serviceFactory;
  }

  @Nullable
  @Override
  public ValueProvider<String> getProjectId() {
    return projectId;
  }

  @Nullable
  @Override
  public ValueProvider<String> getInstanceId() {
    return instanceId;
  }

  @Nullable
  @Override
  public ValueProvider<String> getDatabaseId() {
    return databaseId;
  }

  @Nullable
  @Override
  public ValueProvider<String> getHost() {
    return host;
  }

  @Nullable
  @Override
  public ValueProvider<String> getEmulatorHost() {
    return emulatorHost;
  }

  @Nullable
  @Override
  public ValueProvider<Duration> getCommitDeadline() {
    return commitDeadline;
  }

  @Nullable
  @Override
  public ValueProvider<Duration> getMaxCumulativeBackoff() {
    return maxCumulativeBackoff;
  }

  @Nullable
  @Override
  public ValueProvider<RpcPriority> getRpcPriority() {
    return rpcPriority;
  }

  @Nullable
  @VisibleForTesting
  @Override
  ServiceFactory<Spanner, SpannerOptions> getServiceFactory() {
    return serviceFactory;
  }

  @Override
  public String toString() {
    return "SpannerConfig{"
        + "projectId="
        + projectId
        + ", "
        + "instanceId="
        + instanceId
        + ", "
        + "databaseId="
        + databaseId
        + ", "
        + "host="
        + host
        + ", "
        + "emulatorHost="
        + emulatorHost
        + ", "
        + "commitDeadline="
        + commitDeadline
        + ", "
        + "maxCumulativeBackoff="
        + maxCumulativeBackoff
        + ", "
        + "serviceFactory="
        + serviceFactory
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerConfig) {
      SpannerConfig that = (SpannerConfig) o;
      return ((this.projectId == null)
              ? (that.getProjectId() == null)
              : this.projectId.equals(that.getProjectId()))
          && ((this.instanceId == null)
              ? (that.getInstanceId() == null)
              : this.instanceId.equals(that.getInstanceId()))
          && ((this.databaseId == null)
              ? (that.getDatabaseId() == null)
              : this.databaseId.equals(that.getDatabaseId()))
          && ((this.host == null) ? (that.getHost() == null) : this.host.equals(that.getHost()))
          && ((this.emulatorHost == null)
              ? (that.getEmulatorHost() == null)
              : this.emulatorHost.equals(that.getEmulatorHost()))
          && ((this.commitDeadline == null)
              ? (that.getCommitDeadline() == null)
              : this.commitDeadline.equals(that.getCommitDeadline()))
          && ((this.maxCumulativeBackoff == null)
              ? (that.getMaxCumulativeBackoff() == null)
              : this.maxCumulativeBackoff.equals(that.getMaxCumulativeBackoff()))
          && ((this.serviceFactory == null)
              ? (that.getServiceFactory() == null)
              : this.serviceFactory.equals(that.getServiceFactory()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode *= 1000003;
    hashcode ^= (projectId == null) ? 0 : projectId.hashCode();
    hashcode *= 1000003;
    hashcode ^= (instanceId == null) ? 0 : instanceId.hashCode();
    hashcode *= 1000003;
    hashcode ^= (databaseId == null) ? 0 : databaseId.hashCode();
    hashcode *= 1000003;
    hashcode ^= (host == null) ? 0 : host.hashCode();
    hashcode *= 1000003;
    hashcode ^= (emulatorHost == null) ? 0 : emulatorHost.hashCode();
    hashcode *= 1000003;
    hashcode ^= (commitDeadline == null) ? 0 : commitDeadline.hashCode();
    hashcode *= 1000003;
    hashcode ^= (maxCumulativeBackoff == null) ? 0 : maxCumulativeBackoff.hashCode();
    hashcode *= 1000003;
    hashcode ^= (serviceFactory == null) ? 0 : serviceFactory.hashCode();
    return hashcode;
  }

  @Override
  SpannerConfig.Builder toBuilder() {
    return new Builder(this);
  }

  public static SpannerConfig create() {
    return builder()
        .setHost(ValueProvider.StaticValueProvider.of(DEFAULT_HOST))
        .setCommitDeadline(ValueProvider.StaticValueProvider.of(DEFAULT_COMMIT_DEADLINE))
        .setMaxCumulativeBackoff(
            ValueProvider.StaticValueProvider.of(DEFAULT_MAX_CUMULATIVE_BACKOFF))
        .build();
  }

  static Builder builder() {
    return new ExposedSpannerConfig.Builder();
  }

  static final class Builder extends SpannerConfig.Builder {
    private ValueProvider<String> projectId;
    private ValueProvider<String> instanceId;
    private ValueProvider<String> databaseId;
    private ValueProvider<String> host;
    private ValueProvider<String> emulatorHost;
    private ValueProvider<Duration> commitDeadline;
    private ValueProvider<Duration> maxCumulativeBackoff;
    private ValueProvider<RpcPriority> rpcPriority;
    private ServiceFactory<Spanner, SpannerOptions> serviceFactory;

    Builder() {}

    private Builder(SpannerConfig source) {
      this.projectId = source.getProjectId();
      this.instanceId = source.getInstanceId();
      this.databaseId = source.getDatabaseId();
      this.host = source.getHost();
      this.emulatorHost = source.getEmulatorHost();
      this.commitDeadline = source.getCommitDeadline();
      this.maxCumulativeBackoff = source.getMaxCumulativeBackoff();
      this.rpcPriority = source.getRpcPriority();
      this.serviceFactory = source.getServiceFactory();
    }

    @Override
    ExposedSpannerConfig.Builder setProjectId(ValueProvider<String> projectId) {
      this.projectId = projectId;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setInstanceId(ValueProvider<String> instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setDatabaseId(ValueProvider<String> databaseId) {
      this.databaseId = databaseId;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setHost(ValueProvider<String> host) {
      this.host = host;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setEmulatorHost(ValueProvider<String> emulatorHost) {
      this.emulatorHost = emulatorHost;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setCommitDeadline(ValueProvider<Duration> commitDeadline) {
      this.commitDeadline = commitDeadline;
      return this;
    }

    @Override
    SpannerConfig.Builder setMaxCumulativeBackoff(ValueProvider<Duration> maxCumulativeBackoff) {
      this.maxCumulativeBackoff = maxCumulativeBackoff;
      return this;
    }

    @Override
    SpannerConfig.Builder setRpcPriority(ValueProvider<RpcPriority> rpcPriority) {
      this.rpcPriority = rpcPriority;
      return this;
    }

    @Override
    ExposedSpannerConfig.Builder setServiceFactory(
        ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      this.serviceFactory = serviceFactory;
      return this;
    }

    @Override
    public ExposedSpannerConfig build() {
      return new ExposedSpannerConfig(
          this.projectId,
          this.instanceId,
          this.databaseId,
          this.host,
          this.emulatorHost,
          this.commitDeadline,
          this.maxCumulativeBackoff,
          this.rpcPriority,
          this.serviceFactory);
    }
  }
}
