/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.spanner.spannerio.SpannerAccessor;
import com.google.cloud.teleport.spanner.spannerio.SpannerConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the maximum worker parallelism of the Dataflow job does not exceed the allowed
 * Spanner Data Boost concurrency quota.
 */
public class VerifyDataBoostParallelism extends PTransform<PBegin, PCollection<Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(VerifyDataBoostParallelism.class);

  public static final long DEFAULT_DATA_BOOST_QUOTA = 400L;

  private static final String DATA_BOOST_QUOTA_LIMIT_URL_TEMPLATE =
      "https://serviceusage.googleapis.com/v1beta1/projects/%s/services/spanner.googleapis.com/"
          + "consumerQuotaMetrics/spanner.googleapis.com%%2Fdata_boost_quota/"
          + "limits/%%2F5min%%2Fproject%%2Fregion";

  private static final String REGIONAL_CONFIG_PREFIX = "regional-";

  /** Serializable supplier for {@link HttpRequestFactory} to allow mocking in unit tests. */
  @VisibleForTesting
  @FunctionalInterface
  interface HttpRequestFactorySupplier extends Serializable {
    HttpRequestFactory get() throws IOException;
  }

  private final SpannerConfig spannerConfig;
  private final ValueProvider<Integer> maxDataBoostParallelism;
  private final HttpRequestFactorySupplier requestFactorySupplier;

  public VerifyDataBoostParallelism(SpannerConfig spannerConfig) {
    this(spannerConfig, null);
  }

  public VerifyDataBoostParallelism(
      SpannerConfig spannerConfig, ValueProvider<Integer> maxDataBoostParallelism) {
    this(
        spannerConfig,
        maxDataBoostParallelism,
        VerifyDataBoostParallelism::createDefaultRequestFactory);
  }

  @VisibleForTesting
  VerifyDataBoostParallelism(
      SpannerConfig spannerConfig,
      ValueProvider<Integer> maxDataBoostParallelism,
      HttpRequestFactorySupplier requestFactorySupplier) {
    this.spannerConfig = spannerConfig;
    this.maxDataBoostParallelism = maxDataBoostParallelism;
    this.requestFactorySupplier = requestFactorySupplier;
  }

  @Override
  public PCollection<Integer> expand(PBegin begin) {
    return begin
        .apply("Create Element", Create.of(1))
        .apply(
            "Verify DataBoost Parallelism DoFn",
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c, PipelineOptions options) {
                    ValueProvider<Boolean> dataBoostEnabled = spannerConfig.getDataBoostEnabled();
                    if (dataBoostEnabled != null
                        && dataBoostEnabled.isAccessible()
                        && Boolean.TRUE.equals(dataBoostEnabled.get())) {

                      DataflowPipelineWorkerPoolOptions poolOptions =
                          options.as(DataflowPipelineWorkerPoolOptions.class);
                      int maxWorkers =
                          poolOptions.getMaxNumWorkers() > 0
                              ? poolOptions.getMaxNumWorkers()
                              : 1000;

                      DataflowPipelineDebugOptions debugOptions =
                          options.as(DataflowPipelineDebugOptions.class);
                      int threadsPerWorker =
                          debugOptions.getNumberOfWorkerHarnessThreads() > 0
                              ? debugOptions.getNumberOfWorkerHarnessThreads()
                              : Runtime.getRuntime().availableProcessors();

                      long maxParallelism = (long) maxWorkers * threadsPerWorker;
                      long allowedParallelism = resolveMaxDataBoostParallelism(options);

                      if (maxParallelism > allowedParallelism) {
                        throw new IllegalArgumentException(
                            String.format(
                                "Job max parallelism (%d workers * %d threads/worker = %d"
                                    + " concurrent requests) exceeds Spanner Data Boost quota"
                                    + " (%d). Reduce --maxWorkers or increase quota.",
                                maxWorkers, threadsPerWorker, maxParallelism, allowedParallelism));
                      }
                    }
                    c.output(c.element());
                  }
                }));
  }

  @VisibleForTesting
  long resolveMaxDataBoostParallelism(PipelineOptions options) {
    try {
      if (maxDataBoostParallelism != null
          && maxDataBoostParallelism.isAccessible()
          && maxDataBoostParallelism.get() != null
          && maxDataBoostParallelism.get() > 0) {
        long configuredLimit = maxDataBoostParallelism.get();
        LOG.info("Using user-configured maxDataBoostParallelism: {}", configuredLimit);
        return configuredLimit;
      }

      String projectId = resolveProjectId(options);
      String region = resolveRegion(options);
      return getDataBoostQuota(projectId, region, requestFactorySupplier.get());
    } catch (Exception e) {
      LOG.warn(
          "Unexpected error resolving Spanner Data Boost quota; defaulting to {}: {}",
          DEFAULT_DATA_BOOST_QUOTA,
          e.getMessage());
      return DEFAULT_DATA_BOOST_QUOTA;
    }
  }

  @VisibleForTesting
  String resolveProjectId(PipelineOptions options) {
    try {
      ValueProvider<String> configProject = spannerConfig.getProjectId();
      if (configProject != null
          && configProject.isAccessible()
          && !Strings.isNullOrEmpty(configProject.get())) {
        return configProject.get();
      }
    } catch (Exception e) {
      LOG.debug("Unable to resolve project ID from SpannerConfig", e);
    }
    try {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      if (!Strings.isNullOrEmpty(dataflowOptions.getProject())) {
        return dataflowOptions.getProject();
      }
    } catch (Exception e) {
      LOG.debug("Unable to resolve project ID from DataflowPipelineOptions", e);
    }
    return SpannerOptions.getDefaultProjectId();
  }

  @VisibleForTesting
  String resolveRegion(PipelineOptions options) {
    try {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      if (!Strings.isNullOrEmpty(dataflowOptions.getRegion())) {
        return dataflowOptions.getRegion();
      }
    } catch (Exception e) {
      LOG.debug("Unable to resolve region from DataflowPipelineOptions", e);
    }
    ValueProvider<String> instanceId = spannerConfig.getInstanceId();
    if (instanceId != null
        && instanceId.isAccessible()
        && !Strings.isNullOrEmpty(instanceId.get())) {
      try {
        SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
        try {
          String instanceConfigId = spannerAccessor.getInstanceConfigId();
          if (instanceConfigId.startsWith(REGIONAL_CONFIG_PREFIX)) {
            return instanceConfigId.substring(REGIONAL_CONFIG_PREFIX.length());
          }
        } finally {
          spannerAccessor.close();
        }
      } catch (Exception e) {
        LOG.debug("Unable to resolve region from Spanner instance config", e);
      }
    }
    return null;
  }

  @VisibleForTesting
  static HttpRequestFactory createDefaultRequestFactory() throws IOException {
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    return new NetHttpTransport().createRequestFactory(new HttpCredentialsAdapter(credentials));
  }

  /**
   * Fetches the Spanner Data Boost concurrent requests quota ({@code
   * spanner.googleapis.com/data_boost_quota}) for a given project and region from the Service Usage
   * Consumer Quota API. If any error occurs while fetching or parsing the quota, defaults to {@link
   * #DEFAULT_DATA_BOOST_QUOTA} (400) without throwing an exception.
   */
  @VisibleForTesting
  static long getDataBoostQuota(
      String projectId, String region, HttpRequestFactory requestFactory) {
    if (Strings.isNullOrEmpty(projectId)) {
      LOG.warn(
          "Project ID is null or empty when querying Spanner Data Boost quota; defaulting to {}",
          DEFAULT_DATA_BOOST_QUOTA);
      return DEFAULT_DATA_BOOST_QUOTA;
    }

    try {
      String url = String.format(DATA_BOOST_QUOTA_LIMIT_URL_TEMPLATE, projectId);
      HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
      request.getHeaders().set("x-goog-user-project", projectId);

      HttpResponse response = request.execute();
      String jsonResponse;
      try {
        jsonResponse = response.parseAsString();
      } finally {
        response.disconnect();
      }

      JsonObject root = JsonParser.parseString(jsonResponse).getAsJsonObject();
      JsonArray quotaBuckets = root.getAsJsonArray("quotaBuckets");
      if (quotaBuckets == null) {
        LOG.warn(
            "No quotaBuckets found in ConsumerQuotaLimit response for project={}, region={};"
                + " defaulting to {}",
            projectId,
            region,
            DEFAULT_DATA_BOOST_QUOTA);
        return DEFAULT_DATA_BOOST_QUOTA;
      }

      long defaultLimit = -1;
      for (JsonElement element : quotaBuckets) {
        JsonObject bucket = element.getAsJsonObject();
        if (!bucket.has("effectiveLimit")) {
          continue;
        }
        long effectiveLimit = bucket.get("effectiveLimit").getAsLong();

        if (bucket.has("dimensions")) {
          JsonObject dimensions = bucket.getAsJsonObject("dimensions");
          if (!Strings.isNullOrEmpty(region)
              && dimensions.has("region")
              && region.equals(dimensions.get("region").getAsString())) {
            if (effectiveLimit > 0) {
              LOG.info(
                  "Fetched Spanner Data Boost quota for project={}, region={}: {}",
                  projectId,
                  region,
                  effectiveLimit);
              return effectiveLimit;
            }
          }
        } else {
          // Bucket without region dimension is the default limit across all other regions
          defaultLimit = effectiveLimit;
        }
      }

      if (defaultLimit > 0) {
        LOG.info(
            "Fetched Spanner Data Boost default quota for project={}, region={}: {}",
            projectId,
            region,
            defaultLimit);
        return defaultLimit;
      }

      LOG.warn(
          "Quota API returned non-positive limit ({}) for project={}, region={}; defaulting to {}",
          defaultLimit,
          projectId,
          region,
          DEFAULT_DATA_BOOST_QUOTA);
    } catch (Exception e) {
      LOG.warn(
          "Failed to fetch Spanner Data Boost quota for project={}, region={}; defaulting to {}:"
              + " {}",
          projectId,
          region,
          DEFAULT_DATA_BOOST_QUOTA,
          e.getMessage());
    }
    return DEFAULT_DATA_BOOST_QUOTA;
  }
}
