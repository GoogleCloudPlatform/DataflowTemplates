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
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.ReplicaInfo;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * Verifies at pipeline execution time that the maximum possible worker parallelism of a Dataflow
 * batch job does not exceed the allowed Cloud Spanner Data Boost concurrency quota ({@code
 * spanner.googleapis.com/data_boost_quota}).
 *
 * <h3>Background</h3>
 *
 * <p>When Spanner Data Boost is enabled ({@code --dataBoostEnabled=true}), each active partitioned
 * {@code ExecuteStreamingSql} or {@code StreamingRead} RPC consumes 1 unit of the per-project,
 * per-region Data Boost concurrency quota ({@code DataBoostQuotaPerProjectPerRegion}, metric {@code
 * spanner.googleapis.com/data_boost_quota}, unit {@code 1/5min/{project}/{region}}). In Dataflow
 * batch pipelines, each worker harness thread can execute one partition read concurrently, so the
 * worst-case number of concurrent Data Boost requests is:
 *
 * <pre>{@code
 * maxParallelism = maxNumWorkers * threadsPerWorker
 * }</pre>
 *
 * <p>where {@code threadsPerWorker} is {@code --numberOfWorkerHarnessThreads} if explicitly set, or
 * the worker VM's vCPU count ({@link Runtime#availableProcessors()}) otherwise.
 *
 * <h3>Validation Behavior</h3>
 *
 * <ul>
 *   <li>If Data Boost is disabled, this transform is a no-op.
 *   <li>If {@code --maxNumWorkers} is not specified ({@code <= 0}), this transform logs a detailed
 *       warning and skips validation without failing the job.
 *   <li>If {@code --maxNumWorkers} is specified ({@code > 0}), this transform resolves the allowed
 *       parallelism (from the user-supplied {@code --maxDataBoostParallelism} override if set, or
 *       by querying the Service Usage Consumer Quota API for the Spanner project and instance
 *       region(s), falling back to {@link #DEFAULT_DATA_BOOST_QUOTA} on any API error) and fails
 *       fast with an {@link IllegalArgumentException} if {@code maxParallelism >
 *       allowedParallelism}.
 * </ul>
 */
public class VerifyDataBoostParallelism extends PTransform<PBegin, PCollection<Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(VerifyDataBoostParallelism.class);

  /**
   * Default Spanner Data Boost concurrent requests quota used as a safe fallback when the Service
   * Usage Consumer Quota API cannot be reached or does not return a valid limit. Standard regions
   * outside {@code us-central1} default to 400 concurrent operations (while {@code us-central1}
   * defaults to 1,000).
   */
  public static final long DEFAULT_DATA_BOOST_QUOTA = 400L;

  /**
   * REST endpoint template for fetching the {@code spanner.googleapis.com/data_boost_quota} limit
   * (unit {@code /5min/project/region}) from the Service Usage v1beta1 Consumer Quota API. Slashes
   * in the metric name and limit unit are URL-encoded as {@code %2F} (escaped as {@code %%2F} for
   * {@link String#format}).
   */
  private static final String DATA_BOOST_QUOTA_LIMIT_URL_TEMPLATE =
      "https://serviceusage.googleapis.com/v1beta1/projects/%s/services/spanner.googleapis.com/"
          + "consumerQuotaMetrics/spanner.googleapis.com%%2Fdata_boost_quota/"
          + "limits/%%2F5min%%2Fproject%%2Fregion";

  /**
   * Prefix used by Google-managed regional Spanner instance configurations (for example, {@code
   * regional-us-central1}).
   */
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
    // Emit a single dummy element so the validation DoFn executes once on a Dataflow worker at
    // runtime (where runtime ValueProviders and the worker VM's availableProcessors() are
    // accessible). The resulting PCollection<Integer> can be passed to Wait.on(...) to gate
    // downstream transforms until this validation succeeds.
    return begin
        .apply("Create Element", Create.of(1))
        .apply(
            "Verify DataBoost Parallelism DoFn",
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c, PipelineOptions options) {
                    ValueProvider<Boolean> dataBoostEnabled = spannerConfig.getDataBoostEnabled();
                    // Only validate when Spanner Data Boost is explicitly enabled for the job.
                    if (dataBoostEnabled != null
                        && dataBoostEnabled.isAccessible()
                        && Boolean.TRUE.equals(dataBoostEnabled.get())) {

                      // Retrieve --maxNumWorkers safely (getMaxNumWorkers() returns an Integer
                      // which may be null or 0 when not explicitly configured by the user).
                      DataflowPipelineWorkerPoolOptions poolOptions =
                          options.as(DataflowPipelineWorkerPoolOptions.class);
                      int maxNumWorkers =
                          Optional.ofNullable(poolOptions.getMaxNumWorkers()).orElse(0);

                      if (maxNumWorkers <= 0) {
                        // When --maxNumWorkers is not specified, we cannot compute a deterministic
                        // upper bound on worker parallelism. Log an actionable warning and allow
                        // the pipeline to proceed without performing quota validation.
                        LOG.warn(
                            "You have not specified --maxNumWorkers. When Spanner Data Boost is"
                                + " enabled, the job is governed by the Spanner Data Boost"
                                + " concurrent requests quota limit (see"
                                + " https://cloud.google.com/spanner/docs/databoost/databoost-quotas)."
                                + " Skipping Spanner Data Boost parallelism validation. If the"
                                + " Dataflow job scales up such that concurrent requests exceed"
                                + " this quota, there is a possibility that the export job might"
                                + " fail. It is highly recommended to set --maxNumWorkers and"
                                + " --workerMachineType parameters such that the parallelism is"
                                + " below the quota limit.");
                      } else {
                        // Determine the number of harness threads per worker. In Dataflow batch
                        // runner, if --numberOfWorkerHarnessThreads is not explicitly set, the
                        // worker harness defaults to 1 thread per vCPU on the worker machine.
                        DataflowPipelineDebugOptions debugOptions =
                            options.as(DataflowPipelineDebugOptions.class);
                        int numWorkerHarnessThreads =
                            Optional.ofNullable(debugOptions.getNumberOfWorkerHarnessThreads())
                                .orElse(0);
                        int threadsPerWorker =
                            numWorkerHarnessThreads > 0
                                ? numWorkerHarnessThreads
                                : Runtime.getRuntime().availableProcessors();

                        // Compute worst-case concurrent Data Boost requests across all workers.
                        long maxParallelism = (long) maxNumWorkers * threadsPerWorker;
                        long allowedParallelism = resolveMaxDataBoostParallelism(options);

                        // Fail fast before launching expensive export queries if the configured
                        // worker parallelism can exceed the allowed Data Boost quota.
                        if (maxParallelism > allowedParallelism) {
                          String errorMessage =
                              String.format(
                                  "Job max parallelism (%d workers * %d threads/worker = %d"
                                      + " concurrent requests) exceeds Spanner Data Boost quota"
                                      + " (%d). Reduce --maxNumWorkers or increase quota. If"
                                      + " required, set the --maxDataBoostParallelism parameter to"
                                      + " a very high value to bypass this validation and proceed"
                                      + " with the export.",
                                  maxNumWorkers,
                                  threadsPerWorker,
                                  maxParallelism,
                                  allowedParallelism);
                          LOG.error(errorMessage);
                          throw new IllegalArgumentException(errorMessage);
                        }
                      }
                    }
                    c.output(c.element());
                  }
                }));
  }

  /**
   * Resolves the maximum allowed concurrent Data Boost requests in the following precedence order:
   *
   * <ol>
   *   <li>If the user explicitly provided a positive {@code --maxDataBoostParallelism} parameter,
   *       returns that value directly (bypassing the Service Usage Quota API).
   *   <li>Otherwise, resolves the Spanner project ID and serving replica region(s) and queries the
   *       Service Usage Consumer Quota API.
   *   <li>If any unexpected error occurs, logs a warning and falls back to {@link
   *       #DEFAULT_DATA_BOOST_QUOTA} (400).
   * </ol>
   */
  @VisibleForTesting
  long resolveMaxDataBoostParallelism(PipelineOptions options) {
    try {
      // 1. Check for an explicit user-configured override (--maxDataBoostParallelism).
      if (maxDataBoostParallelism != null && maxDataBoostParallelism.isAccessible()) {
        Integer configuredLimit = maxDataBoostParallelism.get();
        if (configuredLimit != null && configuredLimit > 0) {
          LOG.info("Using user-configured maxDataBoostParallelism: {}", configuredLimit);
          return configuredLimit;
        }
      }

      // 2. Otherwise, query the live Data Boost quota for the Spanner project and region(s).
      String projectId = resolveProjectId(options);
      Set<String> regions = resolveRegions();
      return getDataBoostQuota(projectId, regions, requestFactorySupplier.get());
    } catch (Exception e) {
      LOG.warn(
          "Unexpected error resolving Spanner Data Boost quota; defaulting to {}: {}",
          DEFAULT_DATA_BOOST_QUOTA,
          e.getMessage());
      return DEFAULT_DATA_BOOST_QUOTA;
    }
  }

  /**
   * Resolves the Google Cloud project ID that owns the target Spanner database.
   *
   * <p>Spanner Data Boost quota ({@code spanner.googleapis.com/data_boost_quota}) is always charged
   * against the project that owns the Spanner instance/database, even when the Dataflow job runs in
   * a different project. Therefore, {@link SpannerConfig#getProjectId()} takes precedence over
   * {@link DataflowPipelineOptions#getProject()}.
   */
  @VisibleForTesting
  String resolveProjectId(PipelineOptions options) {
    // 1. Prefer the Spanner project ID from SpannerConfig (--spannerProjectId).
    try {
      ValueProvider<String> configProject = spannerConfig.getProjectId();
      if (configProject != null && configProject.isAccessible()) {
        String projectId = configProject.get();
        if (!Strings.isNullOrEmpty(projectId)) {
          return projectId;
        }
      }
    } catch (Exception e) {
      LOG.debug("Unable to resolve project ID from SpannerConfig", e);
    }
    // 2. Fall back to the Dataflow job's project ID (--project).
    try {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      String projectId = dataflowOptions.getProject();
      if (!Strings.isNullOrEmpty(projectId)) {
        return projectId;
      }
    } catch (Exception e) {
      LOG.debug("Unable to resolve project ID from DataflowPipelineOptions", e);
    }
    // 3. Final fallback to the environment's default project ID.
    return SpannerOptions.getDefaultProjectId();
  }

  /**
   * Resolves the set of GCP regions that can serve Data Boost requests for the target Spanner
   * instance.
   *
   * <ul>
   *   <li><b>Primary path</b>: Fetches the {@link InstanceConfig} metadata from Spanner and
   *       collects the locations of all non-{@code WITNESS} replicas (read-write and read-only
   *       replicas hold a full copy of data and can serve Data Boost requests, whereas witness
   *       replicas only vote on commits and never serve reads). Using {@link InstanceConfig} as the
   *       primary source handles multi-region configs (e.g., {@code nam3}, {@code eur6}), custom
   *       configs ({@code custom-...}), standard regional configs ({@code regional-us-central1}),
   *       and tiered/private regional configs whose IDs contain suffixes after the GCP region name
   *       (e.g., {@code regional-us-central1-private1}, {@code regional-europe-west2-plus}).
   *   <li><b>Fallback path</b>: If {@link InstanceConfig} metadata cannot be retrieved (for
   *       example, due to missing {@code spanner.instanceConfigs.get} permission) and the
   *       configuration ID starts with {@code regional-}, extracts the region name by stripping the
   *       {@code regional-} prefix.
   * </ul>
   */
  @VisibleForTesting
  Set<String> resolveRegions() {
    ValueProvider<String> instanceId = spannerConfig.getInstanceId();
    if (instanceId != null && instanceId.isAccessible()) {
      String instanceIdValue = instanceId.get();
      if (!Strings.isNullOrEmpty(instanceIdValue)) {
        try {
          SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
          try {
            // SpannerAccessor caches the instanceConfigId (e.g. "regional-us-central1" or "nam3")
            // when establishing the connection.
            String instanceConfigId = spannerAccessor.getInstanceConfigId();
            if (!Strings.isNullOrEmpty(instanceConfigId) && !"unknown".equals(instanceConfigId)) {
              // Primary path: query InstanceConfig to inspect constituent non-witness replicas for
              // all configurations (regional, dual-region, multi-region, and custom).
              try {
                InstanceConfig instanceConfig =
                    spannerAccessor.getInstanceAdminClient().getInstanceConfig(instanceConfigId);
                if (instanceConfig != null && instanceConfig.getReplicas() != null) {
                  Set<String> replicaRegions = new LinkedHashSet<>();
                  for (ReplicaInfo replica : instanceConfig.getReplicas()) {
                    // Exclude WITNESS replicas because they do not store data or serve reads.
                    if (replica != null
                        && replica.getType() != ReplicaInfo.ReplicaType.WITNESS
                        && !Strings.isNullOrEmpty(replica.getLocation())) {
                      replicaRegions.add(replica.getLocation());
                    }
                  }
                  if (!replicaRegions.isEmpty()) {
                    return replicaRegions;
                  }
                }
              } catch (Exception e) {
                LOG.debug(
                    "Unable to fetch InstanceConfig for {}; falling back to config ID parsing",
                    instanceConfigId,
                    e);
              }
              // Fallback for standard regional configs when InstanceConfig metadata is unavailable
              // (e.g. if the caller lacks spanner.instanceConfigs.get IAM permission).
              if (instanceConfigId.startsWith(REGIONAL_CONFIG_PREFIX)) {
                return Collections.singleton(
                    instanceConfigId.substring(REGIONAL_CONFIG_PREFIX.length()));
              }
            }
          } finally {
            spannerAccessor.close();
          }
        } catch (Exception e) {
          LOG.debug("Unable to resolve region from Spanner instance config", e);
        }
      }
    }
    return Collections.emptySet();
  }

  /**
   * Creates an authenticated {@link HttpRequestFactory} using Application Default Credentials
   * (ADC). On Dataflow worker VMs, ADC is automatically provided by the GCE metadata server.
   */
  @VisibleForTesting
  static HttpRequestFactory createDefaultRequestFactory() throws IOException {
    return createRequestFactory(GoogleCredentials.getApplicationDefault());
  }

  /**
   * Wraps the given {@link GoogleCredentials} with the {@code cloud-platform} OAuth scope and
   * returns an {@link HttpRequestFactory} for calling Google Cloud REST APIs.
   */
  @VisibleForTesting
  static HttpRequestFactory createRequestFactory(GoogleCredentials credentials) {
    GoogleCredentials scopedCredentials =
        credentials.createScoped(
            Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    return new NetHttpTransport()
        .createRequestFactory(new HttpCredentialsAdapter(scopedCredentials));
  }

  /**
   * Fetches the Spanner Data Boost concurrent requests quota ({@code
   * spanner.googleapis.com/data_boost_quota}) for a given project and set of Spanner regions from
   * the Service Usage Consumer Quota API.
   *
   * <h3>Multi-Region Quota Handling</h3>
   *
   * <p>Spanner does not maintain a single aggregated multi-region Data Boost quota bucket; instead,
   * quota is enforced independently in each constituent GCP region where the Spanner Frontend
   * receives the streaming RPC. Because GFE/GSLB routes requests based on network proximity to the
   * client (and all Dataflow workers run in a single GCP region), up to 100% of a job's Data Boost
   * requests can be routed to a single constituent Spanner region. Therefore, for multi-region
   * instances, this method returns the <b>minimum</b> effective quota across all serving replica
   * regions.
   *
   * <h3>Error & Unlimited Quota Handling</h3>
   *
   * <ul>
   *   <li>The Service Usage API represents an unlimited quota with {@code effectiveLimit == -1},
   *       which this method maps to {@link Long#MAX_VALUE}.
   *   <li>If any error occurs while fetching or parsing the quota, this method logs a warning and
   *       defaults to {@link #DEFAULT_DATA_BOOST_QUOTA} (400) without throwing an exception.
   * </ul>
   */
  @VisibleForTesting
  static long getDataBoostQuota(
      String projectId, Set<String> regions, HttpRequestFactory requestFactory) {
    if (Strings.isNullOrEmpty(projectId) || requestFactory == null) {
      LOG.warn(
          "Project ID or HttpRequestFactory is null or empty when querying Spanner Data Boost"
              + " quota; defaulting to {}",
          DEFAULT_DATA_BOOST_QUOTA);
      return DEFAULT_DATA_BOOST_QUOTA;
    }

    try {
      String url = String.format(DATA_BOOST_QUOTA_LIMIT_URL_TEMPLATE, projectId);
      HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
      // Set x-goog-user-project so Service Usage bills/checks quota against the target project.
      request.getHeaders().set("x-goog-user-project", projectId);

      HttpResponse response = request.execute();
      String jsonResponse;
      try {
        jsonResponse = response.parseAsString();
      } finally {
        response.disconnect();
      }

      JsonObject root = JsonParser.parseString(Strings.nullToEmpty(jsonResponse)).getAsJsonObject();
      JsonArray quotaBuckets =
          root.has("quotaBuckets") && root.get("quotaBuckets").isJsonArray()
              ? root.getAsJsonArray("quotaBuckets")
              : null;
      if (quotaBuckets == null) {
        LOG.warn(
            "No quotaBuckets found in ConsumerQuotaLimit response for project={}, regions={};"
                + " defaulting to {}",
            projectId,
            regions,
            DEFAULT_DATA_BOOST_QUOTA);
        return DEFAULT_DATA_BOOST_QUOTA;
      }

      // Parse all quota buckets from the response:
      // - Buckets with a {"dimensions": {"region": "<region>"}} object represent region-specific
      //   limits (e.g. us-central1 defaulting to 1000, or custom regional quota overrides).
      // - The bucket without a "dimensions" field represents the default limit across all other
      //   regions (typically 400).
      long defaultLimit = 0;
      Map<String, Long> regionalLimits = new HashMap<>();
      for (JsonElement element : quotaBuckets) {
        JsonObject bucket = element.getAsJsonObject();
        if (!bucket.has("effectiveLimit") || bucket.get("effectiveLimit").isJsonNull()) {
          continue;
        }
        long rawLimit = bucket.get("effectiveLimit").getAsLong();
        // Service Usage Consumer Quota API returns -1 when a quota is unlimited.
        long effectiveLimit = rawLimit == -1 ? Long.MAX_VALUE : rawLimit;

        if (bucket.has("dimensions") && bucket.get("dimensions").isJsonObject()) {
          JsonObject dimensions = bucket.getAsJsonObject("dimensions");
          if (dimensions.has("region")
              && !dimensions.get("region").isJsonNull()
              && effectiveLimit > 0) {
            regionalLimits.put(dimensions.get("region").getAsString(), effectiveLimit);
          }
        } else {
          // Bucket without region dimension is the default limit across all other regions.
          defaultLimit = effectiveLimit;
        }
      }

      // If one or more serving regions were resolved for the Spanner instance, look up each
      // region's specific limit (falling back to defaultLimit if the region has no override bucket)
      // and take the minimum across all serving regions.
      if (regions != null && !regions.isEmpty()) {
        long minRegionLimit = Long.MAX_VALUE;
        boolean foundValidLimit = false;
        for (String region : regions) {
          if (!Strings.isNullOrEmpty(region)) {
            long regionLimit = regionalLimits.getOrDefault(region, defaultLimit);
            if (regionLimit > 0) {
              minRegionLimit = Math.min(minRegionLimit, regionLimit);
              foundValidLimit = true;
            }
          }
        }
        if (foundValidLimit) {
          LOG.info(
              "Fetched Spanner Data Boost quota for project={}, regions={}: {}",
              projectId,
              regions,
              minRegionLimit);
          return minRegionLimit;
        }
      } else if (defaultLimit > 0) {
        // If the instance's region(s) could not be resolved, use the project's default regional
        // limit from the Quota API if positive.
        LOG.info(
            "Fetched Spanner Data Boost default quota for project={}, regions={}: {}",
            projectId,
            regions,
            defaultLimit);
        return defaultLimit;
      }

      LOG.warn(
          "Quota API returned non-positive limit ({}) for project={}, regions={}; defaulting to {}",
          defaultLimit,
          projectId,
          regions,
          DEFAULT_DATA_BOOST_QUOTA);
    } catch (Exception e) {
      LOG.warn(
          "Failed to fetch Spanner Data Boost quota for project={}, regions={}; defaulting to {}:"
              + " {}",
          projectId,
          regions,
          DEFAULT_DATA_BOOST_QUOTA,
          e.getMessage());
    }
    return DEFAULT_DATA_BOOST_QUOTA;
  }
}
