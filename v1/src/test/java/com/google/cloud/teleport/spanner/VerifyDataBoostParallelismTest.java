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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.ReplicaInfo;
import com.google.cloud.spanner.ReplicaInfo.ReplicaType;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.spanner.ExportPipeline.ExportPipelineOptions;
import com.google.cloud.teleport.spanner.ExportPipeline.ExportPipelineOptions.ChecksumAlgorithm;
import com.google.cloud.teleport.spanner.spannerio.SpannerConfig;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link VerifyDataBoostParallelism} with all external calls mocked. */
public class VerifyDataBoostParallelismTest implements Serializable {

  private static final String STANDARD_QUOTA_RESPONSE_JSON =
      "{"
          + "\"quotaBuckets\": ["
          + "  {\"effectiveLimit\": \"400\", \"defaultLimit\": \"400\"},"
          + "  {\"effectiveLimit\": \"1000\", \"defaultLimit\": \"1000\","
          + "   \"dimensions\": {\"region\": \"us-central1\"}}"
          + "]"
          + "}";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** ValueProvider that reports {@code isAccessible() == false}. */
  private static class InaccessibleValueProvider<T> implements ValueProvider<T>, Serializable {
    @Override
    public T get() {
      throw new IllegalStateException("Not accessible");
    }

    @Override
    public boolean isAccessible() {
      return false;
    }
  }

  /** ValueProvider that throws a RuntimeException when accessed. */
  private static class ThrowingValueProvider<T> implements ValueProvider<T>, Serializable {
    @Override
    public T get() {
      throw new RuntimeException("Simulated provider failure");
    }

    @Override
    public boolean isAccessible() {
      return true;
    }
  }

  /** Helper to create an in-memory {@link HttpRequestFactory} returning a canned response. */
  private static HttpRequestFactory createFakeRequestFactory(int statusCode, String responseBody) {
    HttpTransport transport =
        new HttpTransport() {
          @Override
          protected LowLevelHttpRequest buildRequest(String method, String url) {
            return new LowLevelHttpRequest() {
              @Override
              public void addHeader(String name, String value) {}

              @Override
              public LowLevelHttpResponse execute() {
                return new LowLevelHttpResponse() {
                  @Override
                  public InputStream getContent() {
                    return new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8));
                  }

                  @Override
                  public String getContentEncoding() {
                    return null;
                  }

                  @Override
                  public long getContentLength() {
                    return responseBody.length();
                  }

                  @Override
                  public String getContentType() {
                    return "application/json";
                  }

                  @Override
                  public String getStatusLine() {
                    return "HTTP/1.1 " + statusCode;
                  }

                  @Override
                  public int getStatusCode() {
                    return statusCode;
                  }

                  @Override
                  public String getReasonPhrase() {
                    return statusCode == 200 ? "OK" : "Error";
                  }

                  @Override
                  public int getHeaderCount() {
                    return 0;
                  }

                  @Override
                  public String getHeaderName(int index) {
                    return null;
                  }

                  @Override
                  public String getHeaderValue(int index) {
                    return null;
                  }
                };
              }
            };
          }
        };
    return transport.createRequestFactory();
  }

  @Test
  public void testGetDataBoostQuotaMockedForRegionsAndEdgeCases() {
    HttpRequestFactory standardFactory =
        createFakeRequestFactory(200, STANDARD_QUOTA_RESPONSE_JSON);

    // 1. Regional match (us-central1 -> 1000), default bucket fallback (europe-west2 -> 400),
    // and multi-region minimum across replica regions ([us-central1, europe-west2] -> 400)
    assertEquals(
        1000L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), standardFactory));
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("europe-west2"), standardFactory));
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", ImmutableSet.of("us-central1", "europe-west2"), standardFactory));

    // 2. Null or empty projectId or null requestFactory returns 400 without making a request
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            null, Collections.singleton("us-central1"), standardFactory));
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "", Collections.singleton("us-central1"), standardFactory));
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), null));

    // 3. Missing or null quotaBuckets field returns 400
    HttpRequestFactory missingBucketsFactory = createFakeRequestFactory(200, "{}");
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), missingBucketsFactory));
    HttpRequestFactory nullBucketsFactory =
        createFakeRequestFactory(200, "{\"quotaBuckets\": null}");
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), nullBucketsFactory));

    // 4. Bucket without effectiveLimit, bucket with null effectiveLimit, bucket with non-region
    // dimension, bucket with null region dimension, buckets with multiple regional overrides,
    // bucket with matching region but non-positive effectiveLimit (0), and default bucket
    String complexJson =
        "{"
            + "\"quotaBuckets\": ["
            + "  {\"defaultLimit\": \"500\"},"
            + "  {\"effectiveLimit\": null},"
            + "  {\"effectiveLimit\": \"800\", \"dimensions\": {\"zone\": \"us-central1-a\"}},"
            + "  {\"effectiveLimit\": \"700\", \"dimensions\": {\"region\": null}},"
            + "  {\"effectiveLimit\": \"900\", \"dimensions\": {\"region\": \"us-east1\"}},"
            + "  {\"effectiveLimit\": \"1200\", \"dimensions\": {\"region\": \"us-east4\"}},"
            + "  {\"effectiveLimit\": \"-1\", \"dimensions\": {\"region\": \"us-west1\"}},"
            + "  {\"effectiveLimit\": \"0\", \"dimensions\": {\"region\": \"us-central1\"}},"
            + "  {\"effectiveLimit\": \"650\", \"dimensions\": null}"
            + "]"
            + "}";
    HttpRequestFactory complexFactory = createFakeRequestFactory(200, complexJson);
    assertEquals(
        650L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), complexFactory));
    assertEquals(
        900L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", ImmutableSet.of("us-east1", "us-east4"), complexFactory));
    assertEquals(
        Long.MAX_VALUE,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-west1"), complexFactory));
    assertEquals(
        650L, VerifyDataBoostParallelism.getDataBoostQuota("test-project", null, complexFactory));
    assertEquals(
        650L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.emptySet(), complexFactory));
    Set<String> setWithEmptyAndValid = new HashSet<>(Arrays.asList("", null, "us-east1"));
    assertEquals(
        900L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", setWithEmptyAndValid, complexFactory));

    // Unlimited default quota (-1) returns Long.MAX_VALUE
    HttpRequestFactory unlimitedDefaultFactory =
        createFakeRequestFactory(200, "{\"quotaBuckets\": [{\"effectiveLimit\": \"-1\"}]}");
    assertEquals(
        Long.MAX_VALUE,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.emptySet(), unlimitedDefaultFactory));

    // 5. Empty quotaBuckets or non-positive defaultLimit falls back to 400
    HttpRequestFactory nonPositiveDefaultFactory =
        createFakeRequestFactory(200, "{\"quotaBuckets\": [{\"effectiveLimit\": \"0\"}]}");
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), nonPositiveDefaultFactory));
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.emptySet(), nonPositiveDefaultFactory));

    // 6. HTTP 403 error and malformed JSON fall back to 400 without throwing
    HttpRequestFactory forbiddenFactory =
        createFakeRequestFactory(403, "{\"error\": \"Forbidden\"}");
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), forbiddenFactory));

    HttpRequestFactory malformedJsonFactory = createFakeRequestFactory(200, "not-valid-json{{{");
    assertEquals(
        400L,
        VerifyDataBoostParallelism.getDataBoostQuota(
            "test-project", Collections.singleton("us-central1"), malformedJsonFactory));
  }

  @Test
  public void testResolveMaxDataBoostParallelismBranches() {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    ServiceFactory<Spanner, SpannerOptions> regionalServiceFactory =
        createMockServiceFactory("regional-us-central1", null);
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("regional-inst-quota")
            .withDatabaseId("test-db")
            .withCredentials(mock(GoogleCredentials.class))
            .withServiceFactory(regionalServiceFactory);

    VerifyDataBoostParallelism.HttpRequestFactorySupplier mockedQuotaSupplier =
        () -> createFakeRequestFactory(200, STANDARD_QUOTA_RESPONSE_JSON);
    VerifyDataBoostParallelism.HttpRequestFactorySupplier failingSupplier =
        () -> {
          throw new IOException("Simulated Quota API failure");
        };

    // 1. Positive user-configured maxDataBoostParallelism skips Quota API (even if supplier fails)
    VerifyDataBoostParallelism withPositiveParam =
        new VerifyDataBoostParallelism(spannerConfig, StaticValueProvider.of(750), failingSupplier);
    assertEquals(750L, withPositiveParam.resolveMaxDataBoostParallelism(options));

    // 2. Null, inaccessible, null-value, or non-positive maxDataBoostParallelism calls Quota API
    assertEquals(
        1000L,
        new VerifyDataBoostParallelism(spannerConfig, null, mockedQuotaSupplier)
            .resolveMaxDataBoostParallelism(options));
    assertEquals(
        1000L,
        new VerifyDataBoostParallelism(
                spannerConfig, new InaccessibleValueProvider<>(), mockedQuotaSupplier)
            .resolveMaxDataBoostParallelism(options));
    assertEquals(
        1000L,
        new VerifyDataBoostParallelism(
                spannerConfig, StaticValueProvider.of(null), mockedQuotaSupplier)
            .resolveMaxDataBoostParallelism(options));
    assertEquals(
        1000L,
        new VerifyDataBoostParallelism(
                spannerConfig, StaticValueProvider.of(0), mockedQuotaSupplier)
            .resolveMaxDataBoostParallelism(options));

    // 3. Exception thrown by supplier or ValueProvider is caught and defaults to 400
    assertEquals(
        400L,
        new VerifyDataBoostParallelism(spannerConfig, null, failingSupplier)
            .resolveMaxDataBoostParallelism(options));
    assertEquals(
        400L,
        new VerifyDataBoostParallelism(
                spannerConfig, new ThrowingValueProvider<>(), mockedQuotaSupplier)
            .resolveMaxDataBoostParallelism(options));
  }

  @Test
  public void testResolveProjectIdBranches() {
    String previousDefaultProject = System.getProperty("google.cloud.project");
    try {
      System.setProperty("google.cloud.project", "mock-default-project");

      DataflowPipelineOptions options =
          PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
      options.setProject("options-project-id");

      // 1. Project ID from SpannerConfig
      VerifyDataBoostParallelism fromSpannerConfig =
          new VerifyDataBoostParallelism(
              SpannerConfig.create().withProjectId("spanner-project-id"));
      assertEquals("spanner-project-id", fromSpannerConfig.resolveProjectId(options));

      // 2. SpannerConfig projectId is null, inaccessible, empty, or throws -> falls back to options
      assertEquals(
          "options-project-id",
          new VerifyDataBoostParallelism(SpannerConfig.create()).resolveProjectId(options));
      assertEquals(
          "options-project-id",
          new VerifyDataBoostParallelism(
                  SpannerConfig.create().withProjectId(new InaccessibleValueProvider<>()))
              .resolveProjectId(options));
      assertEquals(
          "options-project-id",
          new VerifyDataBoostParallelism(SpannerConfig.create().withProjectId(""))
              .resolveProjectId(options));
      assertEquals(
          "options-project-id",
          new VerifyDataBoostParallelism(
                  SpannerConfig.create().withProjectId(new ThrowingValueProvider<>()))
              .resolveProjectId(options));

      // 3. Both SpannerConfig and DataflowPipelineOptions project are empty or options is null ->
      // falls back to SpannerOptions.getDefaultProjectId()
      DataflowPipelineOptions emptyOptions =
          PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
      emptyOptions.setProject("");
      VerifyDataBoostParallelism fallbackTransform =
          new VerifyDataBoostParallelism(SpannerConfig.create());
      assertEquals(
          SpannerOptions.getDefaultProjectId(), fallbackTransform.resolveProjectId(emptyOptions));
      assertEquals(SpannerOptions.getDefaultProjectId(), fallbackTransform.resolveProjectId(null));
    } finally {
      if (previousDefaultProject == null) {
        System.clearProperty("google.cloud.project");
      } else {
        System.setProperty("google.cloud.project", previousDefaultProject);
      }
    }
  }

  @Test
  public void testResolveRegionsBranches() {
    GoogleCredentials mockCredentials = mock(GoogleCredentials.class);

    // 1. Null, inaccessible, or empty instanceId -> empty set
    assertTrue(new VerifyDataBoostParallelism(SpannerConfig.create()).resolveRegions().isEmpty());
    assertTrue(
        new VerifyDataBoostParallelism(
                SpannerConfig.create().withInstanceId(new InaccessibleValueProvider<>()))
            .resolveRegions()
            .isEmpty());
    assertTrue(
        new VerifyDataBoostParallelism(SpannerConfig.create().withInstanceId(""))
            .resolveRegions()
            .isEmpty());

    // 2. Regional instance config ("regional-us-west1") returns singleton ["us-west1"]
    ServiceFactory<Spanner, SpannerOptions> regionalServiceFactory =
        createMockServiceFactory("regional-us-west1", null);
    SpannerConfig regionalConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("regional-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(regionalServiceFactory);
    assertEquals(
        Collections.singleton("us-west1"),
        new VerifyDataBoostParallelism(regionalConfig).resolveRegions());

    // 3. Multi-region instance config ("nam3") returns non-witness replica regions
    ReplicaInfo rwReplica = mock(ReplicaInfo.class);
    when(rwReplica.getType()).thenReturn(ReplicaType.READ_WRITE);
    when(rwReplica.getLocation()).thenReturn("us-east4");

    ReplicaInfo roReplica = mock(ReplicaInfo.class);
    when(roReplica.getType()).thenReturn(ReplicaType.READ_ONLY);
    when(roReplica.getLocation()).thenReturn("us-central1");

    ReplicaInfo witnessReplica = mock(ReplicaInfo.class);
    when(witnessReplica.getType()).thenReturn(ReplicaType.WITNESS);
    when(witnessReplica.getLocation()).thenReturn("us-west2");

    ReplicaInfo emptyLocReplica = mock(ReplicaInfo.class);
    when(emptyLocReplica.getType()).thenReturn(ReplicaType.READ_WRITE);
    when(emptyLocReplica.getLocation()).thenReturn("");

    InstanceConfig nam3Config = mock(InstanceConfig.class);
    when(nam3Config.getReplicas())
        .thenReturn(Arrays.asList(rwReplica, roReplica, witnessReplica, emptyLocReplica, null));

    ServiceFactory<Spanner, SpannerOptions> multiRegionServiceFactory =
        createMockServiceFactory("nam3", nam3Config);
    SpannerConfig multiRegionConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("multiregion-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(multiRegionServiceFactory);
    assertEquals(
        ImmutableSet.of("us-east4", "us-central1"),
        new VerifyDataBoostParallelism(multiRegionConfig).resolveRegions());

    // 4. Multi-region instance config with null InstanceConfig or null replicas returns empty set
    ServiceFactory<Spanner, SpannerOptions> nullConfigFactory =
        createMockServiceFactory("nam6", null);
    SpannerConfig nullInstanceConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("null-config-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(nullConfigFactory);
    assertTrue(new VerifyDataBoostParallelism(nullInstanceConfig).resolveRegions().isEmpty());

    InstanceConfig nullReplicasInstanceConfig = mock(InstanceConfig.class);
    when(nullReplicasInstanceConfig.getReplicas()).thenReturn(null);
    ServiceFactory<Spanner, SpannerOptions> nullReplicasFactory =
        createMockServiceFactory("eur3", nullReplicasInstanceConfig);
    SpannerConfig nullReplicasConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("null-replicas-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(nullReplicasFactory);
    assertTrue(new VerifyDataBoostParallelism(nullReplicasConfig).resolveRegions().isEmpty());

    // 5. Unknown or empty instanceConfigId returns empty set
    ServiceFactory<Spanner, SpannerOptions> unknownConfigFactory =
        createMockServiceFactory("unknown", null);
    SpannerConfig unknownConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("unknown-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(unknownConfigFactory);
    assertTrue(new VerifyDataBoostParallelism(unknownConfig).resolveRegions().isEmpty());

    ServiceFactory<Spanner, SpannerOptions> emptyConfigIdFactory =
        createMockServiceFactory("", null);
    SpannerConfig emptyConfigId =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("empty-config-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(emptyConfigIdFactory);
    assertTrue(new VerifyDataBoostParallelism(emptyConfigId).resolveRegions().isEmpty());

    // 6. Exception when creating SpannerAccessor is caught and returns empty set
    @SuppressWarnings("unchecked")
    ServiceFactory<Spanner, SpannerOptions> throwingFactory = mock(ServiceFactory.class);
    when(throwingFactory.create(any())).thenThrow(new RuntimeException("Connection failed"));
    SpannerConfig failingConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withInstanceId("failing-inst")
            .withDatabaseId("test-db")
            .withCredentials(mockCredentials)
            .withServiceFactory(throwingFactory);
    assertTrue(new VerifyDataBoostParallelism(failingConfig).resolveRegions().isEmpty());
  }

  private static ServiceFactory<Spanner, SpannerOptions> createMockServiceFactory(
      String instanceConfigName, InstanceConfig instanceConfig) {
    @SuppressWarnings("unchecked")
    ServiceFactory<Spanner, SpannerOptions> serviceFactory = mock(ServiceFactory.class);
    Spanner spanner = mock(Spanner.class);
    InstanceAdminClient instanceAdminClient = mock(InstanceAdminClient.class);
    Instance instance = mock(Instance.class);
    when(serviceFactory.create(any())).thenReturn(spanner);
    when(spanner.getInstanceAdminClient()).thenReturn(instanceAdminClient);
    when(instanceAdminClient.getInstance(any())).thenReturn(instance);
    when(instance.getInstanceConfigId())
        .thenReturn(InstanceConfigId.of("test-proj", instanceConfigName));
    when(instanceAdminClient.getInstanceConfig(instanceConfigName)).thenReturn(instanceConfig);
    return serviceFactory;
  }

  @Test
  public void testPipelineExecutionWhenDataBoostDisabledOrInaccessible() {
    SpannerConfig nullDataBoostConfig = SpannerConfig.create();
    SpannerConfig inaccessibleDataBoostConfig =
        SpannerConfig.create().withDataBoostEnabled(new InaccessibleValueProvider<>());
    SpannerConfig falseDataBoostConfig =
        SpannerConfig.create().withDataBoostEnabled(StaticValueProvider.of(false));

    PCollection<Integer> out1 =
        pipeline.apply("NullDataBoost", new VerifyDataBoostParallelism(nullDataBoostConfig));
    PCollection<Integer> out2 =
        pipeline.apply(
            "InaccessibleDataBoost", new VerifyDataBoostParallelism(inaccessibleDataBoostConfig));
    PCollection<Integer> out3 =
        pipeline.apply("FalseDataBoost", new VerifyDataBoostParallelism(falseDataBoostConfig));

    PAssert.that(out1).containsInAnyOrder(1);
    PAssert.that(out2).containsInAnyOrder(1);
    PAssert.that(out3).containsInAnyOrder(1);
    pipeline.run();
  }

  @Test
  public void testPipelineExecutionWhenDataBoostEnabledWithinQuota() {
    pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).setMaxNumWorkers(10);
    pipeline
        .getOptions()
        .as(DataflowPipelineDebugOptions.class)
        .setNumberOfWorkerHarnessThreads(20);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test-proj")
            .withDataBoostEnabled(StaticValueProvider.of(true));

    // 10 * 20 = 200 <= 400 (fallback quota when mocked supplier throws)
    PCollection<Integer> result =
        pipeline.apply(
            new VerifyDataBoostParallelism(
                spannerConfig,
                null,
                () -> {
                  throw new IOException("Mocked API error");
                }));
    PAssert.that(result).containsInAnyOrder(1);
    pipeline.run();
  }

  @Test
  public void testPipelineExecutionSkipsValidationWhenMaxNumWorkersNotSpecified() {
    pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).setMaxNumWorkers(0);

    SpannerConfig spannerConfig =
        SpannerConfig.create().withDataBoostEnabled(StaticValueProvider.of(true));

    // Even with a tiny quota limit (1), validation is skipped when maxNumWorkers <= 0
    PCollection<Integer> result =
        pipeline.apply(new VerifyDataBoostParallelism(spannerConfig, StaticValueProvider.of(1)));
    PAssert.that(result).containsInAnyOrder(1);
    pipeline.run();
  }

  @Test
  public void testPipelineExecutionThrowsWhenExceedingQuota() {
    // Set maxNumWorkers=600 and leave numberOfWorkerHarnessThreads=0 (defaults to
    // availableProcessors() >= 1), so 600 * availableProcessors() > 500
    pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).setMaxNumWorkers(600);
    pipeline.getOptions().as(DataflowPipelineDebugOptions.class).setNumberOfWorkerHarnessThreads(0);

    SpannerConfig spannerConfig =
        SpannerConfig.create().withDataBoostEnabled(StaticValueProvider.of(true));

    pipeline.apply(new VerifyDataBoostParallelism(spannerConfig, StaticValueProvider.of(500)));

    PipelineExecutionException thrown =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run());
    assertThat(
        thrown.getMessage(),
        containsString("exceeds Spanner Data Boost quota (500). Reduce --maxNumWorkers"));
  }

  @Test
  public void testCreateDefaultRequestFactoryAndExportPipelineConstructors() {
    GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
    when(mockCredentials.createScoped(anyCollection())).thenReturn(mockCredentials);
    assertNotNull(VerifyDataBoostParallelism.createRequestFactory(mockCredentials));

    try {
      assertNotNull(VerifyDataBoostParallelism.createDefaultRequestFactory());
    } catch (IOException ignored) {
      // Expected in CI/CD environments without Application Default Credentials configured
    }

    ExportPipelineOptions options =
        PipelineOptionsFactory.fromArgs("--maxDataBoostParallelism=600")
            .withValidation()
            .as(ExportPipelineOptions.class);
    assertEquals(Integer.valueOf(600), options.getMaxDataBoostParallelism().get());

    SpannerConfig spannerConfig = SpannerConfig.create();
    ValueProvider<String> dir = StaticValueProvider.of("/tmp");
    ValueProvider<String> empty = StaticValueProvider.of("");
    ValueProvider<Boolean> boolFalse = StaticValueProvider.of(false);
    ValueProvider<ChecksumAlgorithm> md5 = StaticValueProvider.of(ChecksumAlgorithm.MD5);

    assertNotNull(new ExportTransform(spannerConfig, dir, empty));
    assertNotNull(
        new ExportTransform(spannerConfig, dir, empty, empty, empty, boolFalse, boolFalse, dir));
    ExportTransform exportTransform =
        new ExportTransform(
            spannerConfig,
            dir,
            empty,
            empty,
            empty,
            boolFalse,
            boolFalse,
            dir,
            md5,
            options.getMaxDataBoostParallelism());
    assertNotNull(exportTransform);

    org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create(options);
    assertNotNull(p.apply("Run Export", exportTransform));
  }
}
