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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.spannerio.SpannerConfig;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ApplyDDLTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static DatabaseAdminClient mockDatabaseAdminClient;
  private static Spanner mockSpanner;
  private static FakeServiceFactory serviceFactory;

  public static class FakeServiceFactory
      implements ServiceFactory<Spanner, SpannerOptions>, Serializable {
    @Override
    public Spanner create(SpannerOptions serviceOptions) {
      return mockSpanner;
    }
  }

  @Before
  public void setUp() {
    mockDatabaseAdminClient = mock(DatabaseAdminClient.class, withSettings().serializable());
    mockSpanner = mock(Spanner.class, withSettings().serializable());
    when(mockSpanner.getDatabaseAdminClient()).thenReturn(mockDatabaseAdminClient);
    serviceFactory = new FakeServiceFactory();
  }

  @Test
  public void testApplyDDLBatched() {
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory);

    List<String> statements =
        new java.util.ArrayList<>(Arrays.asList("CREATE TABLE t1", "CREATE TABLE t2"));
    PCollectionView<List<String>> statementsView =
        pipeline.apply("Create statements", Create.of(statements)).apply(View.asList());

    OperationFuture mockFuture = mock(OperationFuture.class, withSettings().serializable());
    when(mockDatabaseAdminClient.updateDatabaseDdl(
            eq("test-instance"), eq("test-database"), eq(statements), any()))
        .thenReturn(mockFuture);

    Ddl ddl = Ddl.builder().build();

    pipeline
        .apply("Create DDL", Create.of(ddl))
        .apply(
            "Apply DDL",
            new ApplyDDLTransform(
                spannerConfig,
                statementsView,
                StaticValueProvider.of(false),
                StaticValueProvider.of(false),
                "table"));

    pipeline.run();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<String>> statementsCaptor = ArgumentCaptor.forClass(Iterable.class);

    verify(mockDatabaseAdminClient, times(1))
        .updateDatabaseDdl(
            eq("test-instance"), eq("test-database"), statementsCaptor.capture(), isNull());

    List<String> actualStatements = new java.util.ArrayList<>();
    statementsCaptor.getValue().forEach(actualStatements::add);
    java.util.Collections.sort(statements);
    java.util.Collections.sort(actualStatements);
    assertEquals(statements, actualStatements);
  }

  @Test
  public void testApplyDDLParallel() {
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory);

    List<String> statements = Arrays.asList("CREATE INDEX i1", "CREATE INDEX i2");
    PCollectionView<List<String>> statementsView =
        pipeline.apply("Create statements", Create.of(statements)).apply(View.asList());

    OperationFuture mockFuture = mock(OperationFuture.class, withSettings().serializable());
    when(mockDatabaseAdminClient.updateDatabaseDdl(any(), any(), any(), any()))
        .thenReturn(mockFuture);

    Ddl ddl = Ddl.builder().build();

    pipeline
        .apply("Create DDL", Create.of(ddl))
        .apply(
            "Apply DDL",
            new ApplyDDLTransform(
                spannerConfig,
                statementsView,
                StaticValueProvider.of(false),
                StaticValueProvider.of(true),
                "index"));

    pipeline.run();

    verify(mockDatabaseAdminClient, times(1))
        .updateDatabaseDdl(
            eq("test-instance"), eq("test-database"), eq(Arrays.asList("CREATE INDEX i1")), any());
    verify(mockDatabaseAdminClient, times(1))
        .updateDatabaseDdl(
            eq("test-instance"), eq("test-database"), eq(Arrays.asList("CREATE INDEX i2")), any());
  }
}
