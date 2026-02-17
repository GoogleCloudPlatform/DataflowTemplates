/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link ProcessInformationSchemaFn}. */
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class ProcessInformationSchemaFnTest {

  @Mock private SpannerConfig spannerConfig;
  @Mock private SpannerConfig shadowTableSpannerConfig;
  @Mock private SpannerAccessor spannerAccessor;
  @Mock private SpannerAccessor shadowTableSpannerAccessor;
  @Mock private DatabaseAdminClient databaseAdminClient;
  @Mock private DatabaseAdminClient shadowDatabaseAdminClient;
  @Mock private Database database;
  @Mock private Database shadowDatabase;
  @Mock private BatchClient batchClient;
  @Mock private BatchReadOnlyTransaction batchReadOnlyTransaction;
  @Mock private DoFn<Void, Ddl>.ProcessContext processContext;
  @Mock private Ddl ddl;

  private MockedStatic<SpannerAccessor> mockedSpannerAccessor;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    mockedSpannerAccessor = mockStatic(SpannerAccessor.class);
    mockedSpannerAccessor
        .when(() -> SpannerAccessor.getOrCreate(spannerConfig))
        .thenReturn(spannerAccessor);
    mockedSpannerAccessor
        .when(() -> SpannerAccessor.getOrCreate(shadowTableSpannerConfig))
        .thenReturn(shadowTableSpannerAccessor);

    when(spannerConfig.getInstanceId()).thenReturn(StaticValueProvider.of("instance"));
    when(spannerConfig.getDatabaseId()).thenReturn(StaticValueProvider.of("database"));
    when(shadowTableSpannerConfig.getInstanceId())
        .thenReturn(StaticValueProvider.of("shadow-instance"));
    when(shadowTableSpannerConfig.getDatabaseId())
        .thenReturn(StaticValueProvider.of("shadow-database"));

    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(shadowTableSpannerAccessor.getDatabaseAdminClient()).thenReturn(shadowDatabaseAdminClient);

    when(databaseAdminClient.getDatabase("instance", "database")).thenReturn(database);
    when(shadowDatabaseAdminClient.getDatabase("shadow-instance", "shadow-database"))
        .thenReturn(shadowDatabase);

    when(database.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(shadowDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);

    when(spannerAccessor.getBatchClient()).thenReturn(batchClient);
    when(shadowTableSpannerAccessor.getBatchClient()).thenReturn(batchClient);
    when(batchClient.batchReadOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(batchReadOnlyTransaction);
  }

  @After
  public void tearDown() {
    mockedSpannerAccessor.close();
  }

  @Test
  public void testProcessElement() throws Exception {
    ProcessInformationSchemaFn fn =
        new ProcessInformationSchemaFn(spannerConfig, shadowTableSpannerConfig, "shadow_");

    fn.setup();

    try (MockedConstruction<ShadowTableCreator> mockedShadowTableCreator =
            mockConstruction(
                ShadowTableCreator.class,
                (mock, context) -> {
                  doNothing().when(mock).createShadowTablesInSpanner();
                });
        MockedConstruction<InformationSchemaScanner> mockedScanner =
            mockConstruction(
                InformationSchemaScanner.class,
                (mock, context) -> {
                  when(mock.scan()).thenReturn(ddl);
                })) {

      fn.processElement(processContext);

      verify(processContext).output(SpannerInformationSchemaProcessorTransform.MAIN_DDL_TAG, ddl);
      verify(processContext)
          .output(SpannerInformationSchemaProcessorTransform.SHADOW_TABLE_DDL_TAG, ddl);

      assert (mockedShadowTableCreator.constructed().size() == 1);
      verify(mockedShadowTableCreator.constructed().get(0)).createShadowTablesInSpanner();

      assert (mockedScanner.constructed().size() == 2); // One for main, one for shadow
      verify(mockedScanner.constructed().get(0)).scan();
      verify(mockedScanner.constructed().get(1)).scan();
    }

    fn.teardown();
    verify(spannerAccessor).close();
    verify(shadowTableSpannerAccessor).close();
  }
}
