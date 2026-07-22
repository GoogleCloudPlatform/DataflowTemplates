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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceWriterTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private SourceSchema mockSourceSchema;

  private List<Shard> shards;

  @Before
  public void setUp() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shards = Collections.singletonList(shard);
  }

  @Test
  public void testSourceWriterTransform_EmptyInput() {
    try (MockedStatic<SpannerAccessor> mockedSpannerAccessor =
        Mockito.mockStatic(SpannerAccessor.class)) {
      SpannerAccessor mockAccessor = mock(SpannerAccessor.class);
      mockedSpannerAccessor.when(() -> SpannerAccessor.getOrCreate(any())).thenReturn(mockAccessor);

      Ddl dummyDdl =
          Ddl.builder()
              .createTable("shadow_T")
              .column("c1")
              .string()
              .endColumn()
              .endTable()
              .build();
      PCollectionView<Ddl> ddlView =
          pipeline.apply("Create Ddl", Create.of(dummyDdl)).apply("View Ddl", View.asSingleton());
      PCollectionView<Ddl> shadowTableDdlView =
          pipeline
              .apply("Create Shadow Ddl", Create.of(dummyDdl))
              .apply("View Shadow Ddl", View.asSingleton());

      PCollection<KV<String, TrimmedShardedDataChangeRecord>> input =
          pipeline.apply(
              Create.empty(new TypeDescriptor<KV<String, TrimmedShardedDataChangeRecord>>() {}));

      SourceWriterTransform transform =
          new SourceWriterTransform(
              shards,
              mockSpannerConfig,
              "+00:00",
              ddlView,
              shadowTableDdlView,
              mockSourceSchema,
              "shadow_",
              "skip",
              10,
              "mysql",
              null,
              "",
              "",
              "",
              "");

      SourceWriterTransform.Result result = input.apply(transform);

      pipeline.run();
    }
  }
}
