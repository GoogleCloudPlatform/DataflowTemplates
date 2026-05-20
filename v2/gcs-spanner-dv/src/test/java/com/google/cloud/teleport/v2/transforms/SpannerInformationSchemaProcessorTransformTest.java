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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

public class SpannerInformationSchemaProcessorTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExpand() {
    // 1. Setup Mock Ddl
    Ddl mockDdl = mock(Ddl.class);

    // 2. Create Transform with overridden readInformationSchema
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerInformationSchemaProcessorTransform transform =
        new SpannerInformationSchemaProcessorTransform(spannerConfig) {
          @Override
          protected PTransform<@NotNull PCollection<? extends Void>, @NotNull PCollection<Ddl>>
              readInformationSchema() {
            return new PTransform<>() {
              @Override
              public @NotNull PCollection<Ddl> expand(@NotNull PCollection<? extends Void> input) {
                return input.getPipeline().apply("MockRead", Create.of(mockDdl));
              }
            };
          }
        };

    // 3. Run Pipeline
    PCollectionView<Ddl> outputView = pipeline.apply("Read Schema", transform);

    // 4. Verify - We can't directly verify PCollectionView content easily without
    // using it.
    // So we apply a transform that uses the view and asserts on the output.
    PCollection<Ddl> result =
        pipeline
            .apply("CreateImpulse", Create.of("impulse"))
            .apply(
                "ExtractView",
                new PTransform<>() {
                  @Override
                  public @NotNull PCollection<Ddl> expand(@NotNull PCollection<String> input) {
                    return input.apply(
                        "ParDo",
                        org.apache.beam.sdk.transforms.ParDo.of(
                                new org.apache.beam.sdk.transforms.DoFn<String, Ddl>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) {
                                    c.output(c.sideInput(outputView));
                                  }
                                })
                            .withSideInputs(outputView));
                  }
                });

    PAssert.that(result).containsInAnyOrder(mockDdl);

    pipeline.run();
  }

  @Test
  public void testOriginalReadInformationSchema() {
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerInformationSchemaProcessorTransform transform =
        new SpannerInformationSchemaProcessorTransform(spannerConfig);

    assertNotNull(transform.readInformationSchema());
  }
}
