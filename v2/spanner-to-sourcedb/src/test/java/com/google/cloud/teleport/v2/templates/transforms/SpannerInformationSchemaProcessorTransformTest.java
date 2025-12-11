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

import static com.google.cloud.teleport.v2.templates.transforms.SpannerInformationSchemaProcessorTransform.MAIN_DDL_TAG;
import static com.google.cloud.teleport.v2.templates.transforms.SpannerInformationSchemaProcessorTransform.SHADOW_TABLE_DDL_TAG;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link SpannerInformationSchemaProcessorTransform}. */
@RunWith(JUnit4.class)
public class SpannerInformationSchemaProcessorTransformTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule
  public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock
  private SpannerConfig spannerConfig;
  @Mock
  private SpannerConfig shadowTableSpannerConfig;

  @Test
  public void testExpand() {
    SpannerInformationSchemaProcessorTransform transform = new SpannerInformationSchemaProcessorTransform(
        spannerConfig, shadowTableSpannerConfig, "shadow_");

    PCollectionTuple output = pipeline.apply(transform);

    assertThat(output.has(MAIN_DDL_TAG), is(true));
    assertThat(output.has(SHADOW_TABLE_DDL_TAG), is(true));
  }
}
