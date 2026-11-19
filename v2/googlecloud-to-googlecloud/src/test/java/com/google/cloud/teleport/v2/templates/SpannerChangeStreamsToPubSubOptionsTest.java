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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.spanner.SpannerTestHelper;
import com.google.spanner.v1.DirectedReadOptions;
import com.google.spanner.v1.DirectedReadOptions.IncludeReplicas;
import com.google.spanner.v1.DirectedReadOptions.ReplicaSelection;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for the {@link SpannerChangeStreamsToPubSub} pipeline options.
 *
 * <p>These tests are kept separate from {@link SpannerChangeStreamsToPubSubTest} because that class
 * declares a {@code TestPubsubSignal} rule, which creates real Pub/Sub topics before every test and
 * therefore requires a GCP project to run.
 */
@RunWith(JUnit4.class)
public final class SpannerChangeStreamsToPubSubOptionsTest extends SpannerTestHelper {

  private static final String TEST_LABEL = "cs2pubsub";
  private static final String DIRECTED_READ_OPTIONS_JSON =
      "{\"includeReplicas\":{\"replicaSelections\":[{\"location\":\"us-central1\",\"type\":\"READ_ONLY\"}]}}";
  private static final DirectedReadOptions EXPECTED_DIRECTED_READ_OPTIONS =
      DirectedReadOptions.newBuilder()
          .setIncludeReplicas(
              IncludeReplicas.newBuilder()
                  .addReplicaSelections(
                      ReplicaSelection.newBuilder()
                          .setLocation("us-central1")
                          .setType(ReplicaSelection.Type.READ_ONLY)
                          .build())
                  .build())
          .build();

  public SpannerChangeStreamsToPubSubOptionsTest() {
    super(TEST_LABEL);
  }

  @Test
  public void testSpannerDirectedReadOptions() {
    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setSpannerDirectedReadOptions(DIRECTED_READ_OPTIONS_JSON);

    // The template forwards the raw option value to SpannerIO.readChangeStream(), which parses it
    // onto the SpannerConfig that is used to read the change stream.
    SpannerConfig spannerConfig =
        getFakeSpannerConfig().withDirectedReadOptions(options.getSpannerDirectedReadOptions());

    assertEquals(EXPECTED_DIRECTED_READ_OPTIONS, spannerConfig.getDirectedReadOptions().get());
  }

  @Test
  public void testSpannerDirectedReadOptionsNotSet() {
    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);

    // The option is optional, so an unset value must leave the SpannerConfig untouched.
    SpannerConfig spannerConfig =
        getFakeSpannerConfig().withDirectedReadOptions(options.getSpannerDirectedReadOptions());

    assertNull(spannerConfig.getDirectedReadOptions());
  }

  @Test
  public void testInvalidSpannerDirectedReadOptions() {
    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setSpannerDirectedReadOptions("this-is-not-valid-json");

    assertThrows(
        RuntimeException.class,
        () ->
            getFakeSpannerConfig()
                .withDirectedReadOptions(options.getSpannerDirectedReadOptions()));
  }
}
