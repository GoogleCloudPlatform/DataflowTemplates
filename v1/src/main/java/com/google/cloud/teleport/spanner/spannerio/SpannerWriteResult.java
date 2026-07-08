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
package com.google.cloud.teleport.spanner.spannerio;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * The results of a {@link SpannerIO#write()} transform.
 *
 * <p>Use {@link #getFailedMutations()} to access a {@link PCollection} of MutationGroups that
 * failed to write.
 *
 * <p>The {@link PCollection} returned by {@link #getOutput()} can be used in batch pipelines as a
 * completion signal to {@link org.apache.beam.sdk.transforms.Wait Wait.OnSignal} to indicate when
 * all input has been written. Note that in streaming pipelines, this signal will never be triggered
 * as the input is unbounded and this {@link PCollection} is using the {@link
 * org.apache.beam.sdk.transforms.windowing.GlobalWindow GlobalWindow}.
 *
 * <p>WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
public class SpannerWriteResult implements POutput {
  private final Pipeline pipeline;
  private final PCollection<Void> output;
  private final PCollection<com.google.cloud.teleport.spanner.spannerio.MutationGroup>
      failedMutations;
  private final TupleTag<com.google.cloud.teleport.spanner.spannerio.MutationGroup>
      failedMutationsTag;

  public SpannerWriteResult(
      Pipeline pipeline,
      PCollection<Void> output,
      PCollection<com.google.cloud.teleport.spanner.spannerio.MutationGroup> failedMutations,
      TupleTag<com.google.cloud.teleport.spanner.spannerio.MutationGroup> failedMutationsTag) {
    this.pipeline = pipeline;
    this.output = output;
    this.failedMutations = failedMutations;
    this.failedMutationsTag = failedMutationsTag;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(failedMutationsTag, failedMutations);
  }

  public PCollection<MutationGroup> getFailedMutations() {
    return failedMutations;
  }

  public PCollection<Void> getOutput() {
    return output;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
