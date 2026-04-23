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

import com.google.cloud.teleport.v2.templates.dofn.ScaleTicksFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * {@link PTransform} that takes a fixed-rate baseline tick stream and rescales it dynamically to
 * match the total root-table QPS declared by the schema side input.
 *
 * <p>The driver-side baseline (driven by {@code GenerateSequence.withRate(...)}) must be fixed at
 * pipeline construction time because Beam's source rate is a compile-time constant. The schema,
 * however, is read via a side input to avoid the Dataflow template-launcher construction timeout on
 * large (~5000-table) schemas. This transform performs the rate correction at runtime, so the
 * effective emission rate converges to the schema's total root QPS without requiring the schema to
 * be known at construction time.
 *
 * <p>See {@link ScaleTicksFn} for the scaling logic.
 */
public class GenerateTicks extends PTransform<PCollection<Long>, PCollection<Long>> {

  @VisibleForTesting static final int DEFAULT_BASE_TICK_RATE = 1000;

  private final int insertQps;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(int insertQps, PCollectionView<DataGeneratorSchema> schemaView) {
    this.insertQps = insertQps;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<Long> input) {
    int baseTickRate = resolveBaseTickRate(insertQps);
    return input.apply(
        "ScaleTicks",
        ParDo.of(new ScaleTicksFn(schemaView, baseTickRate)).withSideInputs(schemaView));
  }

  /**
   * Chooses the baseline tick rate based on insertQps.
   *
   * <p>Priority: {@code insertQps} → {@link #DEFAULT_BASE_TICK_RATE}.
   */
  @VisibleForTesting
  public static int resolveBaseTickRate(int insertQps) {
    return insertQps > 0 ? insertQps : DEFAULT_BASE_TICK_RATE;
  }
}
