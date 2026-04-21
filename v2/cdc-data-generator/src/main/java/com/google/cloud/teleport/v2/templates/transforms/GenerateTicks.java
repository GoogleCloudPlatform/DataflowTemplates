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

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions;
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
 * <p>The driver-side baseline (driven by {@code GenerateSequence.withRate(...)}) must be fixed
 * at pipeline construction time because Beam's source rate is a compile-time constant. The
 * schema, however, is read via a side input to avoid the Dataflow template-launcher
 * construction timeout on large (~5000-table) schemas. This transform performs the rate
 * correction at runtime, so the effective emission rate converges to the schema's total root
 * QPS without requiring the schema to be known at construction time.
 *
 * <p>See {@link ScaleTicksFn} for the scaling logic.
 */
public class GenerateTicks extends PTransform<PCollection<Long>, PCollection<Long>> {

  /** Last-resort default for the baseline tick rate. See {@link #resolveBaseTickRate}. */
  @VisibleForTesting static final int DEFAULT_BASE_TICK_RATE = 1000;

  private final CdcDataGeneratorOptions options;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(
      CdcDataGeneratorOptions options, PCollectionView<DataGeneratorSchema> schemaView) {
    this.options = options;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<Long> input) {
    int baseTickRate = resolveBaseTickRate(options);
    return input.apply(
        "ScaleTicks",
        ParDo.of(new ScaleTicksFn(schemaView, baseTickRate)).withSideInputs(schemaView));
  }

  /**
   * Chooses the baseline tick rate with the same priority the driver uses, so rate
   * construction stays in sync across call sites.
   *
   * <p>Priority: {@code --baseTickRate} → {@code --insertQps} → {@link #DEFAULT_BASE_TICK_RATE}.
   *
   * @throws IllegalArgumentException if the resolved rate is non-positive. A non-positive rate
   *     is never meaningful for {@code GenerateSequence.withRate(...)} and will silently starve
   *     the pipeline if left to divide-by-zero at runtime.
   */
  @VisibleForTesting
  public static int resolveBaseTickRate(CdcDataGeneratorOptions options) {
    int rate =
        options.getBaseTickRate() != null
            ? options.getBaseTickRate()
            : (options.getInsertQps() != null ? options.getInsertQps() : DEFAULT_BASE_TICK_RATE);
    if (rate <= 0) {
      throw new IllegalArgumentException(
          "baseTickRate must be > 0; got " + rate + ". Set --baseTickRate or --insertQps.");
    }
    return rate;
  }
}
