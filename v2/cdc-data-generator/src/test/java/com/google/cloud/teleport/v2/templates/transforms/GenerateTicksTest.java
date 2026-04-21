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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.dofn.ScaleTicksFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GenerateTicks} and its {@link ScaleTicksFn}. */
@RunWith(JUnit4.class)
public class GenerateTicksTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static CdcDataGeneratorOptions options() {
    return PipelineOptionsFactory.create().as(CdcDataGeneratorOptions.class);
  }

  private static DataGeneratorTable root(String name, int insertQps) {
    return DataGeneratorTable.builder()
        .name(name)
        .insertQps(insertQps)
        .updateQps(0)
        .deleteQps(0)
        .isRoot(true)
        .columns(ImmutableList.of())
        .primaryKeys(ImmutableList.of())
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .recordsPerTick(1.0)
        .build();
  }

  private static DataGeneratorTable child(String name, String parent, int insertQps) {
    return DataGeneratorTable.builder()
        .name(name)
        .insertQps(insertQps)
        .updateQps(0)
        .deleteQps(0)
        .isRoot(false)
        .interleavedInTable(parent)
        .columns(ImmutableList.of())
        .primaryKeys(ImmutableList.of())
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .recordsPerTick(1.0)
        .build();
  }

  // -------------------- resolveBaseTickRate --------------------

  @Test
  public void testResolveBaseTickRate_usesBaseTickRateWhenSet() {
    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(500);
    opts.setInsertQps(1234);
    assertEquals(500, GenerateTicks.resolveBaseTickRate(opts));
  }

  @Test
  public void testResolveBaseTickRate_fallsBackToInsertQps() {
    CdcDataGeneratorOptions opts = options();
    opts.setInsertQps(750);
    assertEquals(750, GenerateTicks.resolveBaseTickRate(opts));
  }

  @Test
  public void testResolveBaseTickRate_defaultsToConstantWhenBothNull() {
    CdcDataGeneratorOptions opts = options();
    // baseTickRate and insertQps are both @Nullable and unset.
    assertEquals(GenerateTicks.DEFAULT_BASE_TICK_RATE, GenerateTicks.resolveBaseTickRate(opts));
  }

  @Test
  public void testResolveBaseTickRate_rejectsNonPositive() {
    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(0);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateTicks.resolveBaseTickRate(opts));
    assertEquals(true, ex.getMessage().contains("baseTickRate must be > 0"));
  }

  @Test
  public void testResolveBaseTickRate_rejectsNegativeInsertQpsFallback() {
    CdcDataGeneratorOptions opts = options();
    opts.setInsertQps(-5);
    assertThrows(IllegalArgumentException.class, () -> GenerateTicks.resolveBaseTickRate(opts));
  }

  // -------------------- totalRootQps --------------------

  @Test
  public void testTotalRootQps_sumsOnlyRootTables() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "A", root("A", 10),
                    "B", child("B", "A", 100),
                    "C", root("C", 50)))
            .build();
    // Only A (10) + C (50) are roots; child B's QPS does not contribute to the root-tick rate.
    assertEquals(60, ScaleTicksFn.totalRootQps(schema));
  }

  @Test
  public void testTotalRootQps_emptySchemaIsZero() {
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();
    assertEquals(0, ScaleTicksFn.totalRootQps(schema));
  }

  @Test
  public void testTotalRootQps_allNonRoot() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("B", child("B", "A", 100)))
            .build();
    assertEquals(0, ScaleTicksFn.totalRootQps(schema));
  }

  // -------------------- End-to-end pipeline behaviour --------------------

  @Test
  public void testScaleTicks_zeroTotalQpsEmitsNothing() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("B", child("B", "A", 42))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(100);

    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(50)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    PAssert.that(output).empty();
    pipeline.run();
  }

  @Test
  public void testScaleTicks_totalQpsEqualsBaseRateEmitsSameCount() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 100))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(100);

    int inputCount = 20;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    // multiplier = 1, remainder = 0 → output = input (no probabilistic branch taken).
    PAssert.thatSingleton(output.apply("Count", org.apache.beam.sdk.transforms.Count.globally()))
        .isEqualTo((long) inputCount);
    pipeline.run();
  }

  @Test
  public void testScaleTicks_totalQpsExactMultipleOfBaseRate() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 300))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(100);

    int inputCount = 10;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    // multiplier = 3, remainder = 0 → output = 3 * input, deterministically.
    PAssert.thatSingleton(output.apply("Count", org.apache.beam.sdk.transforms.Count.globally()))
        .isEqualTo((long) inputCount * 3);
    pipeline.run();
  }

  @Test
  public void testScaleTicks_totalQpsLessThanBaseRateStaysBelow() {
    // Probabilistic path — assert bounds instead of an exact count. With totalQps=10,
    // baseTickRate=100, each input is kept with probability 0.10, so over 500 inputs the
    // expected output is ~50 but the worst-case upper bound is still 500 (if every coin flip
    // somehow came up "keep"). Asserting <= input size is a deterministic check.
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 10))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(100);

    int inputCount = 500;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    PAssert.that(output)
        .satisfies(
            iter -> {
              int count = 0;
              for (Long ignored : iter) {
                count++;
              }
              if (count > inputCount) {
                throw new AssertionError(
                    "Probabilistic filter must not emit more than input; got "
                        + count
                        + " for "
                        + inputCount);
              }
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testScaleTicks_scaleUpWithRemainder_emitsAtLeastMultiplierCopies() {
    // totalQps=250, baseTickRate=100 → multiplier=2, remainder=50 (p=0.5 bonus output).
    // The deterministic part alone gives 2 * input copies; the probabilistic part adds up to
    // input more. Assert the output count is in the deterministic lower bound [2N, 3N].
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 250))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setBaseTickRate(100);

    final int inputCount = 40;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    PAssert.that(output)
        .satisfies(
            iter -> {
              int count = 0;
              for (Long ignored : iter) {
                count++;
              }
              int lowerBound = 2 * inputCount;
              int upperBound = 3 * inputCount;
              if (count < lowerBound || count > upperBound) {
                throw new AssertionError(
                    "Expected output in ["
                        + lowerBound
                        + ", "
                        + upperBound
                        + "] but got "
                        + count);
              }
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testScaleTicks_fallbackToInsertQps() {
    // baseTickRate unset → resolveBaseTickRate falls through to insertQps (20).
    // totalRootQps=20 → multiplier=1, remainder=0 → output count == input count.
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 20))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    CdcDataGeneratorOptions opts = options();
    opts.setInsertQps(20);

    int inputCount = 15;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(opts, schemaView));

    PAssert.thatSingleton(output.apply("Count", org.apache.beam.sdk.transforms.Count.globally()))
        .isEqualTo((long) inputCount);
    pipeline.run();
  }

  // -------------------- Helpers --------------------

  private static List<Long> buildSequence(int count) {
    if (count <= 0) {
      return Collections.emptyList();
    }
    List<Long> out = new ArrayList<>(count);
    for (long i = 0; i < count; i++) {
      out.add(i);
    }
    return out;
  }
}
