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

import com.google.cloud.teleport.v2.templates.dofn.ScaleTicksFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @Test
  public void testResolveBaseTickRate_usesInsertQps() {
    assertEquals(500, GenerateTicks.resolveBaseTickRate(500));
  }

  @Test
  public void testResolveBaseTickRate_defaultsToConstantWhenZeroOrNegative() {
    assertEquals(GenerateTicks.DEFAULT_BASE_TICK_RATE, GenerateTicks.resolveBaseTickRate(0));
    assertEquals(GenerateTicks.DEFAULT_BASE_TICK_RATE, GenerateTicks.resolveBaseTickRate(-5));
  }

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
        DataGeneratorSchema.builder().tables(ImmutableMap.of("B", child("B", "A", 100))).build();
    assertEquals(0, ScaleTicksFn.totalRootQps(schema));
  }

  @Test
  public void testScaleTicks_zeroTotalQpsEmitsNothing() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("B", child("B", "A", 42))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(50)))
            .apply("GenerateTicks", new GenerateTicks(100, schemaView));

    PAssert.that(output).empty();
    pipeline.run();
  }

  @Test
  public void testScaleTicks_totalQpsEqualsBaseRateEmitsSameCount() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 100))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    int inputCount = 100;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(100, schemaView));

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

    int inputCount = 100;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(100, schemaView));

    PAssert.thatSingleton(output.apply("Count", org.apache.beam.sdk.transforms.Count.globally()))
        .isEqualTo((long) inputCount * 3);
    pipeline.run();
  }

  @Test
  public void testScaleTicks_totalQpsLessThanBaseRateStaysBelow() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 10))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    int inputCount = 100;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(100, schemaView));

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
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", root("A", 250))).build();
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    final int inputCount = 100;
    PCollection<Long> output =
        pipeline
            .apply("Input", Create.of(buildSequence(inputCount)))
            .apply("GenerateTicks", new GenerateTicks(100, schemaView));

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
                    "Expected output in [" + lowerBound + ", " + upperBound + "] but got " + count);
              }
              return null;
            });
    pipeline.run();
  }

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
