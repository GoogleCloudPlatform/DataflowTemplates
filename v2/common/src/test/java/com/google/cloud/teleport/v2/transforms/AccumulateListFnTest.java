/*
 * Copyright (C) 2021 Google LLC
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link AccumulateListFn}. */
@RunWith(JUnit4.class)
public class AccumulateListFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testWithCustomConverter() {
    List<KV<String, Integer>> input = Arrays.asList(KV.of("k1", 1), KV.of("k2", 2), KV.of("k3", 3));

    PCollection<List<Integer>> results =
        pipeline
            .apply("CreateInput", Create.of(input))
            .apply("CombineValuesIntoList", Combine.globally(new AccumulateValuesFn()));

    PAssert.that(results)
        .satisfies(
            collection -> {
              List<Integer> list = collection.iterator().next();
              assertThat(list, containsInAnyOrder(3, 2, 1));
              return null;
            });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWithNoConverter() {
    List<Integer> input = Arrays.asList(1, 2, 3);

    PCollection<List<Integer>> results =
        pipeline
            .apply("CreateInput", Create.of(input))
            .apply("CombineValuesIntoList", Combine.globally(AccumulateValuesFn.createSimple()));

    PAssert.that(results)
        .satisfies(
            collection -> {
              List<Integer> list = collection.iterator().next();
              assertThat(list, containsInAnyOrder(3, 2, 1));
              return null;
            });

    pipeline.run();
  }

  private static class AccumulateValuesFn extends AccumulateListFn<KV<String, Integer>, Integer> {
    @Override
    protected Integer convertInput(KV<String, Integer> input) {
      return input.getValue();
    }
  }
}
