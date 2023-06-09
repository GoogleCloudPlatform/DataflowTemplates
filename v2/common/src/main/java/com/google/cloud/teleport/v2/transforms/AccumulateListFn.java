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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * {@code AccumulateListFn<InputT, OutputListT>} specifies how to combine a collection of input
 * values of type {@code InputT} to a single list of values of type {@code OutputListT}.
 *
 * <p>If no conversion between types is required, i.e. you want to accumulate {@code String}s into
 * {@code List<String>}, use {@link AccumulateListFn#createSimple()}.
 *
 * <p>Example usage:
 *
 * <pre>
 *   // Combines all values in a collection of KV&lt;Void, String&gt;
 *   // into a list of strings (single element).
 *   PCollection&lt;KV&lt;Void, String&gt;&gt; p1 = ...;
 *   PCollection&lt;List&lt;String&gt;&gt; p2 = p1.apply(
 *       Combine.globally(
 *           new AccumulateListFn&lt;KV&lt;Void, String&gt;, String&gt;() {
 *             protected String convertInput(KV&lt;Void, String&gt; input) {
 *               return input.getValue();
 *           }
 *       }));
 * </pre>
 */
public abstract class AccumulateListFn<InputT, OutputListT>
    extends CombineFn<InputT, List<OutputListT>, List<OutputListT>> {

  public static <T> AccumulateListFn<T, T> createSimple() {
    return new AccumulateListFn<T, T>() {
      @Override
      protected T convertInput(T input) {
        return input;
      }
    };
  }

  protected abstract OutputListT convertInput(InputT input);

  @Override
  public List<OutputListT> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<OutputListT> addInput(List<OutputListT> mutableAccumulator, InputT input) {
    if (input != null) {
      mutableAccumulator.add(convertInput(input));
    }
    return mutableAccumulator;
  }

  @Override
  public List<OutputListT> mergeAccumulators(Iterable<List<OutputListT>> accumulators) {
    List<OutputListT> result = new ArrayList<>();
    accumulators.forEach(result::addAll);
    return result;
  }

  @Override
  public List<OutputListT> extractOutput(List<OutputListT> accumulator) {
    return accumulator;
  }
}
