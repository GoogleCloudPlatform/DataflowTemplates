/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.PCollection;

/** Combines all elements in the {@link PCollection} as a list. */
public class AsList {

  public static <T> Combine.CombineFn<T, List<T>, List<T>> fn() {
    return new Impl<>();
  }

  private static class Impl<T> extends Combine.CombineFn<T, List<T>, List<T>> {

    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = new ArrayList<>();
      for (List<T> acc : accumulators) {
        result.addAll(acc);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }
  }
}
