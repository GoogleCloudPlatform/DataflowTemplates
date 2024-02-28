/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import java.util.ArrayList;
import java.util.List;

class Partitioner {

  public static <T> Iterable<Iterable<T>> partition(Iterable<T> elements, int size) {
    List<Iterable<T>> batches = new ArrayList<>();
    List<T> currentBatch = new ArrayList<>(size);
    for (T element : elements) {
      currentBatch.add(element);
      if (currentBatch.size() == size) {
        batches.add(currentBatch);
        currentBatch = new ArrayList<>(size);
      }
    }
    if (!currentBatch.isEmpty()) {
      batches.add(currentBatch);
    }
    return batches;
  }
}
