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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

/** Batches individual rows (Structs) into groups of the given size. */
@AutoValue
public abstract class MakeBatchesTransform
    extends PTransform<PCollection<Struct>, PCollection<Iterable<Struct>>> {

  public static MakeBatchesTransform create(Integer batchSize) {
    return new AutoValue_MakeBatchesTransform(batchSize);
  }

  abstract Integer batchSize();

  @Override
  public PCollection<Iterable<Struct>> expand(PCollection<Struct> input) {
    return input
        .apply("CreateArbitraryBatchKey", WithKeys.of(1))
        .apply("GroupRowsIntoBatches", GroupIntoBatches.ofSize(batchSize()))
        .apply("RemoveArbitraryBatchKey", Values.create());
  }
}
