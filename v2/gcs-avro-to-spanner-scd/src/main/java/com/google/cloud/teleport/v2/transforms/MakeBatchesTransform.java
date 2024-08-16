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
import com.google.cloud.teleport.v2.utils.StructHelper;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Batches individual rows (Structs) into groups of the given size. */
@AutoValue
public abstract class MakeBatchesTransform
    extends PTransform<PCollection<Struct>, PCollection<Iterable<Struct>>> {

  public static MakeBatchesTransform create(
      Integer batchSize, Iterable<String> primaryKeyColumns, String endDateColumnName) {
    return new AutoValue_MakeBatchesTransform(batchSize, primaryKeyColumns, endDateColumnName);
  }

  abstract Integer batchSize();

  abstract Iterable<String> primaryKeyColumns();

  @Nullable
  abstract String endDateColumnName();

  @Override
  public PCollection<Iterable<Struct>> expand(PCollection<Struct> input) {
    return input
        .apply("AddPrimaryKey", WithKeys.of(new ExtractPrimaryKey()))
        .apply("GroupByPrimaryKey", GroupByKey.create())
        .apply("GroupArbitrarilyForBatchSize", WithKeys.of(1))
        .apply("GroupIntoBatchesOfPrimaryKey", GroupIntoBatches.ofSize(batchSize()))
        .apply("RemoveArbitraryBatchKey", Values.create())
        .apply("RemovePrimaryKeyFromBatch", ParDo.of(new UngroupPrimaryKey()));
  }

  private class ExtractPrimaryKey implements SerializableFunction<Struct, String> {
    @Override
    public String apply(Struct record) {
      // Cannot use Key directly as order is non-deterministic.
      return StructHelper.of(record)
          .keyMaker(
              StreamSupport.stream(primaryKeyColumns().spliterator(), false)
                  .filter(
                      columnName ->
                          endDateColumnName() == null || columnName != endDateColumnName())
                  .collect(Collectors.toList()))
          .createKeyString();
    }
  }

  private class UngroupPrimaryKey
      extends DoFn<Iterable<KV<String, Iterable<Struct>>>, Iterable<Struct>> {

    @ProcessElement
    public void ungroup(
        @Element Iterable<KV<String, Iterable<Struct>>> inputBatch,
        OutputReceiver<Iterable<Struct>> output) {
      // TODO(Nito): add orderBy logic.
      ArrayList<Struct> batchWithoutKeys = new ArrayList<>();
      inputBatch.forEach(
          kvBatch -> {
            Iterable<Struct> batch = kvBatch.getValue();
            batch.forEach(batchWithoutKeys::add);
          });
      output.output(batchWithoutKeys);
    }
  }
}
