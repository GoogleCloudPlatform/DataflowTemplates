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
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.OrderByOrder;
import com.google.cloud.teleport.v2.utils.StructComparator;
import com.google.cloud.teleport.v2.utils.StructHelper;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  abstract Integer batchSize();

  abstract List<String> primaryKeyColumns();

  @Nullable
  abstract String orderByColumnName();

  @Nullable
  abstract OrderByOrder orderByOrder();

  @Nullable
  abstract String endDateColumnName();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBatchSize(Integer value);

    public abstract Builder setPrimaryKeyColumns(List<String> value);

    public abstract Builder setOrderByColumnName(String value);

    public abstract Builder setOrderByOrder(OrderByOrder value);

    public abstract Builder setEndDateColumnName(String value);

    public abstract MakeBatchesTransform build();
  }

  public static MakeBatchesTransform.Builder builder() {
    return new AutoValue_MakeBatchesTransform.Builder();
  }

  @Override
  public PCollection<Iterable<Struct>> expand(PCollection<Struct> input) {
    return input
        .apply("AddPrimaryKey", WithKeys.of(new ExtractPrimaryKey()))
        .apply("GroupByPrimaryKey", GroupByKey.create())
        .apply("GroupArbitrarilyForBatchSize", WithKeys.of(1))
        .apply("GroupIntoBatchesOfPrimaryKey", GroupIntoBatches.ofSize(batchSize()))
        .apply("RemoveArbitraryBatchKey", Values.create())
        .apply("RemovePrimaryKeyFromBatch", ParDo.of(new UngroupPrimaryKey()))
        .apply("OrderByColumnName", ParDo.of(new OrderByColumnName()));
  }

  private class ExtractPrimaryKey implements SerializableFunction<Struct, String> {
    @Override
    public String apply(Struct record) {
      return StructHelper.of(record)
          .keyMaker(
              primaryKeyColumns(),
              endDateColumnName() == null
                  ? ImmutableList.of()
                  : ImmutableList.of(endDateColumnName()))
          // Cannot use Key directly as order is non-deterministic.
          .createKeyString();
    }
  }

  private class UngroupPrimaryKey
      extends DoFn<Iterable<KV<String, Iterable<Struct>>>, Iterable<Struct>> {

    /**
     * Ungroups data in a double batch (batch by size and primary key) into a single iterable.
     *
     * @param inputBatch Batch to ungroup.
     * @param output Output receiver.
     */
    @ProcessElement
    public void ungroup(
        @Element Iterable<KV<String, Iterable<Struct>>> inputBatch,
        OutputReceiver<Iterable<Struct>> output) {
      ArrayList<Struct> batchWithoutKeys = new ArrayList<>();
      inputBatch.forEach(
          kvBatch -> {
            Iterable<Struct> batch = kvBatch.getValue();
            batch.forEach(batchWithoutKeys::add);
          });
      output.output(batchWithoutKeys);
    }
  }

  private class OrderByColumnName extends DoFn<Iterable<Struct>, Iterable<Struct>> {

    /**
     * Orders iterable of structs by the requested orderByColumnName.
     *
     * @param inputBatch Batch to sort.
     * @param output Output receiver.
     */
    @ProcessElement
    public void sortIterable(
        @Element Iterable<Struct> inputBatch, OutputReceiver<Iterable<Struct>> output) {

      if (orderByColumnName() == null) {
        output.output(inputBatch);
      } else {
        List<Struct> orderedBatch =
            StreamSupport.stream(inputBatch.spliterator(), false)
                .sorted(StructComparator.create(orderByColumnName()))
                .collect(Collectors.toList());

        if (orderByOrder() == OrderByOrder.DESC) {
          Collections.reverse(orderedBatch);
        }

        output.output(orderedBatch);
      }
    }
  }
}
