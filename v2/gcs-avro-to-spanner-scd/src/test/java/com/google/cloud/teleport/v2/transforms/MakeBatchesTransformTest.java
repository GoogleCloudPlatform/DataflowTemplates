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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.OrderByOrder;
import com.google.cloud.teleport.v2.utils.StructHelper;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class MakeBatchesTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final ImmutableList<String> PRIMARY_KEYS = ImmutableList.of("id");

  private static class SampleCreator {

    public static SampleCreator create() {
      return new SampleCreator();
    }

    public List<Struct> createSamples(int count) {
      return IntStream.range(0, count)
          .boxed()
          .map(i -> createSimpleStruct())
          .collect(Collectors.toList());
    }

    public Struct createSimpleStruct() {
      return Struct.newBuilder().set("id").to(UUID.randomUUID().toString()).build();
    }
  }

  @Test
  public void testExpand_createsBatches_oneBatch() {
    int batchSize = 10;
    var inputRecords = SampleCreator.create().createSamples(5);
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(batchSize)
                .setPrimaryKeyColumns(PRIMARY_KEYS)
                .build());

    PAssert.thatSingletonIterable(output).containsInAnyOrder(inputRecords);
    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_createsBatches_multipleBatchesOfBatchSize() {
    int batchSize = 10;
    List<Struct> inputRecords = SampleCreator.create().createSamples(25);
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));
    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(batchSize)
                .setPrimaryKeyColumns(PRIMARY_KEYS)
                .build());

    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              // Expecting 25 rows to batch in 3 batches of size 10, 10, 5.
              assertThat(listOfBatches).hasSize(3);
              listOfBatches.forEach(
                  batch -> assertThat(ImmutableList.copyOf(batch).size()).isAtMost(batchSize));
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_createsBatches_batchesPrimaryKeysTogether_evenWhenLargerThatBatchSize() {
    int batchSize = 2;
    SampleCreator sampleCreator = SampleCreator.create();
    ArrayList<Struct> inputRecords =
        new ArrayList<>(Collections.nCopies(4, sampleCreator.createSimpleStruct()));
    inputRecords.addAll(Collections.nCopies(2, sampleCreator.createSimpleStruct()));
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(batchSize)
                .setPrimaryKeyColumns(PRIMARY_KEYS)
                .build());

    PAssert.thatSingletonIterable(output).containsInAnyOrder(inputRecords);
    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);
              // Expected size is 6 from 2 unique keys: one with 4 rows, and one with 2 rows.
              listOfBatches.forEach(
                  batch -> assertThat(ImmutableList.copyOf(batch).size()).isEqualTo(6));
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_createsBatches_batchesPrimaryKeysTogether() {
    int batchSize = 2;
    SampleCreator sampleCreator = SampleCreator.create();
    Struct repeatedRow = sampleCreator.createSimpleStruct();
    String repeatedKey = StructHelper.of(repeatedRow).keyMaker(PRIMARY_KEYS).createKeyString();
    ArrayList<Struct> inputRecords = new ArrayList<>(Collections.nCopies(4, repeatedRow));
    inputRecords.addAll(sampleCreator.createSamples(5));
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(batchSize)
                .setPrimaryKeyColumns(PRIMARY_KEYS)
                .build());

    PAssert.that(output)
        .satisfies(
            collection -> {
              // Expecting 3 batches from 6 unique keys:
              // 1 key with 4 repeated rows, and 5 other unique keys with 1 row each.
              ImmutableList<Iterable<Struct>> batches = ImmutableList.copyOf(collection);
              assertThat(batches.size()).isEqualTo(3);

              long countRepeated =
                  batches.stream()
                      .map(ImmutableList::copyOf)
                      // Given batchSize = 2, expecting batch sizes of size:
                      // 5, for the case with 4 repeated rows + another row.
                      // 2 for the general case, 1 if there are not enough elements.
                      .peek(batch -> assertThat(batch.size()).isIn(ImmutableList.of(5, 2, 1)))
                      .filter(batch -> batch.size() == 5)
                      .flatMap(List::stream)
                      // Expecting the repeated row to appear 4 times.
                      .filter(
                          record ->
                              StructHelper.of(record)
                                  .keyMaker(PRIMARY_KEYS)
                                  .createKeyString()
                                  .equals(repeatedKey))
                      .count();
              assertThat(countRepeated).isEqualTo(4);
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_createsBatches_ignoresEndDateColumn() {
    int batchSize = 10;
    var inputRecords = SampleCreator.create().createSamples(5);
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(batchSize)
                .setPrimaryKeyColumns(ImmutableList.of("id", "end_date"))
                .setEndDateColumnName("end_date")
                .build());

    PAssert.thatSingletonIterable(output).containsInAnyOrder(inputRecords);
    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_sortsBatches_default() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_TIMESTAMP)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(1000))
            .set("other")
            .to("f")
            .build();
    var inputRecords = ImmutableList.of(r2, r3, r1); // Out of order.
    var expectedOutputRecords = ImmutableList.of(r1, r2, r3);
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(3)
                .setPrimaryKeyColumns(ImmutableList.of("other"))
                .setOrderByColumnName("orderColumn")
                .build());

    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);

              Iterable<Struct> batch = listOfBatches.get(0);
              assertThat(batch).containsExactlyElementsIn(expectedOutputRecords).inOrder();
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_sortsBatches_asc() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_TIMESTAMP)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(1000))
            .set("other")
            .to("f")
            .build();
    var inputRecords = ImmutableList.of(r2, r3, r1); // Out of order.
    var expectedOutputRecords = ImmutableList.of(r1, r2, r3);
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(3)
                .setPrimaryKeyColumns(ImmutableList.of("other"))
                .setOrderByColumnName("orderColumn")
                .setOrderByOrder(OrderByOrder.ASC)
                .build());

    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);

              Iterable<Struct> batch = listOfBatches.get(0);
              assertThat(batch).containsExactlyElementsIn(expectedOutputRecords).inOrder();
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpand_sortsBatches_desc() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_TIMESTAMP)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(1000))
            .set("other")
            .to("f")
            .build();
    var inputRecords = ImmutableList.of(r2, r3, r1); // Out of order.
    var expectedOutputRecords = ImmutableList.of(r3, r2, r1); // Inverse order.
    PCollection<Struct> input = pipeline.apply(Create.of(inputRecords));

    PCollection<Iterable<Struct>> output =
        input.apply(
            MakeBatchesTransform.builder()
                .setBatchSize(3)
                .setPrimaryKeyColumns(ImmutableList.of("other"))
                .setOrderByColumnName("orderColumn")
                .setOrderByOrder(OrderByOrder.DESC)
                .build());

    PAssert.that(output)
        .satisfies(
            collection -> {
              ImmutableList<Iterable<Struct>> listOfBatches = ImmutableList.copyOf(collection);
              assertThat(listOfBatches).hasSize(1);

              Iterable<Struct> batch = listOfBatches.get(0);
              assertThat(batch).containsExactlyElementsIn(expectedOutputRecords).inOrder();
              return null;
            });
    pipeline.run().waitUntilFinish();
  }
}
