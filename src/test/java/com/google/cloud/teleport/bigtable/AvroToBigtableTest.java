/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.AvroToBigtable.AvroToBigtableFn;
import static com.google.cloud.teleport.bigtable.TestUtils.addAvroCell;
import static com.google.cloud.teleport.bigtable.TestUtils.addBigtableMutation;
import static com.google.cloud.teleport.bigtable.TestUtils.createAvroRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRowMutations;

import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for AvroToBigtable. */
@RunWith(JUnit4.class)
public final class AvroToBigtableTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void applyAvroToBigtableFn() throws Exception {
    BigtableRow avroRow1 = createAvroRow("row1");
    addAvroCell(avroRow1, "family1", "column1", 1, "value1");
    addAvroCell(avroRow1, "family1", "column1", 2, "value2");
    addAvroCell(avroRow1, "family1", "column2", 1, "value3");
    addAvroCell(avroRow1, "family2", "column1", 1, "value4");
    BigtableRow avroRow2 = createAvroRow("row2");
    addAvroCell(avroRow2, "family2", "column2", 2, "value2");
    List<BigtableRow> avroRows = ImmutableList.of(avroRow1, avroRow2);

    KV<ByteString, Iterable<Mutation>> rowMutations1 = createBigtableRowMutations("row1");
    addBigtableMutation(rowMutations1, "family1", "column1", 1, "value1");
    addBigtableMutation(rowMutations1, "family1", "column1", 2, "value2");
    addBigtableMutation(rowMutations1, "family1", "column2", 1, "value3");
    addBigtableMutation(rowMutations1, "family2", "column1", 1, "value4");
    KV<ByteString, Iterable<Mutation>> rowMutations2 = createBigtableRowMutations("row2");
    addBigtableMutation(rowMutations2, "family2", "column2", 2, "value2");
    List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(rowMutations1, rowMutations2);

    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(avroRows))
            .apply("Transform to Bigtable", ParDo.of(AvroToBigtableFn.create()));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void applyAvroToBigtableFnSplitLargeRows() throws Exception {
    ValueProvider<Boolean> splitlargeRows = ValueProvider.StaticValueProvider.of(true);
    BigtableRow avroRow1 = createAvroRow("row1");
    addAvroCell(avroRow1, "family1", "column1", 1, "value1");
    addAvroCell(avroRow1, "family1", "column1", 2, "value2");
    addAvroCell(avroRow1, "family1", "column2", 1, "value3");
    addAvroCell(avroRow1, "family2", "column1", 1, "value4");
    BigtableRow avroRow2 = createAvroRow("row2");
    addAvroCell(avroRow2, "family2", "column2", 2, "value2");
    List<BigtableRow> avroRows = ImmutableList.of(avroRow1, avroRow2);

    KV<ByteString, Iterable<Mutation>> rowMutations1 = createBigtableRowMutations("row1");
    addBigtableMutation(rowMutations1, "family1", "column1", 1, "value1");
    addBigtableMutation(rowMutations1, "family1", "column1", 2, "value2");
    KV<ByteString, Iterable<Mutation>> rowMutations2 = createBigtableRowMutations("row1");
    addBigtableMutation(rowMutations2, "family1", "column2", 1, "value3");
    addBigtableMutation(rowMutations2, "family2", "column1", 1, "value4");
    KV<ByteString, Iterable<Mutation>> rowMutations3 = createBigtableRowMutations("row2");
    addBigtableMutation(rowMutations3, "family2", "column2", 2, "value2");
    List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(rowMutations1, rowMutations2, rowMutations3);

    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(avroRows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(AvroToBigtableFn.createWithSplitLargeRows(splitlargeRows, 2)));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }
}
