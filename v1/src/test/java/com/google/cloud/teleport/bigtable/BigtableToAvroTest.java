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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.BigtableToAvro.BigtableToAvroFn;
import static com.google.cloud.teleport.bigtable.TestUtils.addAvroCell;
import static com.google.cloud.teleport.bigtable.TestUtils.createAvroRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;

import com.google.bigtable.v2.Row;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BigtableToAvro. */
@RunWith(JUnit4.class)
public final class BigtableToAvroTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void applyBigtableToAvroFn() throws Exception {
    Row bigtableRow1 = createBigtableRow("row1");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column1", 1, "value1");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column1", 2, "value2");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column2", 1, "value3");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family2", "column1", 1, "value4");
    Row bigtableRow2 = createBigtableRow("row2");
    bigtableRow2 = upsertBigtableCell(bigtableRow2, "family2", "column2", 1, "value2");
    final List<Row> bigtableRows = ImmutableList.of(bigtableRow1, bigtableRow2);

    BigtableRow avroRow1 = createAvroRow("row1");
    addAvroCell(avroRow1, "family1", "column1", 1, "value1");
    // Expect a new cell due to a different timestamp of "2".
    addAvroCell(avroRow1, "family1", "column1", 2, "value2");
    // Expect a new cell due to a different column of "column2".
    addAvroCell(avroRow1, "family1", "column2", 1, "value3");
    // Expect a new cell due to a different family of "family2".
    addAvroCell(avroRow1, "family2", "column1", 1, "value4");
    BigtableRow avroRow2 = createAvroRow("row2");
    addAvroCell(avroRow2, "family2", "column2", 1, "value2");
    final List<BigtableRow> expectedAvroRows = ImmutableList.of(avroRow1, avroRow2);

    PCollection<BigtableRow> avroRows =
        pipeline
            .apply("Create", Create.of(bigtableRows))
            .apply("Transform to Avro", MapElements.via(new BigtableToAvroFn()));

    PAssert.that(avroRows).containsInAnyOrder(expectedAvroRows);
    pipeline.run();
  }
}
