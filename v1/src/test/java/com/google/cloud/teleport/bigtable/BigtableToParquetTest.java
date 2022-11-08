/*
 * Copyright (C) 2019 Google LLC
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

import static com.google.cloud.teleport.bigtable.BigtableToParquet.BigtableToParquetFn;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;

import com.google.bigtable.v2.Row;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigtableToParquet}. */
@RunWith(JUnit4.class)
public final class BigtableToParquetTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void applyBigtableToParquetFn() throws Exception {
    Row row = createBigtableRow("row1");
    row = upsertBigtableCell(row, "family1", "column1", 1, "value1");

    GenericRecord parquetRecord = new GenericData.Record(BigtableRow.getClassSchema());

    byte[] rowKey = "row1".getBytes();
    ByteBuffer key = ByteBuffer.wrap(rowKey);

    List<BigtableCell> cells = new ArrayList<>();
    String family = "family1";
    byte[] cellQualifier = "column1".getBytes();
    ByteBuffer qualifier = ByteBuffer.wrap(cellQualifier);
    Long timestamp = 1L;
    byte[] cellValue = "value1".getBytes();
    ByteBuffer value = ByteBuffer.wrap(cellValue);
    cells.add(new BigtableCell(family, qualifier, timestamp, value));

    parquetRecord.put("key", key);
    parquetRecord.put("cells", cells);

    PCollection<GenericRecord> parquetRows =
        pipeline
            .apply("Create", Create.of(row))
            .apply("Transform to Parquet", MapElements.via(new BigtableToParquetFn()))
            .setCoder(AvroCoder.of(GenericRecord.class, BigtableRow.getClassSchema()));

    // Assert on the parquetRows.
    PAssert.that(parquetRows).containsInAnyOrder(parquetRecord);

    pipeline.run();
  }
}
