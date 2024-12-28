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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link AccumulatingTableReader}. */
@RunWith(MockitoJUnitRunner.class)
public class AccumulatingTableReaderTest implements Serializable {
  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testAccumulatingTableReader() {
    final String dbName = "testDB";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName(dbName).build());
    final long rowCountPerTable = 10;
    final long tableCount = 1;
    final TupleTag<SourceRow> sourceRowTupleTag = new TupleTag<>();
    final TupleTag<SourceTableReference> sourceTableReferenceTupleTag = new TupleTag<>();
    ReaderTransformTestUtils readerTransformTestUtils =
        new ReaderTransformTestUtils(rowCountPerTable, tableCount, sourceSchemaReference);
    var accumulatingTableReader =
        readerTransformTestUtils.getTestAccumulatingReader(
            sourceRowTupleTag, sourceTableReferenceTupleTag);
    PCollectionTuple results = testPipeline.apply("ReaderUnderTest", accumulatingTableReader);

    PCollection<Long> rowsCount = results.get(sourceRowTupleTag).apply(Count.globally());
    PAssert.that(rowsCount).containsInAnyOrder(tableCount * rowCountPerTable);

    PAssert.that(results.get(sourceTableReferenceTupleTag))
        .containsInAnyOrder(readerTransformTestUtils.getSourceTableReferences());
    testPipeline.run();
  }
}
