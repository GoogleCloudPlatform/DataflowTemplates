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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;
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
        SourceSchemaReference.builder().setDbName(dbName).build();
    final long rowCountPerTable = 10;
    final long tableCount = 1;
    final TupleTag<SourceRow> sourceRowTupleTag = new TupleTag<>();
    final TupleTag<SourceTableReference> sourceTableReferenceTupleTag = new TupleTag<>();
    ReaderTransformTestUtils readerTransformTestUtils =
        new ReaderTransformTestUtils(rowCountPerTable, tableCount, sourceSchemaReference);
    var accumulatingTableReader =
        readerTransformTestUtils.getTestAccumulatingReader(
            sourceRowTupleTag, sourceTableReferenceTupleTag);
    PCollection<TestResultType> results =
        testPipeline
            .apply("ReaderUnderTest", accumulatingTableReader)
            .apply(
                "result mapper",
                new TestAccumulatorResultTracker(sourceRowTupleTag, sourceTableReferenceTupleTag));

    PAssert.that(results).satisfies(new TestSatisfyFunction(rowCountPerTable, tableCount));
    testPipeline.run();
  }

  enum TestResultType {
    SOURCE_ROW,
    TABLE_COMPLETION
  }

  static class TestAccumulatorResultTracker
      extends PTransform<@NonNull PCollectionTuple, @NonNull PCollection<TestResultType>> {
    private final TupleTag<SourceRow> sourceRowTupleTag;
    private final TupleTag<SourceTableReference> sourceTableReferenceTupleTag;

    TestAccumulatorResultTracker(
        TupleTag<SourceRow> sourceRowTupleTag,
        TupleTag<SourceTableReference> sourceTableReferenceTupleTag) {
      this.sourceRowTupleTag = sourceRowTupleTag;
      this.sourceTableReferenceTupleTag = sourceTableReferenceTupleTag;
    }

    @Override
    @NonNull
    public PCollection<TestResultType> expand(@NonNull PCollectionTuple input) {
      return PCollectionList.of(
              input
                  .get(this.sourceTableReferenceTupleTag)
                  .apply(
                      "MapTestTableCompletions",
                      MapElements.via(
                          new SimpleFunction<SourceTableReference, TestResultType>() {
                            @Override
                            public TestResultType apply(SourceTableReference input) {
                              return TestResultType.TABLE_COMPLETION;
                            }
                          })))
          .and(
              input
                  .get(this.sourceRowTupleTag)
                  .apply(
                      "MapTestRowOutput",
                      MapElements.via(
                          new SimpleFunction<>() {
                            @Override
                            public TestResultType apply(SourceRow input) {
                              return TestResultType.SOURCE_ROW;
                            }
                          })))
          .apply("FlattenMappedTestOutput", Flatten.pCollections());
    }
  }

  class TestSatisfyFunction implements SerializableFunction<Iterable<TestResultType>, Void> {
    private final long rowCountPerTable;
    private final long tableCount;

    TestSatisfyFunction(long rowCountPerTable, long tableCount) {
      this.rowCountPerTable = rowCountPerTable;
      this.tableCount = tableCount;
    }

    @Override
    public Void apply(Iterable<TestResultType> input) {
      Preconditions.checkNotNull(input);
      AtomicLong resultRowCount = new AtomicLong();
      AtomicLong resultCompletionCount = new AtomicLong();
      input.forEach(
          result -> {
            if (result.equals(TestResultType.TABLE_COMPLETION)) {
              resultCompletionCount.getAndIncrement();
            } else {
              resultRowCount.getAndIncrement();
            }
          });
      assertThat(resultRowCount.get()).isEqualTo(rowCountPerTable);
      assertThat(resultCompletionCount.get()).isEqualTo(tableCount);
      return null;
    }
  }
}
