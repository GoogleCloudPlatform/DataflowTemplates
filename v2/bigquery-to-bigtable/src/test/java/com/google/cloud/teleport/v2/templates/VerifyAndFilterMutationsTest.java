/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryToBigtable.VerifyAndFilterMutationsFn}. */
@RunWith(JUnit4.class)
public class VerifyAndFilterMutationsTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  public void testValidAndEmptyRows() {
    // 1. Create test input PCollection
    List<Mutation> input = new ArrayList<>();
    Put put =
        new Put(Bytes.toBytes("row1"))
            .addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qf1"), Bytes.toBytes("v1"));
    input.add(put);
    put = new Put(Bytes.toBytes("row2")); // empty row
    input.add(put);
    put =
        new Put(Bytes.toBytes("row3"))
            .addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("qf3"), Bytes.toBytes("v3"));
    input.add(put);
    PCollection<Mutation> mutations = p.apply(Create.of(input));

    // 2. Apply the DoFn using ParDo
    PCollection<Mutation> filteredMutations =
        mutations.apply(
            "VerifyAndFilterMutations",
            ParDo.of(new BigQueryToBigtable.VerifyAndFilterMutationsFn()));

    // Assert
    // Assert: Expected Rows
    PAssert.that(filteredMutations)
        .satisfies(
            actualMutations -> {
              List<Put> actualPuts =
                  StreamSupport.stream(actualMutations.spliterator(), false)
                      .filter(m -> m instanceof Put)
                      .map(m -> (Put) m)
                      .collect(Collectors.toList());

              // 3. Assert the expected output
              List<Put> expectedOutput = new ArrayList<>();
              Put expectedPut =
                  new Put(Bytes.toBytes("row1"))
                      .addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qf1"), Bytes.toBytes("v1"));
              expectedOutput.add(expectedPut);
              expectedPut =
                  new Put(Bytes.toBytes("row3"))
                      .addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("qf3"), Bytes.toBytes("v3"));
              expectedOutput.add(expectedPut);

              for (Put expectedPutCheck : expectedOutput) {
                String expectedRowKey = Bytes.toString(expectedPutCheck.getRow());
                String actualRowKey = null;

                for (Put actualPutCheck : actualPuts) {
                  if (expectedRowKey.equals(
                      Bytes.toString(actualPutCheck.getRow()))) { // matching row found
                    actualRowKey = Bytes.toString(actualPutCheck.getRow());
                    break;
                  }
                }

                // Assert on row keys
                assertThat(actualRowKey).isEqualTo(expectedRowKey);
              }

              return null;
            });

    // 4. Run the pipeline
    p.run().waitUntilFinish();
  }
}
