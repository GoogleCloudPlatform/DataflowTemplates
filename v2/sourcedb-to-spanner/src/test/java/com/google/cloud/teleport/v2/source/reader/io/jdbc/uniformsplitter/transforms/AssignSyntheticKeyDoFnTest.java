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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
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

@RunWith(JUnit4.class)
public class AssignSyntheticKeyDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testAssignSyntheticKey() {
    TableIdentifier table1 = TableIdentifier.builder().setTableName("table1").build();
    TableIdentifier table2 = TableIdentifier.builder().setTableName("table2").build();

    Range range1 =
        Range.<Long>builder()
            .setTableIdentifier(table1)
            .setColName("id")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .build();

    Range range2 =
        Range.<Long>builder()
            .setTableIdentifier(table2)
            .setColName("id")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .build();

    int numBatches = 10;
    int expectedKey1 = Math.floorMod(table1.hashCode(), numBatches);
    int expectedKey2 = Math.floorMod(table2.hashCode(), numBatches);

    PCollection<KV<Integer, Range>> output =
        pipeline
            .apply(Create.of(range1, range2))
            .apply(ParDo.of(AssignSyntheticKeyDoFn.builder().setNumBatches(numBatches).build()));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(expectedKey1, range1), KV.of(expectedKey2, range2));

    pipeline.run();
  }
}
