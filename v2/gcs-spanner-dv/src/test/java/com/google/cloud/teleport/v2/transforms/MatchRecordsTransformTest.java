/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

public class MatchRecordsTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMatchRecords() {
    ComparisonRecord record =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash1")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    PCollection<ComparisonRecord> source = pipeline.apply("CreateSource", Create.of(record));
    PCollection<ComparisonRecord> spanner = pipeline.apply("CreateSpanner", Create.of(record));

    PCollectionTuple input = PCollectionTuple.of(SOURCE_TAG, source).and(SPANNER_TAG, spanner);

    PCollectionTuple output = input.apply(new MatchRecordsTransform());

    PAssert.that(output.get(MATCHED_TAG)).containsInAnyOrder(record);
    PAssert.that(output.get(MISSING_IN_SPANNER_TAG)).empty();
    PAssert.that(output.get(MISSING_IN_SOURCE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void testMissingInSpanner() {
    ComparisonRecord record =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash1")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    PCollection<ComparisonRecord> source = pipeline.apply("CreateSource", Create.of(record));
    // we re-use the coder from the source PCollection for Spanner while creating the empty
    // PCollection for ease-of-use. The alternative is to construct a coder which is not trivial
    // given that ComparisonRecord is an autoValue class using a SchemaCoder under the hood.
    PCollection<ComparisonRecord> spanner =
        pipeline.apply("CreateSpanner", Create.empty(source.getCoder()));

    PCollectionTuple input = PCollectionTuple.of(SOURCE_TAG, source).and(SPANNER_TAG, spanner);

    PCollectionTuple output = input.apply(new MatchRecordsTransform());

    PAssert.that(output.get(MATCHED_TAG)).empty();
    PAssert.that(output.get(MISSING_IN_SPANNER_TAG)).containsInAnyOrder(record);
    PAssert.that(output.get(MISSING_IN_SOURCE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void testMissingInSource() {
    ComparisonRecord record =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash1")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    PCollection<ComparisonRecord> spanner = pipeline.apply("CreateSpanner", Create.of(record));
    PCollection<ComparisonRecord> sourceEmpty =
        pipeline.apply("CreateSource", Create.empty(spanner.getCoder()));

    PCollectionTuple input = PCollectionTuple.of(SOURCE_TAG, sourceEmpty).and(SPANNER_TAG, spanner);

    PCollectionTuple output = input.apply(new MatchRecordsTransform());

    PAssert.that(output.get(MATCHED_TAG)).empty();
    PAssert.that(output.get(MISSING_IN_SPANNER_TAG)).empty();
    PAssert.that(output.get(MISSING_IN_SOURCE_TAG)).containsInAnyOrder(record);

    pipeline.run();
  }

  @Test
  public void testMixedScenarios() {
    ComparisonRecord matched =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("matched")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    ComparisonRecord missingInSpanner =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("missingInSpanner")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    ComparisonRecord missingInSource =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("missingInSource")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    PCollection<ComparisonRecord> source =
        pipeline.apply("CreateSource", Create.of(matched, missingInSpanner));
    PCollection<ComparisonRecord> spanner =
        pipeline.apply("CreateSpanner", Create.of(matched, missingInSource));

    PCollectionTuple input = PCollectionTuple.of(SOURCE_TAG, source).and(SPANNER_TAG, spanner);

    PCollectionTuple output = input.apply(new MatchRecordsTransform());

    PAssert.that(output.get(MATCHED_TAG)).containsInAnyOrder(matched);
    PAssert.that(output.get(MISSING_IN_SPANNER_TAG)).containsInAnyOrder(missingInSpanner);
    PAssert.that(output.get(MISSING_IN_SOURCE_TAG)).containsInAnyOrder(missingInSource);

    pipeline.run();
  }
}
