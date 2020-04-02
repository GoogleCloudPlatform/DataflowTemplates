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
package com.google.cloud.teleport.cdc.merge;

import com.google.cloud.teleport.cdc.merge.BigQueryMerger.TriggerPerKeyOnFixedIntervals;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for {@link BigQueryMerger}.
 */
public class BigQueryMergerTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static final Logger LOG = LoggerFactory.getLogger(BigQueryMergerTest.class);

  static final List<String> PRIMARY_KEY_COLUMNS = Arrays.asList("pk1", "pk2");
  static final String TIMESTAMP_META_FIELD = "_metadata_timestamp";
  static final String DELETED_META_FIELD = "_metadata_deleted";

  static final List<String> FULL_COLUMN_LIST =
      Arrays.asList("pk1", "pk2", "col1", "col2", "col3", TIMESTAMP_META_FIELD, DELETED_META_FIELD);

  static final String DEFAULT_DATASET = "default_dataset";

  static final String TABLE_1 = String.format("%s.%s", DEFAULT_DATASET, "table1");
  ;
  static final String TABLE_2 = String.format("%s.%s", DEFAULT_DATASET, "table2");

  static final Integer WINDOW_SIZE_MINUTES = 5;

  private static MergeInfo createMergeInfo(String table) {
    return MergeInfo.create(
        TIMESTAMP_META_FIELD,
        DELETED_META_FIELD,
        String.format("%s_staging", table),
        table,
        FULL_COLUMN_LIST,
        PRIMARY_KEY_COLUMNS);
  }

  @Test
  public void testAutoValueMergeInfoClass() throws Exception {
    MergeInfo mergeInfo =
        MergeInfo.create(
            TIMESTAMP_META_FIELD,
            DELETED_META_FIELD,
            TABLE_1,
            TABLE_2,
            FULL_COLUMN_LIST,
            PRIMARY_KEY_COLUMNS);

    PCollection<KV<String, MergeInfo>> result =
        pipeline
            .apply(Create.of(mergeInfo))
            .apply(
                WithKeys.<String, MergeInfo>of(mi -> mi.getReplicaTable())
                    .withKeyType(TypeDescriptors.strings()))
            .apply(
                new TriggerPerKeyOnFixedIntervals<>(Duration.standardMinutes(WINDOW_SIZE_MINUTES)));

    PAssert.that(result).containsInAnyOrder(KV.of(mergeInfo.getReplicaTable(), mergeInfo));
    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleElementPerTablePerWindow() throws CannotProvideCoderException {

    // TODO: WHY DO WE NEED TO ADVANCE 2*WINDOW_SIZE_MINUTES TO FORCE THE TRIGGERS?????
    TestStream<KV<String, String>> inputStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            // First window. Table 1 and Table 2.
            .addElements(
                KV.of(TABLE_1, "window1"), KV.of(TABLE_1, "window1"), KV.of(TABLE_2, "window1"))
            // Second window. Table 1 and Table 2.
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .addElements(
                KV.of(TABLE_1, "window2"), KV.of(TABLE_1, "window2"),
                KV.of(TABLE_2, "window2"), KV.of(TABLE_2, "window2"))
            .addElements(
                KV.of(TABLE_1, "window2"), KV.of(TABLE_1, "window2"),
                KV.of(TABLE_2, "window2"), KV.of(TABLE_2, "window2"))
            // Third window. Only table 2.
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .addElements(KV.of(TABLE_2, "window3"), KV.of(TABLE_2, "window3"))
            // Fourth window. Only table 2.
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .addElements(KV.of(TABLE_2, "window4"), KV.of(TABLE_2, "window4"))
            // .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(KV.of(TABLE_2, "window4"), KV.of(TABLE_2, "window4"))
            // Fifth window. Only table 1.
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .addElements(KV.of(TABLE_1, "window5"), KV.of(TABLE_1, "window5"))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceProcessingTime(Duration.standardMinutes(WINDOW_SIZE_MINUTES))
            .advanceWatermarkToInfinity();

    List<KV<String, String>> expectedResult =
        Arrays.asList(
            KV.of(TABLE_1, "window1"),
            KV.of(TABLE_2, "window1"), // First window
            KV.of(TABLE_1, "window2"),
            KV.of(TABLE_2, "window2"), // Second window
            KV.of(TABLE_2, "window3"), // Third window
            KV.of(TABLE_2, "window4"), // Fourth window
            KV.of(TABLE_1, "window5") // Fifth window
            );

    PCollection<KV<String, String>> result =
        pipeline
            .apply(inputStream)
            .apply(
                new TriggerPerKeyOnFixedIntervals<>(Duration.standardMinutes(WINDOW_SIZE_MINUTES)));
    PAssert.that(result).containsInAnyOrder(expectedResult);
    pipeline.run().waitUntilFinish();
  }
}
