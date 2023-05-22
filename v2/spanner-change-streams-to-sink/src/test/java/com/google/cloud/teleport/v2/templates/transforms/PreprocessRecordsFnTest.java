/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for PreprocessRecordsFnTest class. */
public class PreprocessRecordsFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void basicTest() {
    PCollection<TrimmedDataChangeRecord> output =
        pipeline
            .apply(Create.of(getDataChangeRecord()))
            .apply(ParDo.of(new PreprocessRecordsFn()))
            .setCoder(SerializableCoder.of(TrimmedDataChangeRecord.class));

    PAssert.that(output)
        .containsInAnyOrder(
            getTrimmedDataChangeRecord("shard1"), getTrimmedDataChangeRecord("shard2"));

    pipeline.run();
  }

  public DataChangeRecord getDataChangeRecord() {
    return new DataChangeRecord(
        "partitionToken",
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        true,
        "recordSeq",
        "tableName",
        Arrays.asList(),
        Arrays.asList(
            new Mod(
                "{\"accountId\": \"Id1\"}",
                "{}",
                "{\"accountName\": \"abc\", \"hb_shardId\": \"shard1\"}"),
            new Mod(
                "{\"accountId\": \"Id1\"}",
                "{}",
                "{\"accountName\": \"abc\", \"hb_shardId\": \"shard2\"}")),
        ModType.valueOf("INSERT"),
        ValueCaptureType.valueOf("NEW_ROW"),
        5,
        5,
        "txnTag",
        false,
        null);
  }

  public TrimmedDataChangeRecord getTrimmedDataChangeRecord(String shardId) {
    return new TrimmedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        Collections.singletonList(
            new Mod(
                "{\"accountId\": \"Id1\"}",
                "{}",
                "{\"accountName\": \"abc\", \"hb_shardId\": \"" + shardId + "\"}")),
        ModType.valueOf("INSERT"),
        5,
        "txnTag");
  }
}
