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
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for FilterRecordsFnTest class. */
public class FilterRecordsFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void basicTest() {
    PCollection<DataChangeRecord> output =
        pipeline
            .apply(
                Create.of(
                    getDataChangeRecord("tx1", "txBy=123"),
                    getDataChangeRecord("tx2", ""),
                    getDataChangeRecord("tx3", "txBy=456"),
                    getDataChangeRecord("tx4", "abc"),
                    getDataChangeRecord("tx5", "txBy=678")))
            .apply(ParDo.of(new FilterRecordsFn()));

    PAssert.that(output)
        .containsInAnyOrder(getDataChangeRecord("tx2", ""), getDataChangeRecord("tx4", "abc"));
    PCollection<Long> count = output.apply(Count.globally());
    PAssert.thatSingleton(count).isEqualTo(2L);
    pipeline.run();
  }

  public DataChangeRecord getDataChangeRecord(String serverTransactionId, String txnTag) {
    return new DataChangeRecord(
        "partitionToken",
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        serverTransactionId,
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
        txnTag,
        false,
        null);
  }
}
