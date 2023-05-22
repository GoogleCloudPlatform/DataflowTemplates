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
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for AssignShardIdFnTest class. */
public class AssignShardIdFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void basicTest() {
    List<TrimmedDataChangeRecord> records =
        Arrays.asList(getTrimmedDataChangeRecord("shard1"), getTrimmedDataChangeRecord("shard2"));
    PCollection<KV<String, TrimmedDataChangeRecord>> output =
        pipeline
            .apply(
                Create.of(records).withCoder(SerializableCoder.of(TrimmedDataChangeRecord.class)))
            .apply(ParDo.of(new AssignShardIdFn()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("shard1", getTrimmedDataChangeRecord("shard1")),
            KV.of("shard2", getTrimmedDataChangeRecord("shard2")));

    pipeline.run();
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
        1,
        "");
  }
}
