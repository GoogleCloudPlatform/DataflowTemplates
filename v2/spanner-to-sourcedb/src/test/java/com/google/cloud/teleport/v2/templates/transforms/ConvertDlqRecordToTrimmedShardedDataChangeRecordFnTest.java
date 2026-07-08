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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConvertDlqRecordToTrimmedShardedDataChangeRecordFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();

  @Mock private DoFn.ProcessContext processContext;
  private static Gson gson = new Gson();

  @Test
  public void testConvertDlqRecordToTrimmedShardedDataChangeRecordFn() throws Exception {
    ConvertDlqRecordToTrimmedShardedDataChangeRecordFn
        convertDlqRecordToTrimmedShardedDataChangeRecordFn =
            new ConvertDlqRecordToTrimmedShardedDataChangeRecordFn();
    String message = "test message";
    TrimmedShardedDataChangeRecord record = getTrimmedDataChangeRecord("shardA");
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord = new ChangeStreamErrorRecord(jsonRec, message);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(errorRecord.getOriginalRecord(), errorRecord.getOriginalRecord());
    failsafeElement.setErrorMessage(errorRecord.getErrorMessage());
    when(processContext.element()).thenReturn(failsafeElement);
    convertDlqRecordToTrimmedShardedDataChangeRecordFn.processElement(processContext);
    record.setRetryRecord(true);
    verify(processContext, times(1)).output(eq(record));
  }

  private TrimmedShardedDataChangeRecord getTrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "parent1",
        new Mod("{\"id\": \"42\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }
}
