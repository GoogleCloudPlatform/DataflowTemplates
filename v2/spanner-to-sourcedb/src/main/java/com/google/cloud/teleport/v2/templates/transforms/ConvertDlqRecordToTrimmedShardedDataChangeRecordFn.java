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

import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;

/** Converts a DLQ record to a TrimmedShardedDataChangeRecord. */
public class ConvertDlqRecordToTrimmedShardedDataChangeRecordFn
    extends DoFn<FailsafeElement<String, String>, TrimmedShardedDataChangeRecord>
    implements Serializable {
  private static final Gson gson = new Gson();

  public ConvertDlqRecordToTrimmedShardedDataChangeRecordFn() {}

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String jsonRec = c.element().getPayload();
    TrimmedShardedDataChangeRecord record =
        gson.fromJson(jsonRec, TrimmedShardedDataChangeRecord.class);
    record.setRetryRecord(true);
    c.output(record);
  }
}
