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

import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

/** This DoFn does miscellaneous minor transformations before processing. */
public class PreprocessRecordsFn extends DoFn<DataChangeRecord, TrimmedShardedDataChangeRecord> {

  @ProcessElement
  public void processElement(ProcessContext c) {

    DataChangeRecord record = c.element();
    // A single record can have multiple modifications. We create a separate record for each mod
    // while also trimming away the unnecessary fields.
    for (Mod mod : record.getMods()) {
      c.output(
          new TrimmedShardedDataChangeRecord(
              record.getCommitTimestamp(),
              record.getServerTransactionId(),
              record.getRecordSequence(),
              record.getTableName(),
              Collections.singletonList(mod),
              record.getModType(),
              record.getNumberOfRecordsInTransaction(),
              record.getTransactionTag()));
    }
  }
}
