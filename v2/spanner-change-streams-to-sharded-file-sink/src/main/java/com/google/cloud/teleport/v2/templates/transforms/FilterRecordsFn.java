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

import com.google.cloud.teleport.v2.templates.constants.Constants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

/** This DoFn filters the records that we do not want to process. */
public class FilterRecordsFn extends DoFn<DataChangeRecord, DataChangeRecord> {

  private final String filtrationMode;

  public FilterRecordsFn(String filtrationMode) {
    this.filtrationMode = filtrationMode;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    DataChangeRecord record = c.element();

    // In this mode, filter no records.
    if (filtrationMode.equals(Constants.FILTRATION_MODE_NONE)) {
      c.output(record);
      return;
    }

    // TODO: Fetch forward migration Dataflow job id and do full string match for the tag.
    if (!record.getTransactionTag().startsWith(Constants.FWD_MIGRATION_TRANSACTION_TAG_PREFIX)) {
      c.output(record);
    }
  }
}
