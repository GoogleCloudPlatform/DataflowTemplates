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

import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;

/** Converts a ChangeStreamErrorRecord record to a FailsafeElement. */
public class ConvertChangeStreamErrorRecordToFailsafeElementFn
    extends DoFn<String, FailsafeElement<String, String>> implements Serializable {
  private static final Gson gson = new Gson();

  public ConvertChangeStreamErrorRecordToFailsafeElementFn() {}

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String jsonRec = c.element();
    ChangeStreamErrorRecord record = gson.fromJson(jsonRec, ChangeStreamErrorRecord.class);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(record.getOriginalRecord(), record.getOriginalRecord());
    failsafeElement.setErrorMessage(record.getErrorMessage());
    c.output(failsafeElement);
  }
}
