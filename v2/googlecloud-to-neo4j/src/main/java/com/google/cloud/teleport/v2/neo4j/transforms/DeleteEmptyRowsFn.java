/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

/** Delete empty rows transformation. */
public class DeleteEmptyRowsFn extends DoFn<Row, Row> {

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();
    if (allNull(inputRow.getValues())) {
      // do not include
    } else {
      processContext.output(inputRow);
    }
  }

  private boolean allNull(List<Object> values) {
    for (Object val : values) {
      if (val != null) {
        return false;
      }
    }
    return true;
  }
}
