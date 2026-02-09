/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * DoFn to extract the table name from a {@link SourceRow} and delimit it to match the format used
 * in {@link SourceTableReference}.
 */
class ExtractTableNameFn extends DoFn<SourceRow, String> {
  @ProcessElement
  public void processElement(@Element SourceRow row, OutputReceiver<String> out) {
    out.output(delimitIdentifier(row.tableName()));
  }

  private String delimitIdentifier(String identifier) {
    return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
  }
}
