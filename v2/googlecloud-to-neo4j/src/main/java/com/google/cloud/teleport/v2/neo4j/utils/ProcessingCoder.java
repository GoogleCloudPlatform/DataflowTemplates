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
package com.google.cloud.teleport.v2.neo4j.utils;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;

/** Utils for streamlining creation of processing records. */
public class ProcessingCoder {
  private static Schema getProcessingSchema() {
    return Schema.of(
        Schema.Field.of("JOB", Schema.FieldType.STRING),
        Schema.Field.of("TS", Schema.FieldType.DATETIME),
        Schema.Field.of("DESCRIPTION", Schema.FieldType.STRING),
        Schema.Field.of("TYPE", Schema.FieldType.STRING),
        Schema.Field.of("SUBTYPE", Schema.FieldType.STRING),
        Schema.Field.of("AMOUNT", Schema.FieldType.DOUBLE));
  }

  public static RowCoder of() {
    return RowCoder.of(getProcessingSchema());
  }
}
