/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.applier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class DecodeRows extends PTransform<PCollection<byte[]>, PCollection<Row>> {

  private final Coder<Row> coder;
  private final Schema schema;

  private DecodeRows(Schema beamSchema) {
    this.coder = RowCoder.of(beamSchema);
    this.schema = beamSchema;
  }

  public static DecodeRows withSchema(Schema beamSchema) {
    return new DecodeRows(beamSchema);
  }

  public PCollection<Row> expand(PCollection<byte[]> input) {
    return input.apply(MapElements.into(TypeDescriptors.rows())
        .via(elm -> {
          ByteArrayInputStream bis = new ByteArrayInputStream(elm);
          try {
            return coder.decode(bis);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }))
        .setRowSchema(this.schema);
  }
}
