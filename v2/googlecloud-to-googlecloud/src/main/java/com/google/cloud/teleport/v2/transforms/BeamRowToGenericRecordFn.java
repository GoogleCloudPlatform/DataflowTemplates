/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.utils.SerializableSchemaSupplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

/** DoFn for converting Beam Row to GenericRecord. */
public class BeamRowToGenericRecordFn extends DoFn<Row, GenericRecord> {

  private final SerializableSchemaSupplier avroSchema;

  public BeamRowToGenericRecordFn(Schema serializedAvroSchema) {
    this.avroSchema = SerializableSchemaSupplier.of(serializedAvroSchema);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    GenericRecord genericRecord = AvroUtils.toGenericRecord(c.element(), avroSchema.get());
    c.output(genericRecord);
  }
}
