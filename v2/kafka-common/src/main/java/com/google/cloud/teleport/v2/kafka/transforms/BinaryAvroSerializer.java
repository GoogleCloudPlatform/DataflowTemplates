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
package com.google.cloud.teleport.v2.kafka.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class BinaryAvroSerializer implements Serializer<GenericRecord> {
  private Schema schema;

  public BinaryAvroSerializer() {}

  public BinaryAvroSerializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public byte[] serialize(String subject, Headers headers, GenericRecord record) {
    return serialize(subject, record);
  }

  @Override
  public byte[] serialize(String topic, GenericRecord record) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(this.schema);
      writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new SerializationException("Error serialing avro message", e.getCause());
    }
  }

  public void close() {}
}
