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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class BinaryAvroDeserializer implements Deserializer<GenericRecord> {
  private Schema schema;

  public BinaryAvroDeserializer() {}

  public BinaryAvroDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public GenericRecord deserialize(String topic, Headers header, byte[] bytes) {
    return deserialize(topic, bytes);
  }

  @Override
  public GenericRecord deserialize(String topic, byte[] bytes) {
    try {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(this.schema);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new SerializationException("Error deserialing avro message", e.getCause());
    }
  }

  public void close() {}
}
