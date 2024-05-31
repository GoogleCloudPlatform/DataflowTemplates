/*
 * Copyright (C) 2024 Google Inc.
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
package org.example.pipeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

/** */
public class Types {

  public static final Map<String, org.apache.avro.Schema> AVRO_SCHEMA_CACHE =
      new ConcurrentHashMap<>();

  public static final Schema NON_PARSED_RECORD_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addNullableStringField("not_parsed_cause")
          .addNullableStringField("record")
          .addDateTimeField("timestamp")
          .build();

  static {
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  public static byte[] toByteArray(GenericRecord record) {
    try {
      var outputStream = new ByteArrayOutputStream();
      var binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      var datumWriter = new GenericDatumWriter<GenericRecord>(record.getSchema());
      datumWriter.write(record, binaryEncoder);
      binaryEncoder.flush();
      outputStream.close();
      return outputStream.toByteArray();
    } catch (IOException ex) {
      throw new RuntimeException("Error while encoding record.", ex);
    }
  }

  public static GenericRecord toGenericRecord(String schemaString, byte[] encodedRecord) {
    try {
      var schema =
          AVRO_SCHEMA_CACHE.computeIfAbsent(
              schemaString, key -> new org.apache.avro.Schema.Parser().parse(schemaString));
      var reader = new GenericDatumReader<GenericRecord>(schema);
      return reader.read(
          null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(encodedRecord), null));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Row notParsedRow(String cause, String record, DateTime datetime) {
    return Row.withSchema(NON_PARSED_RECORD_SCHEMA)
        .withFieldValue("not_parsed_cause", cause)
        .withFieldValue("record", record)
        .withFieldValue("timestamp", datetime)
        .build();
  }
}
