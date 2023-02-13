/*
 * Copyright (C) 2019 Google LLC
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

import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * The {@link KeyValueToGenericRecordFn} class converts a KV&lt;String, String&gt; to a
 * GenericRecord using a static schema.
 */
public class KeyValueToGenericRecordFn extends DoFn<KV<String, String>, GenericRecord> {

  /** Schema used for generating a GenericRecord. */
  private static final String SCHEMA_STRING =
      "{\n"
          + " \"namespace\": \"com.google.cloud.teleport.v2.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"GenericKafkaRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"message\",\"type\": \"string\"},\n"
          + "     {\"name\": \"attributes\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
          + "     {\"name\": \"timestamp\", \"type\": \"long\"}\n"
          + " ]\n"
          + "}";

  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  /** Generates the records using {@link GenericRecordBuilder}. */
  @ProcessElement
  public void processElement(ProcessContext c) {

    KV<String, String> message = c.element();
    String attributeKey = message.getKey();
    String attributeValue = message.getValue();

    Map<String, String> attributeMap;

    if (attributeValue != null) {
      if (attributeKey != null) {
        attributeMap = Collections.singletonMap(attributeKey, attributeValue);
      } else {
        attributeMap = Collections.singletonMap("", attributeValue);
      }
    } else {
      attributeMap = Collections.EMPTY_MAP;
    }

    c.output(
        new GenericRecordBuilder(SCHEMA)
            .set("message", attributeValue)
            .set("attributes", attributeMap)
            .set("timestamp", c.timestamp().getMillis())
            .build());
  }
}
