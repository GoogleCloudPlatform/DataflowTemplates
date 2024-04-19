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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import com.google.cloud.teleport.v2.utils.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroTestingHelper {
  public static final String TIMESTAMPTZ_SCHEMA_JSON =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"timestampTz\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"timestamp\",\n"
          + "     \"type\": \"long\",\n"
          + "     \"logicalType\": \"timestamp-micros\"},\n"
          + "    {\"name\": \"offset\",\n"
          + "     \"type\": \"int\",\n"
          + "     \"logicalType\": \"time-millis\"}\n"
          + "  ]\n"
          + "}";

  public static final String DATETIME_SCHEMA_JSON =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"datetime\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"date\",\n"
          + "     \"type\": \"int\",\n"
          + "     \"logicalType\": \"date\"},\n"
          + "    {\"name\": \"time\",\n"
          + "     \"type\": \"long\",\n"
          + "     \"logicalType\": \"time-micros\"}\n"
          + "  ]\n"
          + "}";

  public static GenericRecord createTimestampTzRecord(Long timestamp, Integer offset) {
    Schema avroSchema = SchemaUtils.parseAvroSchema(TIMESTAMPTZ_SCHEMA_JSON);
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("timestamp", timestamp);
    genericRecord.put("offset", offset);
    return genericRecord;
  }

  public static GenericRecord createDatetimeRecord(Integer date, Long time) {
    Schema avroSchema = SchemaUtils.parseAvroSchema(DATETIME_SCHEMA_JSON);
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("date", date);
    genericRecord.put("time", time);
    return genericRecord;
  }
}
