/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.syndeo.transforms.datagenerator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RecordCreatorTest {

  private static final String avroSchemaString =
      "{\"type\":\"record\",\"name\":\"user_info_flat\",\"namespace\":\"com.google.syndeo\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"username\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"10\"},{\"name\":\"age\",\"type\":\"long\",\"default\":0},{\"name\":\"introduction\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"1000\"},{\"name\":\"street\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"city\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"state\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"country\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"15\"}]}";
  private static final String avroSchemaStringNested =
      "{\"type\":\"record\",\"name\":\"user_info_nested\",\"namespace\":\"com.google.syndeo\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"username\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"10\"},{\"name\":\"age\",\"type\":\"long\",\"default\":0},{\"name\":\"introduction\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"1000\"},{ \"name\":\"address\", \"type\": {\"type\": \"record\", \"name\":\"address\", \"fields\":[{\"name\":\"street\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"city\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"state\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"country\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"15\"}]}}]}";

  private static final String jsonSchemaString =
      "{"
          + "  \"type\": \"object\","
          + "  \"properties\": {"
          + "    \"id\": { \"type\": \"integer\" },"
          + "    \"username\": { \"type\": \"string\" },"
          + "    \"age\": { \"type\": \"integer\" },"
          + "    \"introduction\": { \"type\": \"string\" },"
          + "    \"street\": { \"type\": \"string\" },"
          + "    \"city\": { \"type\": \"string\" },"
          + "    \"state\": { \"type\": \"string\" },"
          + "    \"country\": { \"type\": \"string\" }"
          + "  }"
          + "}";

  @Test
  public void testCreateRowRecordWithAvroSchema() {
    Schema avroSchema = Schema.parse(avroSchemaString);
    Row row = RecordCreator.createRowRecord(avroSchema);
    assertNotNull(row);
    assertNotNull(row.getInt64("id"));
    assertNotNull(row.getInt64("age"));
    assertNotNull(row.getString("username"));
    assertNotNull(row.getString("introduction"));
    assertNotNull(row.getString("username"));
    assertNotNull(row.getString("country"));
    assertNotNull(row.getString("street"));
    // verify size
    assertTrue(row.getString("introduction").length() > 500);
    assertTrue(row.getString("username").length() < 20);
    assertTrue(row.getString("country").length() < 25);
  }

  @Test
  public void testCreateRowRecordWithAvroNestedSchema() {
    Schema avroSchema = Schema.parse(avroSchemaStringNested);
    Row row = RecordCreator.createRowRecord(avroSchema);
    assertNotNull(row);
    assertNotNull(row.getInt64("id"));
    assertNotNull(row.getString("username"));
    assertNotNull(row.getInt64("age"));
    assertNotNull(row.getString("introduction"));
    assertNotNull(row.getRow("address"));
    assertNotNull(row.getRow("address").getString("street"));
    assertNotNull(row.getRow("address").getString("state"));
    assertNotNull(row.getRow("address").getString("city"));
    assertNotNull(row.getRow("address").getString("country"));
    // verify size
    assertTrue(row.getString("introduction").length() > 500);
    assertTrue(row.getString("username").length() < 20);
    assertTrue(row.getRow("address").getString("state").length() < 20);
    assertTrue(row.getRow("address").getString("country").length() < 25);
  }
}
