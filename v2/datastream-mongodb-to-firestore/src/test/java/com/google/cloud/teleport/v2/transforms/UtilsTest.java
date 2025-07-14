/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.util.HashSet;
import java.util.Set;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Utils}. */
@RunWith(JUnit4.class)
public class UtilsTest {

  @Test
  public void testRemoveTableRowFields() {
    Document doc = new Document("field1", "value1").append("field2", 123).append("field3", true);
    Set<String> ignoreFields = new HashSet<>();
    ignoreFields.add("field1");
    ignoreFields.add("field3");

    Utils.removeTableRowFields(doc, ignoreFields);

    assertFalse(doc.containsKey("field1"));
    assertTrue(doc.containsKey("field2"));
    assertFalse(doc.containsKey("field3"));
  }

  @Test
  public void testRemoveTableRowFieldsEmptyIgnoreSet() {
    Document doc = new Document("field1", "value1").append("field2", 123);
    Set<String> ignoreFields = new HashSet<>();

    Utils.removeTableRowFields(doc, ignoreFields);

    assertTrue(doc.containsKey("field1"));
    assertTrue(doc.containsKey("field2"));
  }

  @Test
  public void testRemoveTableRowFieldsAllFieldsIgnored() {
    Document doc = new Document("field1", "value1").append("field2", 123);
    Set<String> ignoreFields = new HashSet<>();
    ignoreFields.add("field1");
    ignoreFields.add("field2");

    Utils.removeTableRowFields(doc, ignoreFields);

    assertFalse(doc.containsKey("field1"));
    assertFalse(doc.containsKey("field2"));
  }

  @Test
  public void testIsNewerTimestamp_ts1Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 2L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);

    assertTrue(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_ts2Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 2L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameSeconds_ts1Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertTrue(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameSeconds_ts2Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameTimestamp() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testJsonToDocument() {
    String jsonString =
        "{\"_id\":\"{\\\"$oid\\\": \\\"6811235eaf8583310cb9d2e9\\\"}\",\"data\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"6811235eaf8583310cb9d2e9\\\"},\\\"arrayField\\\": [\\\"hello\\\",10],\\\"dateField\\\": {\\\"$date\\\": 1565546054692},\\\"dateBefore1970\\\": {\\\"$date\\\": -1577923200000},\\\"decimal128Field\\\": {\\\"$numberDecimal\\\": \\\"10.99\\\"},\\\"documentField\\\": {\\\"a\\\": \\\"hello\\\"},\\\"doubleField\\\": 10.5,\\\"infiniteNumber\\\": Infinity,\\\"int32field\\\": 10,\\\"int64Field\\\": {\\\"$numberLong\\\": \\\"50\\\"},\\\"minKeyField\\\": {\\\"$minKey\\\": 1},\\\"maxKeyField\\\": {\\\"$maxKey\\\": 1},\\\"regexField\\\": {\\\"$regex\\\": \\\"^H\\\",\\\"$options\\\": \\\"i\\\"},\\\"timestampField\\\": {\\\"$timestamp\\\": {\\\"t\\\": 1565545664,\\\"i\\\": 1}},\\\"uuid\\\": {\\\"$binary\\\": \\\"OyQRAeK7QlWMr0E2xWapYg==\\\",\\\"$type\\\": \\\"04\\\"}}\",\"_metadata_stream\":\"extended-types-test\",\"_metadata_timestamp\":1745957556,\"_metadata_read_timestamp\":1745957556,\"_metadata_dataflow_timestamp\":1745963670,\"_metadata_read_method\":\"backfill\",\"_metadata_source_type\":\"backfill\",\"_metadata_deleted\":false,\"_metadata_table\":null,\"_metadata_change_type\":\"READ\",\"_metadata_primary_keys\":null,\"_metadata_uuid\":\"2a9cf8eb-4f35-433c-899a-39921d4c8587\",\"_metadata_timestamp_seconds\":\"1745957556\",\"_metadata_timestamp_nanos\":\"184498000\",\"_metadata_source\":{\"database\":\"extended_types\",\"collection\":\"mycol\",\"change_type\":\"READ\",\"is_deleted\":false,\"primary_key\":[\"_id\"]},\"_metadata_error\":null,\"_metadata_retry_count\":116}";
    Document result = Utils.jsonToDocument(jsonString, 1L);
    assertTrue(
        result
            .toJson()
            .equals(
                "{\"_id\": 1, \"arrayField\": [\"hello\", 10], \"dateField\": {\"$date\": \"2019-08-11T17:54:14.692Z\"}, \"dateBefore1970\": {\"$date\": {\"$numberLong\": \"-1577923200000\"}}, \"decimal128Field\": {\"$numberDecimal\": \"10.99\"}, \"documentField\": {\"a\": \"hello\"}, \"doubleField\": 10.5, \"infiniteNumber\": {\"$numberDouble\": \"Infinity\"}, \"int32field\": 10, \"int64Field\": 50, \"minKeyField\": {\"$minKey\": 1}, \"maxKeyField\": {\"$maxKey\": 1}, \"regexField\": {\"$regularExpression\": {\"pattern\": \"^H\", \"options\": \"i\"}}, \"timestampField\": {\"$timestamp\": {\"t\": 1565545664, \"i\": 1}}, \"uuid\": {\"$binary\": {\"base64\": \"OyQRAeK7QlWMr0E2xWapYg==\", \"subType\": \"04\"}}}"));
  }
}
