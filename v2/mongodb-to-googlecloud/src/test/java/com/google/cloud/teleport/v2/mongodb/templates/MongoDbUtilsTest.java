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
package com.google.cloud.teleport.v2.mongodb.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Date;
import org.bson.Document;
import org.junit.Test;

/** Unit tests for {@link MongoDbUtils}. */
public class MongoDbUtilsTest {

  @Test
  public void testGsonHandlesInfinityValues() {
    // Create a MongoDB document with Infinity values
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("infinityValue", Double.POSITIVE_INFINITY);
    document.put("negativeInfinityValue", Double.NEGATIVE_INFINITY);
    document.put("nanValue", Double.NaN);
    document.put("normalValue", 42.0);

    // Test FLATTEN option - should not throw IllegalArgumentException
    TableRow flattenResult = MongoDbUtils.getTableSchema(document, "FLATTEN");
    assertNotNull("FLATTEN result should not be null", flattenResult);
    assertTrue("FLATTEN result should contain timestamp", flattenResult.containsKey("timestamp"));

    // Test JSON option - should not throw IllegalArgumentException
    TableRow jsonResult = MongoDbUtils.getTableSchema(document, "JSON");
    assertNotNull("JSON result should not be null", jsonResult);
    assertTrue("JSON result should contain id", jsonResult.containsKey("id"));
    assertTrue("JSON result should contain source_data", jsonResult.containsKey("source_data"));
    assertTrue("JSON result should contain timestamp", jsonResult.containsKey("timestamp"));

    // Test NONE option - should not throw IllegalArgumentException
    TableRow noneResult = MongoDbUtils.getTableSchema(document, "NONE");
    assertNotNull("NONE result should not be null", noneResult);
    assertTrue("NONE result should contain id", noneResult.containsKey("id"));
    assertTrue("NONE result should contain source_data", noneResult.containsKey("source_data"));
    assertTrue("NONE result should contain timestamp", noneResult.containsKey("timestamp"));
  }

  @Test
  public void testGsonSerializesSpecialFloatingPointValues() {
    // Test that our Gson instance can serialize special floating point values
    Document document = new Document();
    document.put("infinity", Double.POSITIVE_INFINITY);
    document.put("negativeInfinity", Double.NEGATIVE_INFINITY);
    document.put("nan", Double.NaN);

    // This should not throw an exception with our configured Gson
    String jsonString = MongoDbUtils.GSON.toJson(document);
    assertNotNull("JSON string should not be null", jsonString);
    assertTrue("JSON should contain Infinity", jsonString.contains("Infinity"));
    assertTrue("JSON should contain NaN", jsonString.contains("NaN"));
  }

  @Test
  public void testExtendedJsonDateSerialization() {
    // Create document with date field
    Date testDate = new Date(1738598501924L); // 2026-02-03T15:31:41.924Z
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", testDate);

    // Serialize with EXTENDED_JSON_WRITER_SETTINGS
    String jsonString = document.toJson(MongoDbUtils.EXTENDED_JSON_WRITER_SETTINGS);

    // Verify Extended JSON format with ISO-8601
    assertNotNull("JSON string should not be null", jsonString);
    assertTrue("Should contain $date key", jsonString.contains("\"$date\""));
    assertTrue(
        "Should contain ISO-8601 formatted date",
        jsonString.matches(
            ".*\"\\$date\"\\s*:\\s*\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z\".*"));
    assertFalse(
        "Should NOT contain locale-based date format",
        jsonString.matches(".*[A-Z][a-z]{2}\\s+\\d+,\\s+\\d{4}.*"));

    // Verify round-trip conversion
    Document parsedDocument = Document.parse(jsonString);
    assertEquals("Document ID should match", "test-id", parsedDocument.get("_id"));
  }

  @Test
  public void testExtendedJsonWithMultipleDateTypes() {
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("currentDate", new Date());
    document.put("pastDate", new Date(0L)); // 1970-01-01T00:00:00.000Z
    document.put("futureDate", new Date(4102444800000L)); // 2100-01-01
    document.put("nullDate", null);
    document.put("stringField", "not a date");

    String jsonString = document.toJson(MongoDbUtils.EXTENDED_JSON_WRITER_SETTINGS);

    assertNotNull("JSON string should not be null", jsonString);
    // Verify all date fields use Extended JSON format
    int dateCount = jsonString.split("\"\\$date\"").length - 1;
    assertEquals("Should have 3 $date entries", 3, dateCount);

    // Verify round-trip conversion
    Document parsedDocument = Document.parse(jsonString);
    assertNotNull(parsedDocument.get("currentDate"));
    assertNotNull(parsedDocument.get("pastDate"));
    assertNotNull(parsedDocument.get("futureDate"));
  }
}
