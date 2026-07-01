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
  public void testGetTableSchemaFlattenWithNestedDocument() {
    Document nested = new Document("nestedKey", "nestedValue").append("count", 42);
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("metadata", nested);

    TableRow result = MongoDbUtils.getTableSchema(document, "FLATTEN");

    assertNotNull(result);
    // Nested document should be serialized as JSON string via EXTENDED_JSON_WRITER_SETTINGS
    String metadataJson = (String) result.get("metadata");
    assertNotNull("metadata should be serialized", metadataJson);
    assertTrue("Should contain nestedKey", metadataJson.contains("nestedKey"));
    assertTrue("Should contain nestedValue", metadataJson.contains("nestedValue"));
  }

  @Test
  public void testGetTableSchemaFlattenWithDateField() {
    Date testDate = new Date(1738598501924L); // 2025-02-03T15:41:41.924Z
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", testDate);

    TableRow result = MongoDbUtils.getTableSchema(document, "FLATTEN", true);

    assertNotNull(result);
    String dateValue = (String) result.get("createdAt");
    assertNotNull("createdAt should be set", dateValue);
    // Should be ISO-8601 format like "2025-02-03T15:41:41.924Z"
    assertTrue(
        "Date should be in ISO-8601 format",
        dateValue.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"));
  }

  @Test
  public void testGetTableSchemaFlattenWithDateFieldLegacyDefault() {
    // With the flag disabled (the default), dates retain the previous Date.toString() output.
    Date testDate = new Date(1738598501924L);
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", testDate);

    TableRow defaultResult = MongoDbUtils.getTableSchema(document, "FLATTEN");
    TableRow explicitFalse = MongoDbUtils.getTableSchema(document, "FLATTEN", false);

    assertEquals(
        "2-arg overload must default to legacy behavior",
        defaultResult.get("createdAt"),
        explicitFalse.get("createdAt"));
    assertEquals(
        "Legacy FLATTEN date should be Date.toString()",
        testDate.toString(),
        defaultResult.get("createdAt"));
  }

  @Test
  public void testGetTableSchemaFlattenWithDateFieldFallbackForOutOfRangeYear() {
    // Relaxed extended JSON only emits an ISO string for years 1970-9999; outside that range it
    // emits {"$date":{"$numberLong":...}}, so the ISO extraction fails and we fall back to
    // Date.toString(). This exercises the catch branch.
    Date outOfRangeDate = new Date(253402300800000L); // year 10000-01-01
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", outOfRangeDate);

    TableRow result = MongoDbUtils.getTableSchema(document, "FLATTEN", true);

    assertEquals(
        "Out-of-range date should fall back to Date.toString()",
        outOfRangeDate.toString(),
        result.get("createdAt"));
  }

  @Test
  public void testGetTableSchemaJsonIso8601DateSerialization() {
    // ISO path: dates become $date Extended JSON and null fields are still omitted.
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", new Date(1738598501924L));
    document.put("nullField", null);

    TableRow result = MongoDbUtils.getTableSchema(document, "JSON", true);

    @SuppressWarnings("unchecked")
    java.util.Map<String, Object> sourceData =
        (java.util.Map<String, Object>) result.get("source_data");
    assertNotNull("source_data should be set", sourceData);
    assertFalse("Null field should be omitted", sourceData.containsKey("nullField"));
    assertTrue("Date field should be present", sourceData.containsKey("createdAt"));
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

  @Test
  public void testGetTableSchemaJsonOmitsNullFields() {
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("presentField", "value");
    document.put("nullField", null);

    TableRow result = MongoDbUtils.getTableSchema(document, "JSON");

    assertNotNull(result);
    // source_data is a Map for the JSON user option.
    @SuppressWarnings("unchecked")
    java.util.Map<String, Object> sourceData =
        (java.util.Map<String, Object>) result.get("source_data");
    assertNotNull("source_data should be set", sourceData);
    assertTrue("Non-null field should be present", sourceData.containsKey("presentField"));
    assertFalse("Null field should be omitted", sourceData.containsKey("nullField"));
  }

  @Test
  public void testGetTableSchemaNoneOmitsNullFields() {
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("presentField", "value");
    document.put("nullField", null);
    document.put("createdAt", new Date(1738598501924L)); // 2026-02-03T15:31:41.924Z

    TableRow result = MongoDbUtils.getTableSchema(document, "NONE", true);

    assertNotNull(result);
    // source_data is a raw JSON string for the NONE user option.
    String sourceData = (String) result.get("source_data");
    assertNotNull("source_data should be set", sourceData);
    assertTrue("Non-null field should be present", sourceData.contains("presentField"));
    assertFalse("Null field should be omitted", sourceData.contains("nullField"));
    // Date fields must still serialize as ISO-8601 Extended JSON alongside null omission.
    assertTrue("Date should remain ISO-8601 $date", sourceData.contains("\"$date\""));
  }

  @Test
  public void testGetTableSchemaNoneLegacyDefaultDoesNotUseExtendedJson() {
    // With the flag disabled (the default), serialization stays on the previous Gson path, which
    // does not emit Extended JSON $date markers. This guards the non-breaking default.
    Document document = new Document();
    document.put("_id", "test-id");
    document.put("createdAt", new Date(1738598501924L));

    TableRow result = MongoDbUtils.getTableSchema(document, "NONE");

    String sourceData = (String) result.get("source_data");
    assertNotNull("source_data should be set", sourceData);
    assertFalse("Legacy output must not contain Extended JSON $date", sourceData.contains("$date"));
  }
}
