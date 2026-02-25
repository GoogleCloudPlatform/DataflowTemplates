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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
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
}
