/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link TextToSpannerTest}. */
@RunWith(JUnit4.class)
public class TextToSpannerTest {
  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  private final String projectID = "google.com:deft-testing-integration";
  private final String spannerInstanceName = "teleport-test";
  private final String spannerDBName = "text_to_spanner_unittest";
  private final String spannerTblName = "text_to_spanner_test";
  private final String expected =
      "IATA:STRING,AIRPORT:STRING,CITY:STRING,STATE:STRING,COUNTRY:STRING"
          + ",LATITUDE:FLOAT64,LONGITUDE:FLOAT64";
  private final String schemaFile =
      "gs://cloud-teleport-testing/test_text_to_spanner/spanner_schema";
  private SpannerSchema testSchema = new SpannerSchema();
  private Map<String, String> testRow = new HashMap<String, String>();
  private final int expectedNumOfColumns = 6;
  private TextToSpanner textSpannerPipeline = new TextToSpanner();
  private final TextToSpanner.RowToMutationFn rowToMutationFn =
      new TextToSpanner.RowToMutationFn(
          StaticValueProvider.of(projectID),
          StaticValueProvider.of(spannerInstanceName),
          StaticValueProvider.of(spannerDBName),
          StaticValueProvider.of(spannerTblName),
          StaticValueProvider.of(schemaFile),
          StaticValueProvider.of(','),
          StaticValueProvider.of('"'));

  @Before
  public void setup() {
    testSchema.addEntry("col1", "INT64");
    testSchema.addEntry("col2", "FLOAT64");
    testSchema.addEntry("col3", "STRING(MAX)");
    testSchema.addEntry("col4", "BOOL");
    testSchema.addEntry("col5", "DATE");
    testSchema.addEntry("col6", "TIMESTAMP");

    testRow.put("col1", "1");
    testRow.put("col2", "123.456");
    testRow.put("col3", "bonjovi");
    testRow.put("col4", "True");
    testRow.put("col5", "2018-01-01");
    testRow.put("col6", "2018-01-01T12:30:00Z");
  }

  // Read a sample table from Cloud Spanner to test the schema reading part.
  @Ignore
  @Test
  public void testReadSchemaFromSpanner() {
    SpannerSchema spannerSchema =
        rowToMutationFn.getSpannerSchemaFromSpanner(
            spannerInstanceName, spannerDBName, spannerTblName);

    // Test number of columns found
    assertEquals(expectedNumOfColumns, spannerSchema.spannerSchema.size());

    // Test for all column names and types are correct
    for (int i = 0; i < spannerSchema.spannerSchema.size(); i++) {
      String key = testSchema.getColumnList().get(i);
      SpannerDataTypes type = testSchema.getColumnType(key);
      assertTrue(testSchema.spannerSchema.containsKey(key));
      assertTrue(testSchema.spannerSchema.containsValue(type));
    }
  }

  // Test RowToMutation conversion works with standard CSV content with some spaces
  @Ignore
  @Test
  public void testRowToMutationStandardCSV() throws Exception {
    String testRow = "1,123.456, bonjovi, True, 2018-01-01, 2018-01-01T12:30:00Z";
    Map<String, String> testRowMap = new HashMap<String, String>();
    testRowMap.put("col1", "1");
    testRowMap.put("col2", "123.456");
    testRowMap.put("col3", "bonjovi");
    testRowMap.put("col4", "True");
    testRowMap.put("col5", "2018-01-01");
    testRowMap.put("col6", "2018-01-01T12:30:00Z");

    DoFnTester tester = DoFnTester.of(rowToMutationFn);
    List<Mutation> results = tester.processBundle(testRow);
    validateBundleResult(results, 1, testSchema, testRowMap);
  }

  // Test RowToMutation conversion works with standard CSV content with field qualifier
  @Ignore
  @Test
  public void testRowToMutationDoubleQuote() throws Exception {
    String testRow =
        "\"1\",\"123.456\",\" bonjovi\",\"True\"," + "\" 2018-01-01\", \"2018-01-01T12:30:00Z\"";
    Map<String, String> testRowMap = new HashMap<String, String>();
    testRowMap.put("col1", "1");
    testRowMap.put("col2", "123.456");
    testRowMap.put("col3", "bonjovi");
    testRowMap.put("col4", "True");
    testRowMap.put("col5", "2018-01-01");
    testRowMap.put("col6", "2018-01-01T12:30:00Z");

    DoFnTester tester = DoFnTester.of(rowToMutationFn);
    List<Mutation> results = tester.processBundle(testRow);
    validateBundleResult(results, 1, testSchema, testRowMap);
  }

  // Test valid data types
  @Test
  public void testValidateDataTypeSupportValid() {
    SpannerSchema testSchema = new SpannerSchema();
    testSchema.addEntry("col1", "INT64");
    testSchema.addEntry("col2", "FLOAT64");
    testSchema.addEntry("col3", "STRING(MAX)");
    testSchema.addEntry("col4", "BOOL");
    testSchema.addEntry("col5", "DATE");
    testSchema.addEntry("col6", "TIMESTAMP");
  }

  // Test unsupported data types
  @Test(expected = IllegalArgumentException.class)
  public void testValidateDataTypeSupportUnsupport() {
    SpannerSchema testSchema = new SpannerSchema();
    testSchema.addEntry("col1", "ARRAY");
    testSchema.addEntry("col2", "BYTES");
    testSchema.addEntry("col3", "STRUCT");
    testSchema.addEntry("col4", "INVALIDDATA");
  }

  // Helper method of validating content of result set
  private void validateBundleResult(
      List<Mutation> results,
      int expectedNumMutations,
      SpannerSchema testSchema,
      Map<String, String> testRow) {
    // Test result row count = 1
    assertEquals(results.size(), 1);
    Mutation result = results.get(0);
    Map<String, Value> resultMap = result.asMap();
    String key = null;
    SpannerDataTypes type = null;
    for (int i = 0; i < testSchema.spannerSchema.size(); i++) {
      key = testSchema.getColumnList().get(i);
      type = testSchema.getColumnType(key);
      assertTrue(resultMap.containsKey(key));
      switch (type) {
        case DATE:
          assertEquals(testRow.get(key), resultMap.get(key).getDate().toString());
          break;
        case FLOAT64:
          assertEquals(testRow.get(key), String.format("%.3f", resultMap.get(key).getFloat64()));
          break;
        case STRING:
          assertEquals(testRow.get(key), resultMap.get(key).getString());
          break;
        case INT64:
          assertEquals(testRow.get(key), String.valueOf(resultMap.get(key).getInt64()));
          break;
        case BOOL:
          assertTrue(resultMap.get(key).getBool());
          break;
        case TIMESTAMP:
          assertEquals(testRow.get(key), resultMap.get(key).getTimestamp().toString());
          break;
      }
    }
  }
}
