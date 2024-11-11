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
package com.google.cloud.teleport.v2.templates.source.common;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class DMLGeneratorRequestTest {
  private String modType;
  private String spannerTableName;
  private Schema schema;
  private JSONObject newValuesJson;
  private JSONObject keyValuesJson;
  private String sourceDbTimezoneOffset;

  @Before
  public void setUp() {
    // Initialize mock data
    modType = "INSERT";
    spannerTableName = "my_table";
    schema = mock(Schema.class);
    newValuesJson = new JSONObject();
    newValuesJson.put("column1", "value1");
    keyValuesJson = new JSONObject();
    keyValuesJson.put("column1", "key1");
    sourceDbTimezoneOffset = "+00:00";
  }

  @Test
  public void testConstructorAndGetters() {
    DMLGeneratorRequest request =
        new DMLGeneratorRequest(
            modType,
            spannerTableName,
            schema,
            newValuesJson,
            keyValuesJson,
            sourceDbTimezoneOffset);

    assertEquals(modType, request.getModType());
    assertEquals(spannerTableName, request.getSpannerTableName());
    assertEquals(schema, request.getSchema());
    assertEquals(newValuesJson.toString(), request.getNewValuesJson().toString());
    assertEquals(keyValuesJson.toString(), request.getKeyValuesJson().toString());
    assertEquals(sourceDbTimezoneOffset, request.getSourceDbTimezoneOffset());
  }
}
