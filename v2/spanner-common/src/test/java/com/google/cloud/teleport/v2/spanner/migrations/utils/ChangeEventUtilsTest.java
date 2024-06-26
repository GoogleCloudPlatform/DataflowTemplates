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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ChangeEventUtilsTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testGetEventColumnKeys_withValidKeys() throws Exception {
    String jsonString = "{ \"name\": \"John\", \"age\": \"30\", \"_metadata_id\": \"123\" }";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode changeEvent = objectMapper.readTree(jsonString);

    List<String> eventColumnKeys = ChangeEventUtils.getEventColumnKeys(changeEvent);

    assertEquals(2, eventColumnKeys.size());
    assertTrue(eventColumnKeys.contains("name"));
    assertTrue(eventColumnKeys.contains("age"));
  }

  @Test
  public void testGetEventColumnKeys_withoutKeys() throws Exception {
    expectedEx.expect(InvalidChangeEventException.class);
    expectedEx.expectMessage("No data found in Datastream event.");
    String jsonString = "{ \"_metadata_id\": \"123\" }";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode changeEvent = objectMapper.readTree(jsonString);

    List<String> eventColumnKeys = ChangeEventUtils.getEventColumnKeys(changeEvent);

    assertEquals(0, eventColumnKeys.size());
  }
}
