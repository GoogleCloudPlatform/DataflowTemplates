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
package com.google.cloud.teleport.v2.templates.datastream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ChangeEventContextFactoryTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void testCreateChangeEventContextWithEmptySourceType() throws Exception {
    expectedEx.expect(InvalidChangeEventException.class);
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
  }

  @Test
  public void testCreateChangeEventContextWithNonMatchingSourceType() throws Exception {
    expectedEx.expect(InvalidChangeEventException.class);
    expectedEx.expectMessage("Change event with invalid source");
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.ORACLE_SOURCE_TYPE);
    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
  }

  @Test
  public void testCreateChangeEventContextWithNotSupportedSource() throws Exception {
    expectedEx.expect(InvalidChangeEventException.class);
    expectedEx.expectMessage("Unsupported source database");
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "xyz");
    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()), ddl, "shadow_", "xyz");
  }
}
