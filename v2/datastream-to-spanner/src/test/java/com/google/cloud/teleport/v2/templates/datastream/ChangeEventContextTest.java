/*
 * Copyright (C) 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import java.io.IOException;
import java.util.Collections;
import org.json.JSONObject;
import org.junit.Test;

public class ChangeEventContextTest {

  private static class TestChangeEventContext extends ChangeEventContext {
    public TestChangeEventContext(JsonNode changeEvent, Ddl ddl, String shadowTablePrefix)
        throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
      super(changeEvent, ddl, Collections.emptyMap());
      this.shadowTablePrefix = shadowTablePrefix;
      convertChangeEventToMutation(ddl, ddl);
    }

    @Override
    Mutation generateShadowTableMutation(Ddl ddl, Ddl shadowDdl)
        throws ChangeEventConvertorException {
      return Mutation.newInsertBuilder("shadow_table").build();
    }
  }

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void testConvertChangeEventToMutation_Insert() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "INSERT");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    Mutation mutation = context.getDataMutation();
    assertEquals("Users", mutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());
  }

  @Test
  public void testConvertChangeEventToMutation_Delete() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "DELETE");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    Mutation mutation = context.getDataMutation();
    assertEquals("Users", mutation.getTable());
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
  }

  @Test
  public void testConvertChangeEventToMutation_DeleteWithGeneratedPK() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .string()
            .max()
            .generatedAs("uuid()")
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "DELETE");
    changeEvent.put("id", "123");
    changeEvent.put("name", "Test");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    // Mutation should be null for DELETE on table with generated PK
    assertNull(context.getDataMutation());
  }

  @Test
  public void testGetDataDmlStatement_DeleteWithGeneratedPK() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .string()
            .max()
            .generatedAs("uuid()")
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "DELETE");
    changeEvent.put("id", "123");
    changeEvent.put("name", "Test");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    Statement statement = context.getDataDmlStatement(ddl);
    assertNotNull(statement);
    assertEquals("DELETE FROM Users WHERE name = @name", statement.getSql());
    assertEquals("Test", statement.getParameters().get("name").getString());
  }

  @Test
  public void testGetDataDmlStatement_DeleteWithoutGeneratedPK() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "DELETE");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    Statement statement = context.getDataDmlStatement(ddl);
    assertNull(statement);
  }

  @Test
  public void testGetDataDmlStatement_InsertWithGeneratedPK() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .string()
            .max()
            .generatedAs("uuid()")
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "INSERT");
    changeEvent.put("id", "123");
    changeEvent.put("name", "Test");

    ChangeEventContext context =
        new TestChangeEventContext(getJsonNode(changeEvent.toString()), ddl, "shadow_");

    Statement statement = context.getDataDmlStatement(ddl);
    assertNull(statement);
  }
}
