/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.utils.BeamSchemaUtils.SchemaParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link BeamSchemaUtils}.
 */
public class BeamSchemaUtilsTest {

  @Rule
  public final ExpectedException exceptionRule = ExpectedException.none();
  static final String EXPECTED_FIELD_NAME = "expectedFieldName";
  static ObjectMapper mapper;
  public static final String JSON_SCHEMA = "[\n"
      + "  {\n"
      + "    \"name\": \"byte\",\n"
      + "    \"type\": \"BYTE\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"int16\",\n"
      + "    \"type\": \"INT16\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"int32\",\n"
      + "    \"type\": \"INT32\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"int64\",\n"
      + "    \"type\": \"INT64\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"decimal\",\n"
      + "    \"type\": \"DECIMAL\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"float\",\n"
      + "    \"type\": \"FLOAT\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"double\",\n"
      + "    \"type\": \"DOUBLE\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"string\",\n"
      + "    \"type\": \"STRING\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"datetime\",\n"
      + "    \"type\": \"DATETIME\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"boolean\",\n"
      + "    \"type\": \"BOOLEAN\"\n"
      + "  },\n"
      + "  {\n"
      + "    \"name\": \"bytes\",\n"
      + "    \"type\": \"BYTES\"\n"
      + "  }\n"
      + "]\n";

  @BeforeClass
  public static void before() {
    mapper = new ObjectMapper();
  }


  @Test
  public void testFromJson() throws SchemaParseException, IOException {
    Schema schema = BeamSchemaUtils.fromJson(JSON_SCHEMA);
    assertEquals(11, schema.getFieldCount());

    assertEquals(0, schema.indexOf("byte"));
    assertEquals("byte", schema.getField(0).getName());
    assertEquals(FieldType.BYTE, schema.getField(0).getType());

    assertEquals(1, schema.indexOf("int16"));
    assertEquals("int16", schema.getField(1).getName());
    assertEquals(FieldType.INT16, schema.getField(1).getType());

    assertEquals(2, schema.indexOf("int32"));
    assertEquals("int32", schema.getField(2).getName());
    assertEquals(FieldType.INT32, schema.getField(2).getType());

    assertEquals(3, schema.indexOf("int64"));
    assertEquals("int64", schema.getField(3).getName());
    assertEquals(FieldType.INT64, schema.getField(3).getType());

    assertEquals(4, schema.indexOf("decimal"));
    assertEquals("decimal", schema.getField(4).getName());
    assertEquals(FieldType.DECIMAL, schema.getField(4).getType());

    assertEquals(5, schema.indexOf("float"));
    assertEquals("float", schema.getField(5).getName());
    assertEquals(FieldType.FLOAT, schema.getField(5).getType());

    assertEquals(6, schema.indexOf("double"));
    assertEquals("double", schema.getField(6).getName());
    assertEquals(FieldType.DOUBLE, schema.getField(6).getType());

    assertEquals(7, schema.indexOf("string"));
    assertEquals("string", schema.getField(7).getName());
    assertEquals(FieldType.STRING, schema.getField(7).getType());

    assertEquals(8, schema.indexOf("datetime"));
    assertEquals("datetime", schema.getField(8).getName());
    assertEquals(FieldType.DATETIME, schema.getField(8).getType());

    assertEquals(9, schema.indexOf("boolean"));
    assertEquals("boolean", schema.getField(9).getName());
    assertEquals(FieldType.BOOLEAN, schema.getField(9).getType());

    assertEquals(10, schema.indexOf("bytes"));
    assertEquals("bytes", schema.getField(10).getName());
    assertEquals(FieldType.BYTES, schema.getField(10).getType());
  }

  @Test
  public void testMissedField() throws SchemaParseException, IOException {
    exceptionRule.expect(NullPointerException.class);
    exceptionRule.expectMessage("type is missed: {\"name\":\"testName\"}");
    BeamSchemaUtils.fromJson("[{\"name\": \"testName\"}]");

    exceptionRule.expect(NullPointerException.class);
    exceptionRule.expectMessage("name is missed: {\"type\":\"testType\"}");
    BeamSchemaUtils.fromJson("[{\"type\": \"testType\"}]");
  }

  @Test
  public void testInvalidFormat() throws SchemaParseException, IOException {
    exceptionRule.expect(SchemaParseException.class);
    exceptionRule.expectMessage(
        "Provided schema must be in \"[{\"type\": \"fieldTypeName\", \"name\": \"fieldName\"}, ...]\" format");
    BeamSchemaUtils.fromJson("{\"name\": \"bytes\",\"type\": \"BYTES\"}");
  }

  @Test
  public void testInvalidType() throws SchemaParseException, IOException {
    exceptionRule.expect(SchemaParseException.class);
    exceptionRule.expectMessage("Provided type \"INVALID\" does not exist");
    BeamSchemaUtils.fromJson("[{\"name\": \"bytes\",\"type\": \"INVALID\"}]");
  }

  @Test
  public void testInvalidNodeFormat() throws SchemaParseException, IOException {
    exceptionRule.expect(SchemaParseException.class);
    exceptionRule.expectMessage("Node must be object: [{\"name\":\"bytes\",\"type\":\"BYTES\"}]");
    BeamSchemaUtils.fromJson("[[{\"name\": \"bytes\",\"type\": \"BYTES\"}]]");
  }


  @Test
  public void givenBeamSchema_whenBYTEField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BYTE.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BYTE).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenINT16Field_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT16.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT16).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenINT32Field_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT32.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT32).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenINT64Field_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT64.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT64).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenDECIMALField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DECIMAL.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DECIMAL).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenFLOATField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.FLOAT.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.FLOAT).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenDOUBLEField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DOUBLE.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DOUBLE).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenSTRINGField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.STRING.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.STRING).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenDATETIMEField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DATETIME.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DATETIME).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenBOOLEANField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BOOLEAN.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BOOLEAN).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenBYTESField_thenJsonString() throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BYTES.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BYTES).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }


  @Test
  public void givenBeamSchema_whenAllFieldTypes_thenCorrectJsonString()
      throws JsonProcessingException {
    List<FieldType> fieldTypes = new ArrayList<>();
    fieldTypes.add(FieldType.BYTE);
    fieldTypes.add(FieldType.INT16);
    fieldTypes.add(FieldType.INT32);
    fieldTypes.add(FieldType.INT64);
    fieldTypes.add(FieldType.DECIMAL);
    fieldTypes.add(FieldType.FLOAT);
    fieldTypes.add(FieldType.DOUBLE);
    fieldTypes.add(FieldType.STRING);
    fieldTypes.add(FieldType.DATETIME);
    fieldTypes.add(FieldType.BOOLEAN);
    fieldTypes.add(FieldType.BYTES);

    List<Map<String, Object>> expectedValue = new ArrayList<>();
    Schema.Builder beamSchemaBuilder = Schema.builder();
    for (FieldType fieldType : fieldTypes) {
      String fieldName = EXPECTED_FIELD_NAME + "_" + fieldType.getTypeName().toString();

      Map<String, Object> payload = new HashMap<>();
      payload.put(BeamSchemaUtils.FIELD_NAME, fieldName);
      payload.put(BeamSchemaUtils.FIELD_TYPE, fieldType.getTypeName().toString());
      payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.FALSE);
      expectedValue.add(payload);

      beamSchemaBuilder.addField(fieldName, fieldType);
    }

    String expectedJson = mapper.writeValueAsString(expectedValue);
    Schema beamSchema = beamSchemaBuilder.build();

    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }

  @Test
  public void givenBeamSchema_whenNullableTrue_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BYTES.getTypeName().toString());
    payload.put(BeamSchemaUtils.FIELD_NULLABLE, Boolean.TRUE);
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Field nullableType = Field.of(EXPECTED_FIELD_NAME, FieldType.BYTES).withNullable(true);
    Schema beamSchema = Schema.builder().addField(nullableType).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(mapper.readTree(expectedJson), mapper.readTree(beamSchemaJson));
  }

  @Test
  public void testNullableField() throws IOException, SchemaParseException {
    String jsonString = "[{\"name\": \"nullableField\", \"type\": \"STRING\", \"nullable\": true},{\"name\": \"nonNullableField\", \"type\": \"STRING\", \"nullable\": false},{\"name\": \"defaultNullableValueField\", \"type\": \"STRING\"}]";
    Schema schema = BeamSchemaUtils.fromJson(jsonString);
    assertEquals(3, schema.getFieldCount());
    assertEquals("nullableField", schema.getField(0).getName());
    assertEquals(FieldType.STRING.withNullable(true), schema.getField(0).getType());
    assertEquals("nonNullableField", schema.getField(1).getName());
    assertEquals(FieldType.STRING, schema.getField(1).getType());
    assertEquals("defaultNullableValueField", schema.getField(2).getName());
    assertEquals(FieldType.STRING, schema.getField(2).getType());
  }
}
