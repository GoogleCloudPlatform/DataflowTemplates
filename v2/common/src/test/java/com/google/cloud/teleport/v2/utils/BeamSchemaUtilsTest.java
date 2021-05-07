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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link BeamSchemaUtils}
 */
public class BeamSchemaUtilsTest {

  static final String EXPECTED_FIELD_NAME = "expectedFieldName";
  static ObjectMapper mapper;

  @BeforeClass
  public static void before() {
    mapper = new ObjectMapper();
  }


  @Test
  public void givenBeamSchema_whenBYTEField_thenCorrectJsonString() throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BYTE.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BYTE).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenINT16Field_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT16.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT16).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenINT32Field_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT32.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT32).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenINT64Field_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.INT64.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.INT64).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenDECIMALField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DECIMAL.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DECIMAL).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenFLOATField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.FLOAT.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.FLOAT).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenDOUBLEField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DOUBLE.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DOUBLE).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenSTRINGField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.STRING.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.STRING).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenDATETIMEField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.DATETIME.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.DATETIME).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenBOOLEANField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BOOLEAN.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BOOLEAN).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }


  @Test
  public void givenBeamSchema_whenBYTESField_thenCorrectJsonString()
      throws JsonProcessingException {
    Map<String, String> payload = new HashMap<>();
    payload.put(BeamSchemaUtils.FIELD_NAME, EXPECTED_FIELD_NAME);
    payload.put(BeamSchemaUtils.FIELD_TYPE, FieldType.BYTES.getTypeName().toString());
    String expectedJson = mapper.writeValueAsString(Collections.singletonList(payload));

    Schema beamSchema = Schema.builder().addField(EXPECTED_FIELD_NAME, FieldType.BYTES).build();
    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
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

    List<Map<String, String>> expectedValue = new ArrayList<>();
    Schema.Builder beamSchemaBuilder = Schema.builder();
    for (FieldType fieldType : fieldTypes) {
      String field_name = EXPECTED_FIELD_NAME + "_" + fieldType.getTypeName().toString();

      Map<String, String> payload = new HashMap<>();
      payload.put(BeamSchemaUtils.FIELD_NAME, field_name);
      payload.put(BeamSchemaUtils.FIELD_TYPE, fieldType.getTypeName().toString());
      expectedValue.add(payload);

      beamSchemaBuilder.addField(field_name, fieldType);
    }

    String expectedJson = mapper.writeValueAsString(expectedValue);
    Schema beamSchema = beamSchemaBuilder.build();

    String beamSchemaJson = BeamSchemaUtils.beamSchemaToJson(beamSchema);

    Assert.assertEquals(expectedJson, beamSchemaJson);
  }

}