/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.Parameter;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.ParameterMode;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.ParameterStructType;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.ParameterType;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.ParameterValue;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.createScalarParameter;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.createScalarParameterType;
import static com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.createScalarParameterValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.zetasql.SqlException;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link ParameterTranslator}. */
public final class ParameterTranslatorTest {
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void parseJson_emptyArray() throws Exception {
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson("[]");
    assertThat(parameters, empty());
  }

  @Test
  public void parseJson_namedString() throws Exception {
    String json =
        "[{\"parameterType\": {\"type\": \"STRING\"}, "
            + "\"parameterValue\": {\"value\": \"coffee☕\"}, "
            + "\"name\": \"饮料\"}]";
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson(json);
    assertThat(parameters, contains(createScalarParameter("饮料", "STRING", "coffee☕")));
  }

  @Test
  public void parseJson_unnamedString() throws Exception {
    String json =
        "[{\"parameterType\": {\"type\": \"STRING\"}, "
            + "\"parameterValue\": {\"value\": \"drama🦙\"}}]";
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson(json);
    assertThat(parameters, contains(createScalarParameter("", "STRING", "drama🦙")));
  }

  @Test
  public void parseJson_namedInt64() throws Exception {
    String json =
        "[{\"parameterType\": {\"type\": \"INT64\"}, "
            + "\"parameterValue\": {\"value\": \"24601\"}, "
            + "\"name\": \"id\"}]";
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson(json);
    assertThat(parameters, contains(createScalarParameter("id", "INT64", "24601")));
  }

  @Test
  public void parseJson_namedArray() throws Exception {
    String json =
        "[{\"parameterType\": {\"type\": \"ARRAY\", \"arrayType\": {\"type\": \"STRING\"}}, "
            + "\"parameterValue\": {\"arrayValues\": [{\"value\": \"IN\"}, {\"value\": \"CA\"}]}, "
            + "\"name\": \"states\"}]";
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson(json);

    ParameterType expectedType =
        ParameterType.create("ARRAY", createScalarParameterType("STRING"), null);
    ParameterValue expectedValue =
        ParameterValue.create(
            null,
            ImmutableList.of(createScalarParameterValue("IN"), createScalarParameterValue("CA")),
            null);
    Parameter expected = Parameter.create("states", expectedType, expectedValue);
    assertThat(parameters, contains(expected));
  }

  @Test
  public void parseJson_namedStruct() throws Exception {
    String json =
        "[{\"parameterType\": {\"structTypes\": "
            + "[{\"type\": {\"type\": \"INT64\"}, \"name\": \"x\"}, "
            + "{\"type\": {\"type\": \"STRING\"}, \"name\": \"y\"}], "
            + "\"type\": \"STRUCT\"}, "
            + "\"parameterValue\": {\"structValues\": "
            + "{\"y\": {\"value\": \"foo\"}, \"x\": {\"value\": 1}}}, "
            + "\"name\": \"struct_value\"}]";
    List<ParameterTranslator.Parameter> parameters = ParameterTranslator.parseJson(json);

    List<ParameterStructType> expectedStructTypes =
        ImmutableList.of(
            ParameterStructType.create("x", createScalarParameterType("INT64")),
            ParameterStructType.create("y", createScalarParameterType("STRING")));
    ParameterType expectedType = ParameterType.create("STRUCT", null, expectedStructTypes);
    ParameterValue expectedValue =
        ParameterValue.create(
            null,
            null,
            ImmutableMap.of(
                "x", createScalarParameterValue("1"), "y", createScalarParameterValue("foo")));
    Parameter expected = Parameter.create("struct_value", expectedType, expectedValue);
    assertThat(parameters, contains(expected));
  }

  @Test
  public void inferParameterMode_empty_returnsNone() throws Exception {
    List<Parameter> parameters = Collections.emptyList();
    ParameterMode mode = ParameterTranslator.inferParameterMode(parameters);
    assertThat(mode, equalTo(ParameterMode.NONE));
  }

  @Test
  public void inferParameterMode_allUnnamed_returnsPositional() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("", "INT64", "1"),
            createScalarParameter("", "INT64", "2"),
            createScalarParameter("", "INT64", "3"));
    ParameterMode mode = ParameterTranslator.inferParameterMode(parameters);
    assertThat(mode, equalTo(ParameterMode.POSITIONAL));
  }

  @Test
  public void inferParameterMode_allNamed_returnsNamed() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("foo", "INT64", "1"),
            createScalarParameter("bar", "INT64", "2"),
            createScalarParameter("baz", "INT64", "3"));
    ParameterMode mode = ParameterTranslator.inferParameterMode(parameters);
    assertThat(mode, equalTo(ParameterMode.NAMED));
  }

  @Test
  public void inferParameterMode_someNamed_throws() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("foo", "INT64", "1"),
            createScalarParameter("", "INT64", "2"),
            createScalarParameter("baz", "INT64", "3"));
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(containsString("name"));
    ParameterTranslator.inferParameterMode(parameters);
  }

  @Test
  public void translateNamedParameters_emptyName_throws() throws Exception {
    List<Parameter> parameters = ImmutableList.of(createScalarParameter("", "INT64", "23"));
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(containsString("name"));
    ParameterTranslator.translateNamedParameters(parameters);
  }

  @Test
  public void translateNamedParameters_unknownType_throws() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(createScalarParameter("foo", "BAD TYPE", "value"));
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(containsString("type"));
    ParameterTranslator.translateNamedParameters(parameters);
  }

  @Test
  public void translateNamedParameters_string() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("ascii", "STRING", "abc\n123"),
            createScalarParameter("utf_8", "STRING", "‾\\_(ツ)_/‾"));
    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(2));
    assertThat(translated, hasEntry("ascii", Value.createStringValue("abc\n123")));
    assertThat(translated, hasEntry("utf_8", Value.createStringValue("‾\\_(ツ)_/‾")));
  }

  @Test
  public void translatePositionalParameters_string() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("", "STRING", "abc"), createScalarParameter("", "STRING", "123"));
    List<Value> translated = ParameterTranslator.translatePositionalParameters(parameters);
    assertThat(
        translated, contains(Value.createStringValue("abc"), Value.createStringValue("123")));
  }

  @Test
  public void translateNamedParameters_int64() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("zero", "INT64", "0"),
            createScalarParameter("min", "INT64", "-9223372036854775808"),
            createScalarParameter("max", "INT64", "9223372036854775807"));
    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(3));
    assertThat(translated, hasEntry("zero", Value.createInt64Value(0L)));
    assertThat(translated, hasEntry("min", Value.createInt64Value(-9_223_372_036_854_775_808L)));
    assertThat(translated, hasEntry("max", Value.createInt64Value(9_223_372_036_854_775_807L)));
  }

  @Test
  public void translateNamedParameters_float64() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("zero", "FLOAT64", "0"),
            createScalarParameter("pi", "FLOAT64", "3.14159"),
            createScalarParameter("negative_pi", "FLOAT64", "-3.14159"),
            createScalarParameter("scientific", "FLOAT64", "1e9"));
    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(4));
    assertThat(translated, hasEntry("zero", Value.createDoubleValue(0)));
    assertThat(translated, hasEntry("pi", Value.createDoubleValue(3.14159)));
    assertThat(translated, hasEntry("negative_pi", Value.createDoubleValue(-3.14159)));
    assertThat(translated, hasEntry("scientific", Value.createDoubleValue(1e9)));
  }

  @Test
  public void translateNamedParameters_bool() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("true_upper", "BOOL", "TRUE"),
            createScalarParameter("true_lower", "BOOL", "true"),
            createScalarParameter("false_upper", "BOOL", "FALSE"),
            createScalarParameter("false_lower", "BOOL", "false"));
    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(4));
    assertThat(translated, hasEntry("true_upper", Value.createBoolValue(true)));
    assertThat(translated, hasEntry("true_lower", Value.createBoolValue(true)));
    assertThat(translated, hasEntry("false_upper", Value.createBoolValue(false)));
    assertThat(translated, hasEntry("false_lower", Value.createBoolValue(false)));
  }

  @Test
  public void translateNamedParameters_invalidBool_throws() throws Exception {
    List<Parameter> parameters = ImmutableList.of(createScalarParameter("foo", "BOOL", "banana"));
    exception.expect(SqlException.class);
    exception.expectMessage(containsString("banana"));
    ParameterTranslator.translateNamedParameters(parameters);
  }

  @Test
  public void translateNamedParameters_bytes() throws Exception {
    List<Parameter> parameters =
        ImmutableList.of(
            createScalarParameter("ascii", "BYTES", "abc\n123"),
            createScalarParameter("utf_8", "BYTES", "‾\\_(ツ)_/‾"));
    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(2));
    assertThat(
        translated, hasEntry("ascii", Value.createBytesValue(ByteString.copyFromUtf8("abc\n123"))));
    assertThat(
        translated,
        hasEntry("utf_8", Value.createBytesValue(ByteString.copyFromUtf8("‾\\_(ツ)_/‾"))));
  }

  @Test
  public void translateNamedParameters_array() throws Exception {
    ParameterType parameterType =
        ParameterType.create("ARRAY", createScalarParameterType("STRING"), null);
    ParameterValue parameterValue =
        ParameterValue.create(
            null,
            ImmutableList.of(createScalarParameterValue("IN"), createScalarParameterValue("CA")),
            null);
    Parameter parameter = Parameter.create("states", parameterType, parameterValue);
    List<Parameter> parameters = ImmutableList.of(parameter);

    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(1));
    Value expected =
        Value.createArrayValue(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
            ImmutableList.of(Value.createStringValue("IN"), Value.createStringValue("CA")));
    assertThat(translated, hasEntry("states", expected));
  }

  @Test
  public void translateNamedParameters_struct() throws Exception {
    List<ParameterStructType> structTypes =
        ImmutableList.of(
            ParameterStructType.create("x", createScalarParameterType("INT64")),
            ParameterStructType.create("y", createScalarParameterType("STRING")));
    ParameterType parameterType = ParameterType.create("STRUCT", null, structTypes);
    ParameterValue parameterValue =
        ParameterValue.create(
            null,
            null,
            ImmutableMap.of(
                "x", createScalarParameterValue("1"), "y", createScalarParameterValue("foo")));
    Parameter parameter = Parameter.create("struct_value", parameterType, parameterValue);
    List<Parameter> parameters = ImmutableList.of(parameter);

    Map<String, Value> translated = ParameterTranslator.translateNamedParameters(parameters);
    assertThat(translated.size(), equalTo(1));
    StructType expectedType =
        TypeFactory.createStructType(
            ImmutableList.of(
                new StructField("x", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new StructField("y", TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    ImmutableList<Value> expectedValues =
        ImmutableList.of(Value.createInt64Value(1), Value.createStringValue("foo"));
    Value expected = Value.createStructValue(expectedType, expectedValues);
    assertThat(translated, hasEntry("struct_value", expected));
  }

  @Test
  public void translateNamedParameters_structMissingField_throws() throws Exception {
    List<ParameterStructType> structTypes =
        ImmutableList.of(
            ParameterStructType.create("x", createScalarParameterType("INT64")),
            ParameterStructType.create("missing_field_name", createScalarParameterType("STRING")));
    ParameterType parameterType = ParameterType.create("STRUCT", null, structTypes);
    ParameterValue parameterValue =
        ParameterValue.create(null, null, ImmutableMap.of("x", createScalarParameterValue("1")));
    Parameter parameter = Parameter.create("struct_value", parameterType, parameterValue);
    List<Parameter> parameters = ImmutableList.of(parameter);

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(containsString("missing_field_name"));
    ParameterTranslator.translateNamedParameters(parameters);
  }
}
