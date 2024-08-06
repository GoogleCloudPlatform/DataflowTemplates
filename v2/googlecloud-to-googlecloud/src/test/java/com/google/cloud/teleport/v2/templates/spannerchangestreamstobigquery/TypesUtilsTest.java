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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.TypesUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TypesUtilsTest {

  @Test
  public void testExtractArrayTypeFromTypeCode() {
    // Eg : "{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}" -> ARRAY<STRING>
    JSONObject jsonObject =
        new JSONObject("{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("ARRAY<STRING>");
    jsonObject = new JSONObject("{\"array_element_type\":{\"code\":\"JSON\"},\"code\":\"ARRAY\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("ARRAY<JSON>");
    jsonObject = new JSONObject("{\"array_element_type\":{\"code\":\"INT64\"},\"code\":\"ARRAY\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("ARRAY<INT64>");
  }

  @Test
  public void testExtractTypeFromTypeCode() {
    // Eg : "{\"code\":\"STRING\"}" -> STRING
    JSONObject jsonObject = new JSONObject("{\"code\":\"STRING\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("STRING");
    jsonObject = new JSONObject("{\"code\":\"INT64\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("INT64");
    jsonObject = new JSONObject("{\"code\":\"BOOL\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("BOOL");
    jsonObject = new JSONObject("{\"code\":\"BYTES\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("BYTES");
    jsonObject = new JSONObject("{\"code\":\"FLOAT64\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("FLOAT64");
    jsonObject = new JSONObject("{\"code\":\"FLOAT32\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("FLOAT32");
    jsonObject = new JSONObject("{\"code\":\"DATE\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("DATE");
    jsonObject = new JSONObject("{\"code\":\"NUMERIC\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("NUMERIC");
    jsonObject = new JSONObject("{\"code\":\"TIMESTAMP\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("TIMESTAMP");
    jsonObject = new JSONObject("{\"code\":\"JSON\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("JSON");
  }

  @Test
  public void testInformationSchemaGoogleSQLTypeToSpannerType() {
    assertThat(TypesUtils.informationSchemaGoogleSQLTypeToSpannerType("ARRAY<STRING(1024)>"))
        .isEqualTo(Type.array(Type.string()));
    assertThat(TypesUtils.informationSchemaGoogleSQLTypeToSpannerType("ARRAY<BYTES(MAX)>"))
        .isEqualTo(Type.array(Type.bytes()));
    assertThat(TypesUtils.informationSchemaGoogleSQLTypeToSpannerType("DATE"))
        .isEqualTo(Type.date());
  }

  @Test
  public void testInformationSchemaPostgreSQLTypeToSpannerType() {
    assertThat(TypesUtils.informationSchemaPostgreSQLTypeToSpannerType("CHARACTER VARYING(256)[]"))
        .isEqualTo(Type.array(Type.string()));
    assertThat(TypesUtils.informationSchemaPostgreSQLTypeToSpannerType("JSONB[]"))
        .isEqualTo(Type.array(Type.pgJsonb()));
    assertThat(TypesUtils.informationSchemaPostgreSQLTypeToSpannerType("real"))
        .isEqualTo(Type.float32());
  }

  @Test
  public void testExtractTypeFromInvalidTypeCode() {
    final JSONObject invalidJsonObject = new JSONObject("{\"type_code\":\"STRING\"}");
    assertThrows(JSONException.class, () -> TypesUtils.extractTypeFromTypeCode(invalidJsonObject));
  }
}
