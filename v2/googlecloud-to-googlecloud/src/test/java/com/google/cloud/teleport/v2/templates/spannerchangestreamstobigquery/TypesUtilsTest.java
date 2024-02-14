package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.TypesUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TypesUtilsTest {

  @Test
  public void testExtractTypeFromTypeCode() {
    // Eg 1: "{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}" -> ARRAY<STRING>
    JSONObject jsonObject =
        new JSONObject("{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("ARRAY<STRING>");
    // Eg 2: "{\"code\":\"STRING\"}" -> STRING
    jsonObject = new JSONObject("{\"code\":\"STRING\"}");
    assertThat(TypesUtils.extractTypeFromTypeCode(jsonObject)).isEqualTo("STRING");
    final JSONObject invalidJsonObject = new JSONObject("{\"CODE\":\"STRING\"}");
    assertThrows(JSONException.class, () -> TypesUtils.extractTypeFromTypeCode(invalidJsonObject));
  }
}
