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
package com.google.cloud.teleport.v2.templates.utils;

import static com.google.common.truth.Truth.assertThat;

import java.nio.ByteBuffer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FailureRecord}. */
@RunWith(JUnit4.class)
public class FailureRecordTest {

  @Test
  public void toJson_topLevelKeys() {
    String json = FailureRecord.toJson("Users", "INSERT", null, new RuntimeException("boom"));
    JSONObject obj = new JSONObject(json);
    assertThat(obj.keySet()).containsExactly("timestamp", "table", "operation", "row", "error");
    assertThat(obj.getString("table")).isEqualTo("Users");
    assertThat(obj.getString("operation")).isEqualTo("INSERT");
  }

  @Test
  public void toJson_nullRowFlowsThroughAsJsonNull() {
    String json = FailureRecord.toJson("Users", "INSERT", null, new RuntimeException("err"));
    JSONObject obj = new JSONObject(json);
    assertThat(obj.isNull("row")).isTrue();
  }

  @Test
  public void toJson_nullTableAndOperation() {
    String json = FailureRecord.toJson(null, null, null, new RuntimeException("err"));
    JSONObject obj = new JSONObject(json);
    assertThat(obj.isNull("table")).isTrue();
    assertThat(obj.isNull("operation")).isTrue();
  }

  @Test
  public void toJson_primitiveRowFields() {
    Schema schema =
        Schema.builder()
            .addStringField("name")
            .addInt64Field("age")
            .addBooleanField("active")
            .build();
    Row row = Row.withSchema(schema).addValues("Alice", 30L, true).build();

    JSONObject obj =
        new JSONObject(FailureRecord.toJson("T", "UPDATE", row, new RuntimeException("e")));
    JSONObject rowJson = obj.getJSONObject("row");
    assertThat(rowJson.getString("name")).isEqualTo("Alice");
    assertThat(rowJson.getLong("age")).isEqualTo(30L);
    assertThat(rowJson.getBoolean("active")).isTrue();
  }

  @Test
  public void toJson_byteArrayFieldBase64Encoded() {
    Schema schema = Schema.builder().addByteArrayField("payload").build();
    Row row = Row.withSchema(schema).addValue(new byte[] {0x00, 0x0a, (byte) 0xff}).build();

    JSONObject obj =
        new JSONObject(FailureRecord.toJson("T", "INSERT", row, new RuntimeException("e")));
    String base64 = obj.getJSONObject("row").getString("payload");
    assertThat(base64).isEqualTo("AAr/");
  }

  @Test
  public void toJson_byteBufferFieldBase64Encoded() {
    Schema schema = Schema.builder().addField("payload", Schema.FieldType.BYTES).build();
    Row row = Row.withSchema(schema).addValue(ByteBuffer.wrap(new byte[] {1, 2, 3})).build();

    JSONObject obj =
        new JSONObject(FailureRecord.toJson("T", "INSERT", row, new RuntimeException("e")));
    String base64 = obj.getJSONObject("row").getString("payload");
    assertThat(base64).isEqualTo("AQID");
  }

  @Test
  public void toJson_nullField() {
    Schema schema = Schema.builder().addNullableField("name", Schema.FieldType.STRING).build();
    Row row = Row.withSchema(schema).addValue(null).build();

    JSONObject obj =
        new JSONObject(FailureRecord.toJson("T", "INSERT", row, new RuntimeException("e")));
    assertThat(obj.getJSONObject("row").isNull("name")).isTrue();
  }

  @Test
  public void toJson_emptyRow() {
    Schema schema = Schema.builder().build();
    Row row = Row.withSchema(schema).build();

    JSONObject obj =
        new JSONObject(FailureRecord.toJson("T", "INSERT", row, new RuntimeException("e")));
    JSONObject rowJson = obj.getJSONObject("row");
    assertThat(rowJson.length()).isEqualTo(0);
  }

  @Test
  public void toJson_errorIncludesClassMessageAndStack() {
    RuntimeException ex = new RuntimeException("something failed");
    JSONObject obj = new JSONObject(FailureRecord.toJson("T", "INSERT", null, ex));
    JSONObject err = obj.getJSONObject("error");
    assertThat(err.getString("class")).isEqualTo("java.lang.RuntimeException");
    assertThat(err.getString("message")).isEqualTo("something failed");
    assertThat(err.getString("stackTrace")).contains("FailureRecordTest");
  }

  @Test
  public void toJson_errorWithNullMessageRendersEmptyString() {
    RuntimeException ex = new RuntimeException();
    JSONObject obj = new JSONObject(FailureRecord.toJson("T", "INSERT", null, ex));
    assertThat(obj.getJSONObject("error").getString("message")).isEmpty();
  }

  @Test
  public void toJson_errorWithCausePropagatesStackTrace() {
    Throwable cause = new IllegalStateException("inner");
    RuntimeException ex = new RuntimeException("outer", cause);
    JSONObject obj = new JSONObject(FailureRecord.toJson("T", "INSERT", null, ex));
    String stack = obj.getJSONObject("error").getString("stackTrace");
    assertThat(stack).contains("Caused by");
    assertThat(stack).contains("IllegalStateException");
  }

  @Test
  public void toJson_generationOperation() {
    JSONObject obj =
        new JSONObject(
            FailureRecord.toJson(
                "T", FailureRecord.OPERATION_GENERATION, null, new RuntimeException("e")));
    assertThat(obj.getString("operation")).isEqualTo("GENERATION");
  }
}
