/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static com.google.cloud.spanner.Type.StructField;
import static com.google.cloud.spanner.Value.bool;
import static com.google.cloud.spanner.Value.boolArray;
import static com.google.cloud.spanner.Value.bytes;
import static com.google.cloud.spanner.Value.bytesArray;
import static com.google.cloud.spanner.Value.date;
import static com.google.cloud.spanner.Value.dateArray;
import static com.google.cloud.spanner.Value.float64;
import static com.google.cloud.spanner.Value.float64Array;
import static com.google.cloud.spanner.Value.int64;
import static com.google.cloud.spanner.Value.int64Array;
import static com.google.cloud.spanner.Value.pgNumeric;
import static com.google.cloud.spanner.Value.pgNumericArray;
import static com.google.cloud.spanner.Value.string;
import static com.google.cloud.spanner.Value.stringArray;
import static com.google.cloud.spanner.Value.struct;
import static com.google.cloud.spanner.Value.structArray;
import static com.google.cloud.spanner.Value.timestamp;
import static com.google.cloud.spanner.Value.timestampArray;
import static com.google.cloud.teleport.v2.utils.SpannerUtils.convertStructToJson;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerUtils} class. */
@RunWith(JUnit4.class)
public final class SpannerUtilsTest {

  @Test
  public void testBooleanStructToJson() {
    Struct testStruct = Struct.newBuilder().set("value").to(bool(true)).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", true);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testInteger64StructToJson() {
    Struct testStruct = Struct.newBuilder().set("value").to(int64(20005)).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", 20005);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testFloat64StructToJson() {
    Struct testStruct = Struct.newBuilder().set("value").to(float64(1234.567)).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", 1234.567);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testStringStructToJson() {
    Struct testStruct = Struct.newBuilder().set("value").to(string("test-string")).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", "test-string");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testPGNumericStructToJson() {
    Struct testStruct =
        Struct.newBuilder().set("value").to(pgNumeric("2147483645.1234567")).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", "2147483645.1234567");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testBytesStructToJson() {
    Struct testStruct =
        Struct.newBuilder().set("value").to(bytes(ByteArray.copyFrom("test"))).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", "test");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testDateStructToJson() {
    Struct testStruct =
        Struct.newBuilder().set("value").to(date(Date.fromYearMonthDay(2020, 3, 15))).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", "2020-03-15");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testTimestampStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(timestamp(Timestamp.ofTimeMicroseconds(10)))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    expected.addProperty("value", "1970-01-01T00:00:00.000010000Z");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void nestedStructToJson() {
    Struct innerStruct = Struct.newBuilder().set("val").to(string("test-string")).build();
    Struct testStruct = Struct.newBuilder().set("value").to(struct(innerStruct)).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject innerObject = new JsonObject();
    innerObject.addProperty("val", "test-string");
    JsonObject expected = new JsonObject();
    expected.add("value", innerObject);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testBooleanArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(boolArray(Collections.singletonList(true)))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(true);
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testInt64ArrayStructToJson() {
    Struct testStruct = Struct.newBuilder().set("value").to(int64Array(new long[] {1})).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(1);
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testFloatArrayStructToJson() {
    Struct testStruct =
        Struct.newBuilder().set("value").to(float64Array(new double[] {0.0})).build();

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(0.0);
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testStringArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(stringArray(Collections.singletonList("test-string")))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("test-string");
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testPGNumericArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(pgNumericArray(Collections.singletonList("2147483645.123456")))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("2147483645.123456");
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testBytesArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(bytesArray(Collections.singletonList(ByteArray.copyFrom("test"))))
            .build();
    //spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("test");
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testDateArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(dateArray(Collections.singletonList(Date.fromYearMonthDay(2020, 3, 15))))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("2020-03-15");
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testTimestampArrayStructToJson() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(timestampArray(Collections.singletonList(Timestamp.ofTimeMicroseconds(10))))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("1970-01-01T00:00:00.000010000Z");
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testStructArrayStructToJson() {
    Struct innerStruct = Struct.newBuilder().set("val").to(string("test-string")).build();
    StructField structField = StructField.of("val", Type.string());
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("value").to(structArray(Type.struct(structField), Collections.singletonList(innerStruct)))
            .build();
    // spotless:on

    JsonObject actual = convertStructToJson(testStruct);

    JsonObject expected = new JsonObject();
    JsonObject structObject = new JsonObject();
    structObject.addProperty("val", "test-string");
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(structObject);
    expected.add("value", jsonArray);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMultipleFieldsStructToJson() {
    // arrange
    Struct innerStruct = Struct.newBuilder().set("val").to(string("test-string")).build();
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("col1").to(bool(true))
            .set("col2").to(string("test-string"))
            .set("col3").to(float64(1.0))
            .set("col4").to(stringArray(Collections.singletonList("test-string")))
            .set("col5").to(struct(innerStruct))
            .build();
    //spotless:on

    // act
    JsonObject actual = convertStructToJson(testStruct);

    // assert
    JsonObject expected = new JsonObject();
    JsonObject innerObject = new JsonObject();
    innerObject.addProperty("val", "test-string");
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("test-string");
    expected.addProperty("col1", true);
    expected.addProperty("col2", "test-string");
    expected.addProperty("col3", 1.0);
    expected.add("col4", jsonArray);
    expected.add("col5", innerObject);
    assertThat(actual).isEqualTo(expected);
  }
}
