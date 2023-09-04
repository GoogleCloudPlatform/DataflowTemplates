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
package com.google.cloud.teleport.v2.templates.changestream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for DataChangeRecordTypeConvertor class. */
public final class DataChangeRecordTypeConvertorTest {

  public static JsonNode getJsonNode(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      return mapper.readTree(json);
    } catch (IOException e) {
      // No action. Return null.
    }
    return null;
  }

  /*
   * Tests for boolean conversion.
   */
  @Test
  public void canConvertToBoolean() throws Exception {

    // Change Event with all valid forms of boolean values.
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("bool_field1", true);
    changeEvent.put("bool_field2", false);
    changeEvent.put("bool_field3", JSONObject.NULL);

    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toBoolean(ce, "bool_field1", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        DataChangeRecordTypeConvertor.toBoolean(ce, "bool_field2", /* requiredField= */ true),
        new Boolean(false));
    assertNull(
        DataChangeRecordTypeConvertor.toBoolean(ce, "bool_field3", /* requiredField= */ false));
    assertNull(
        DataChangeRecordTypeConvertor.toBoolean(ce, "non_existent", /* requiredField= */ false));
  }

  /*
   * Tests for long conversion.
   */
  @Test
  public void canConvertToLong() throws Exception {

    // Change Event with all valid forms of long values
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 123456789);
    changeEvent.put("field2", -123456789);
    changeEvent.put("field3", JSONObject.NULL);

    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toLong(ce, "field1", /* requiredField= */ true),
        new Long(123456789));
    assertEquals(
        DataChangeRecordTypeConvertor.toLong(ce, "field2", /* requiredField= */ true),
        new Long(-123456789));
    assertNull(DataChangeRecordTypeConvertor.toLong(ce, "field3", /* requiredField= */ false));
  }

  /*
   * Tests for double conversion.
   */
  @Test
  public void canConvertToDouble() throws Exception {

    // Change Event with all valid forms of double values
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 123456789);
    changeEvent.put("field2", -123456789);
    changeEvent.put("field3", 123456.789);
    changeEvent.put("field4", -123456.789);
    changeEvent.put("field5", 123456789.012345678912);
    changeEvent.put("field6", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toDouble(ce, "field1", /* requiredField= */ true),
        new Double(123456789));
    assertEquals(
        DataChangeRecordTypeConvertor.toDouble(ce, "field2", /* requiredField= */ true),
        new Double(-123456789));
    assertEquals(
        DataChangeRecordTypeConvertor.toDouble(ce, "field3", /* requiredField= */ true),
        new Double(123456.789));
    assertEquals(
        DataChangeRecordTypeConvertor.toDouble(ce, "field4", /* requiredField= */ true),
        new Double(-123456.789));
    assertEquals(
        DataChangeRecordTypeConvertor.toDouble(ce, "field5", /* requiredField= */ true),
        new Double(123456789.012345678912));
    assertNull(DataChangeRecordTypeConvertor.toDouble(ce, "field6", /* requiredField= */ false));
  }

  /*
   * Tests for string conversion.  This functions also checks whether long numbers or decimals
   * encoded as JSON strings are numbers can be converted to string.
   */
  @Test
  public void canConvertToString() throws Exception {
    String jsonChangeEvent =
        "{ "
            + "\"field1\" : \";asidjf987asd\","
            + "\"field2\" : \"\","
            + "\"field3\" : 123456789012345678901234567890.0123456789,"
            + "\"field4\" : \"123456789012345678901234567890.0123456789\","
            + "\"field5\" : 123456789012345678901234567890123456789012345678901234567890,"
            + "\"field6\" : \"123456789012345678901234567890123456789012345678901234567890\","
            + "\"field7\" : true"
            + " }";

    JsonNode ce = getJsonNode(jsonChangeEvent);

    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field1", /* requiredField= */ true),
        new String(";asidjf987asd"));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field2", /* requiredField= */ true),
        new String(""));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field3", /* requiredField= */ true),
        new String("123456789012345678901234567890.0123456789"));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field4", /* requiredField= */ true),
        new String("123456789012345678901234567890.0123456789"));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field5", /* requiredField= */ true),
        new String("123456789012345678901234567890123456789012345678901234567890"));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field6", /* requiredField= */ true),
        new String("123456789012345678901234567890123456789012345678901234567890"));
    assertEquals(
        DataChangeRecordTypeConvertor.toString(ce, "field7", /* requiredField= */ true),
        new String("true"));
  }

  /*
   * Tests for numeric string conversion. Tests json with large numbers and decimals encoded as
   * JSON strings and numbers.
   */
  @Test
  public void canConvertToNumericBigDecimal() throws Exception {
    String jsonChangeEvent =
        "{ \"field1\" : 123456789.0123456789,\"field2\" : -123456789.0123456789,\"field3\" :"
            + " \"-123456789.0123456789\",\"field4\" : 9223372036854775807,\"field5\" :"
            + " \"9223372036854775807\",\"field6\" :"
            + " \"123345678903456545422346373223.903495832\",\"field7\" :"
            + " 123345678903456545422346373223.903495832,\"field8\" :"
            + " 1233456789034565454223463732234502384848374579495483732758539938558,\"field9\" :"
            + " \"1233456789034565454223463732234502384848374579495483732758539938558\",\"field10\""
            + " : \"1.2334567890345654542E10\",\"field11\" :"
            + " 123345.678903456545422346373223903495832,\"field12\" :"
            + " \"123345.678903456545422346373223903495832\" }";
    JsonNode ce = getJsonNode(jsonChangeEvent);

    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field1", /* requiredField= */ true)
            .toString(),
        new String("123456789.012345679"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field2", /* requiredField= */ true)
            .toString(),
        new String("-123456789.012345679"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field3", /* requiredField= */ true)
            .toString(),
        new String("-123456789.012345679"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field4", /* requiredField= */ true)
            .toString(),
        new String("9223372036854775807.000000000"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field5", /* requiredField= */ true)
            .toString(),
        new String("9223372036854775807.000000000"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field6", /* requiredField= */ true)
            .toString(),
        new String("123345678903456545422346373223.903495832"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field7", /* requiredField= */ true)
            .toString(),
        new String("123345678903456545422346373223.903495832"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field8", /* requiredField= */ true)
            .toString(),
        new String(
            "1233456789034565454223463732234502384848374579495483732758539938558.000000000"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field9", /* requiredField= */ true)
            .toString(),
        new String(
            "1233456789034565454223463732234502384848374579495483732758539938558.000000000"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field10", /* requiredField= */ true)
            .toString(),
        new String("12334567890.345654542"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field11", /* requiredField= */ true)
            .toString(),
        new String("123345.678903457"));
    assertEquals(
        DataChangeRecordTypeConvertor.toNumericBigDecimal(ce, "field12", /* requiredField= */ true)
            .toString(),
        new String("123345.678903457"));
  }

  /*
   * Tests for bytearray conversion
   */
  @Test
  public void canConvertToByteArray() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "YWJjCg==");
    changeEvent.put("field2", "aGVsbG8gd29ybGQ=");
    changeEvent.put("field3", "MGI0N2I2Znk3ODN0dQ==");
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toByteArray(ce, "field1", /* requiredField= */ true),
        ByteArray.fromBase64("YWJjCg=="));
    assertEquals(
        DataChangeRecordTypeConvertor.toByteArray(ce, "field2", /* requiredField= */ true),
        ByteArray.fromBase64("aGVsbG8gd29ybGQ="));
    assertEquals(
        DataChangeRecordTypeConvertor.toByteArray(ce, "field3", /* requiredField= */ true),
        ByteArray.fromBase64("MGI0N2I2Znk3ODN0dQ=="));
  }

  /*
   * Tests for Timestamp conversion
   */
  @Test
  public void canConvertToTimestamp() throws Exception {
    // Change events with all valid timestamps
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30T12:12:12Z");
    changeEvent.put("field2", "2020-12-30T12:12:12.1Z");
    changeEvent.put("field3", "2020-12-30T12:12:12.123Z");
    changeEvent.put("field4", "2020-12-30T12:12:12");
    changeEvent.put("field5", "2020-12-30T12:12:12.1");
    changeEvent.put("field6", "2020-12-30T12:12:12.123");
    changeEvent.put("field7", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field2", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field3", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field4", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field5", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field6", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
    assertNull(DataChangeRecordTypeConvertor.toTimestamp(ce, "field7", /* requiredField= */ false));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertDateToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T00:00:00Z"));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertLongToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 1234523342);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertBooleanToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  /*
   * Tests for Date conversion
   */
  @Test
  public void canConvertToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30");
    changeEvent.put("field2", "2020-02-30");
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        DataChangeRecordTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertEquals(
        DataChangeRecordTypeConvertor.toDate(ce, "field2", /* requiredField= */ true),
        Date.parseDate("2020-02-30"));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertRandomStringTodate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertLongToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 123456789);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = DataChangeRecordConvertorException.class)
  public void cannotConvertBooleanToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        DataChangeRecordTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }
}
