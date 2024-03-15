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
package com.google.cloud.teleport.v2.transformer;

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

/** Unit tests for ChangeEventTypeConvertor class. */
public final class ChangeEventTypeConvertorTest {

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
    changeEvent.put("bool_field1", "true");
    changeEvent.put("bool_field2", "True");
    changeEvent.put("bool_field3", "TRUE");
    changeEvent.put("bool_field4", "TrUe");
    changeEvent.put("bool_field5", true);
    changeEvent.put("bool_field6", "false");
    changeEvent.put("bool_field7", "False");
    changeEvent.put("bool_field8", "FALSE");
    changeEvent.put("bool_field9", "fAlSE");
    changeEvent.put("bool_field10", false);
    changeEvent.put("bool_field11", 10); // Integers other than 0 interpreted as true
    changeEvent.put("bool_field12", 0); // False
    changeEvent.put("bool_field13", "y"); // Interpreted as true
    changeEvent.put("bool_field14", "n"); // False
    changeEvent.put("bool_field15", ""); // False
    changeEvent.put("bool_field16", "Trueee"); // Interpreted as false.
    changeEvent.put("bool_field17", 12145213.2233); // Decimals are interpreted as false.
    changeEvent.put("bool_field18", JSONObject.NULL);

    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field1", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field2", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field3", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field4", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field5", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field6", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field7", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field8", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field9", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field10", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field11", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field12", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field13", /* requiredField= */ true),
        new Boolean(true));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field14", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field15", /* requiredField= */ true),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field16", /* requiredField= */ false),
        new Boolean(false));
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field17", /* requiredField= */ false),
        new Boolean(false));
    assertNull(ChangeEventTypeConvertor.toBoolean(ce, "bool_field18", /* requiredField= */ false));
    assertNull(ChangeEventTypeConvertor.toBoolean(ce, "non_existent", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToBoolean() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toBoolean(ce, "bool_field1", /* requiredField= */ true),
        new Boolean(false));
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
    changeEvent.put("field3", 123456.789);
    changeEvent.put("field4", -123456.789);
    changeEvent.put("field5", "123456789");
    changeEvent.put("field6", "-123456789");
    changeEvent.put("field7", "123456.789");
    changeEvent.put("field8", "-123456.789");
    changeEvent.put("field9", true); // Interpreted as 1
    changeEvent.put("field10", false); // Interpreted as 0
    changeEvent.put("field11", JSONObject.NULL);

    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field1", /* requiredField= */ true),
        new Long(123456789));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field2", /* requiredField= */ true),
        new Long(-123456789));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field3", /* requiredField= */ true), new Long(123456));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field4", /* requiredField= */ true),
        new Long(-123456));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field5", /* requiredField= */ true),
        new Long(123456789));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field6", /* requiredField= */ true),
        new Long(-123456789));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field7", /* requiredField= */ true), new Long(123456));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field8", /* requiredField= */ true),
        new Long(-123456));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field9", /* requiredField= */ true), new Long(1));
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field10", /* requiredField= */ true), new Long(0));
    assertNull(ChangeEventTypeConvertor.toLong(ce, "field11", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToLong() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field1", /* requiredField= */ true), new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToLong() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toLong(ce, "field1", /* requiredField= */ true), new Long(123457));
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
    changeEvent.put("field5", "123456789");
    changeEvent.put("field6", "-123456789");
    changeEvent.put("field7", "123456.789");
    changeEvent.put("field8", "-123456.789");
    changeEvent.put("field9", 123456789.012345678912);
    changeEvent.put("field10", true);
    changeEvent.put("field11", false);
    changeEvent.put("field12", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field1", /* requiredField= */ true),
        new Double(123456789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field2", /* requiredField= */ true),
        new Double(-123456789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field3", /* requiredField= */ true),
        new Double(123456.789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field4", /* requiredField= */ true),
        new Double(-123456.789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field5", /* requiredField= */ true),
        new Double(123456789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field6", /* requiredField= */ true),
        new Double(-123456789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field7", /* requiredField= */ true),
        new Double(123456.789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field8", /* requiredField= */ true),
        new Double(-123456.789));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field9", /* requiredField= */ true),
        new Double(123456789.012345678912));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field10", /* requiredField= */ true), new Double(1));
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field11", /* requiredField= */ true), new Double(0));
    assertNull(ChangeEventTypeConvertor.toDouble(ce, "field12", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToDouble() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field1", /* requiredField= */ true),
        new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToDouble() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDouble(ce, "field1", /* requiredField= */ true),
        new Long(123457));
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
            + "\"field7\" : true,"
            + "\"field13\": null"
            + " }";

    JsonNode ce = getJsonNode(jsonChangeEvent);

    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field1", /* requiredField= */ true),
        new String(";asidjf987asd"));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field2", /* requiredField= */ true), new String(""));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field3", /* requiredField= */ true),
        new String("123456789012345678901234567890.0123456789"));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field4", /* requiredField= */ true),
        new String("123456789012345678901234567890.0123456789"));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field5", /* requiredField= */ true),
        new String("123456789012345678901234567890123456789012345678901234567890"));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field6", /* requiredField= */ true),
        new String("123456789012345678901234567890123456789012345678901234567890"));
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field7", /* requiredField= */ true),
        new String("true"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToString() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toString(ce, "field1", /* requiredField= */ true), new String(""));
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
            + " \"123345.678903456545422346373223903495832\", \"field13\": null }";
    JsonNode ce = getJsonNode(jsonChangeEvent);

    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field1", /* requiredField= */ true)
            .toString(),
        new String("123456789.012345679"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field2", /* requiredField= */ true)
            .toString(),
        new String("-123456789.012345679"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field3", /* requiredField= */ true)
            .toString(),
        new String("-123456789.012345679"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field4", /* requiredField= */ true)
            .toString(),
        new String("9223372036854775807.000000000"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field5", /* requiredField= */ true)
            .toString(),
        new String("9223372036854775807.000000000"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field6", /* requiredField= */ true)
            .toString(),
        new String("123345678903456545422346373223.903495832"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field7", /* requiredField= */ true)
            .toString(),
        new String("123345678903456545422346373223.903495832"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field8", /* requiredField= */ true)
            .toString(),
        new String(
            "1233456789034565454223463732234502384848374579495483732758539938558.000000000"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field9", /* requiredField= */ true)
            .toString(),
        new String(
            "1233456789034565454223463732234502384848374579495483732758539938558.000000000"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field10", /* requiredField= */ true)
            .toString(),
        new String("12334567890.345654542"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field11", /* requiredField= */ true)
            .toString(),
        new String("123345.678903457"));
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field12", /* requiredField= */ true)
            .toString(),
        new String("123345.678903457"));
    assertNull(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field13", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToNumeric() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field1", /* requiredField= */ true)
            .toString(),
        new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToNumeric() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field1", /* requiredField= */ true)
            .toString(),
        new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToNumeric() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toNumericBigDecimal(ce, "field1", /* requiredField= */ true),
        new Long(123457));
  }

  /*
   * Tests for bytearray conversion
   */
  @Test
  public void canConvertToByteArray() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "68656c6c6f20686f772061722065796f75");
    changeEvent.put("field2", "");
    changeEvent.put("field3", 123456789);
    changeEvent.put("field4", true);
    changeEvent.put("field5", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toByteArray(ce, "field1", /* requiredField= */ true),
        ByteArray.copyFrom(
            new byte[] {
              104, 101, 108, 108, 111, 32, 104, 111, 119, 32, 97, 114, 32, 101, 121, 111, 117
            }));
    assertEquals(
        ChangeEventTypeConvertor.toByteArray(ce, "field2", /* requiredField= */ true),
        ByteArray.copyFrom(""));
    assertEquals(
        ChangeEventTypeConvertor.toByteArray(ce, "field3", /* requiredField= */ true),
        ByteArray.copyFrom(new byte[] {1, 35, 69, 103, -119}));
    assertEquals(
        ChangeEventTypeConvertor.toByteArray(ce, "field4", /* requiredField= */ true),
        ByteArray.copyFrom(new byte[] {-17, -2}));
    assertNull(ChangeEventTypeConvertor.toByteArray(ce, "field5", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToByteArray() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toByteArray(ce, "field1", /* requiredField= */ true),
        new byte[] {-17, -2});
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
    changeEvent.put("field6", "2020-12-30T12:12:12.12345");
    changeEvent.put("field7", "2023-12-22T15:26:01.769602");
    changeEvent.put("field8", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field2", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field3", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field4", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field5", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field6", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.12345Z"));
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field7", /* requiredField= */ true),
        Timestamp.parseTimestamp("2023-12-22T15:26:01.769602"));
    assertNull(ChangeEventTypeConvertor.toTimestamp(ce, "field8", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertDateToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T00:00:00Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertLongToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 1234523342);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toTimestamp(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  /*
   * Tests for Date conversion
   */
  @Test
  public void canConvertToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30T00:00:00Z");
    changeEvent.put("field2", "2020-12-30");
    changeEvent.put("field3", "2020-12-30T12:12:12Z");
    changeEvent.put("field4", "2020-12-30T00:00:00");
    changeEvent.put("field5", "2020-12-30T12:12:12");
    changeEvent.put("field6", JSONObject.NULL);
    JsonNode ce = getJsonNode(changeEvent.toString());

    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field2", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field3", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field4", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field5", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
    assertNull(ChangeEventTypeConvertor.toDate(ce, "field6", /* requiredField= */ false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertLongToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 123456789);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredFieldToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    JsonNode ce = getJsonNode(changeEvent.toString());
    assertEquals(
        ChangeEventTypeConvertor.toDate(ce, "field1", /* requiredField= */ true),
        Date.parseDate("2020-12-30"));
  }
}
