/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.v2.templates.datastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Unit tests for ChangeEventTypeConvertor class.
 */
public final class ChangeEventTypeConvertorTest {

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
    changeEvent.put("bool_field11", JSONObject.NULL);

    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field1",
                     /*requiredField=*/true), new Boolean(true));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field2",
                     /*requiredField=*/true), new Boolean(true));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field3",
                     /*requiredField=*/true), new Boolean(true));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field4",
                     /*requiredField=*/true), new Boolean(true));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field5",
                     /*requiredField=*/true), new Boolean(true));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field6",
                     /*requiredField=*/true), new Boolean(false));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field7",
                     /*requiredField=*/true), new Boolean(false));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field8",
                     /*requiredField=*/true), new Boolean(false));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field9",
                     /*requiredField=*/true), new Boolean(false));
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field10",
                     /*requiredField=*/true), new Boolean(false));
    assertNull(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field11",
                   /*requiredField=*/false));
    assertNull(ChangeEventTypeConvertor.toBoolean(changeEvent, "non_existent",
                   /*requiredField=*/false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertEmptyStringToBoolean() throws Exception {

    // Change Event with empty string.
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("bool_field1", "");
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field1",
                     /*requiredField=*/true), new Boolean(false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToBoolean() throws Exception {

    // Change Event with all valid forms of boolean values.
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("bool_field1", "Truee");
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field1",
                     /*requiredField=*/true), new Boolean(false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomNumberToBoolean() throws Exception {

    // Change Event with all valid forms of boolean values.
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("bool_field1", 123.456);
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field1",
                     /*requiredField=*/true), new Boolean(false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertNonExistentRequiredField() throws Exception {
    JSONObject changeEvent = new JSONObject();
    assertEquals(ChangeEventTypeConvertor.toBoolean(changeEvent, "bool_field1",
                     /*requiredField=*/true), new Boolean(false));
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
    changeEvent.put("field9", JSONObject.NULL);

    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field1", /*requiredField=*/true),
        new Long(123456789));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field2", /*requiredField=*/true),
        new Long(-123456789));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field3", /*requiredField=*/true),
        new Long(123456));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field4", /*requiredField=*/true),
        new Long(-123456));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field5", /*requiredField=*/true),
        new Long(123456789));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field6", /*requiredField=*/true),
        new Long(-123456789));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field7", /*requiredField=*/true),
        new Long(123456));
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field8", /*requiredField=*/true),
        new Long(-123456));
    assertNull(ChangeEventTypeConvertor.toLong(changeEvent, "field9", /*requiredField=*/false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToLong() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field1", /*requiredField=*/true),
        new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToLong() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    assertEquals(ChangeEventTypeConvertor.toLong(changeEvent, "field1", /*requiredField=*/true),
        new Long(123457));
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
    changeEvent.put("field9", 123456789.012345678);
    changeEvent.put("field10", JSONObject.NULL);

    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field1", /*requiredField=*/true),
        new Double(123456789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field2", /*requiredField=*/true),
        new Double(-123456789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field3", /*requiredField=*/true),
        new Double(123456.789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field4", /*requiredField=*/true),
        new Double(-123456.789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field5", /*requiredField=*/true),
        new Double(123456789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field6", /*requiredField=*/true),
        new Double(-123456789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field7", /*requiredField=*/true),
        new Double(123456.789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field8", /*requiredField=*/true),
        new Double(-123456.789));
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field9", /*requiredField=*/true),
        new Double(123456789.012345678));
    assertNull(ChangeEventTypeConvertor.toDouble(changeEvent, "field10", /*requiredField=*/false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToDouble() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field1", /*requiredField=*/true),
        new Long(123457));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToDouble() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    assertEquals(ChangeEventTypeConvertor.toDouble(changeEvent, "field1", /*requiredField=*/true),
        new Long(123457));
  }

  /*
   * Tests for string conversion
   */
  @Test
  public void canConvertToString() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", ";asidjf987asd");
    changeEvent.put("field2", "");
    changeEvent.put("field3", 123456789.0123456789);
    changeEvent.put("field4", 123456789);
    changeEvent.put("field5", true);
    changeEvent.put("field6", JSONObject.NULL);

    assertEquals(ChangeEventTypeConvertor.toString(changeEvent, "field1", /*requiredField=*/true),
        new String(";asidjf987asd"));
    assertEquals(ChangeEventTypeConvertor.toString(changeEvent, "field2", /*requiredField=*/true),
        new String(""));
    assertEquals(ChangeEventTypeConvertor.toString(changeEvent, "field3", /*requiredField=*/true),
        new String("1.2345678901234567E8"));
    assertEquals(ChangeEventTypeConvertor.toString(changeEvent, "field4", /*requiredField=*/true),
        new String("123456789"));
    assertEquals(ChangeEventTypeConvertor.toString(changeEvent, "field5", /*requiredField=*/true),
        new String("true"));
    assertNull(ChangeEventTypeConvertor.toString(changeEvent, "field6", /*requiredField=*/false));
  }

  /*
   * Tests for bytearray conversion
   */
  @Test
  public void canConvertToByteArray() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asidjf987asd");
    changeEvent.put("field2", "");
    changeEvent.put("field3", 123456789);
    changeEvent.put("field4", true);
    changeEvent.put("field5", JSONObject.NULL);

    assertEquals(ChangeEventTypeConvertor.toByteArray(changeEvent, "field1",
                     /*requiredField=*/true), ByteArray.copyFrom("asidjf987asd"));
    assertEquals(ChangeEventTypeConvertor.toByteArray(changeEvent, "field2",
                     /*requiredField=*/true), ByteArray.copyFrom(""));
    assertEquals(ChangeEventTypeConvertor.toByteArray(changeEvent, "field3",
                     /*requiredField=*/true), ByteArray.copyFrom("123456789"));
    assertEquals(ChangeEventTypeConvertor.toByteArray(changeEvent, "field4",
                     /*requiredField=*/true), ByteArray.copyFrom("true"));
    assertNull(ChangeEventTypeConvertor.toByteArray(changeEvent, "field5",
                   /*requiredField=*/false));
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

    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field1",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field2",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field3",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field4",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field5",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12.1Z"));
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field6",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12.123Z"));
    assertNull(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field7",
                   /*requiredField=*/false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertDateToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "2020-12-30");
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field1",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T00:00:00Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field1",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertLongToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 1234523342);
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field1",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToTimestamp() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    assertEquals(ChangeEventTypeConvertor.toTimestamp(changeEvent, "field1",
                     /*requiredField=*/true), Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
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

    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field1", /*requiredField=*/true),
        Date.parseDate("2020-12-30"));
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field2", /*requiredField=*/true),
        Date.parseDate("2020-12-30"));
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field3", /*requiredField=*/true),
        Date.parseDate("2020-12-30"));
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field4", /*requiredField=*/true),
        Date.parseDate("2020-12-30"));
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field5", /*requiredField=*/true),
        Date.parseDate("2020-12-30"));
    assertNull(ChangeEventTypeConvertor.toDate(changeEvent, "field6", /*requiredField=*/false));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertRandomStringTodate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", "asd123456.789");
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field1", /*requiredField=*/true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertLongToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", 123456789);
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field1", /*requiredField=*/true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }

  @Test(expected = ChangeEventConvertorException.class)
  public void cannotConvertBooleanToDate() throws Exception {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("field1", true);
    assertEquals(ChangeEventTypeConvertor.toDate(changeEvent, "field1", /*requiredField=*/true),
        Timestamp.parseTimestamp("2020-12-30T12:12:12Z"));
  }
}
