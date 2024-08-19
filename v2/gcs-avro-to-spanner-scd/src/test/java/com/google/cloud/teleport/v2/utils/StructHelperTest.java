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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StructHelperTest {

  @Test
  public void testOmitColumns_createsCopyOfRecordWithoutOmittedColumns() {
    Timestamp timestamp = Timestamp.now();
    Struct inputRecord =
        Struct.newBuilder()
            .set("pk1")
            .to(777)
            .set("pk2")
            .to(timestamp)
            .set("pk3")
            .to("value")
            .set("column1")
            .to("Nito")
            .build();
    Struct expectedOutputRecord =
        Struct.newBuilder().set("pk1").to(777).set("column1").to("Nito").build();

    StructHelper structHelper =
        StructHelper.of(inputRecord).omitColumNames(ImmutableList.of("pk2", "pk3"));

    assertThat(structHelper.getStruct()).isEqualTo(expectedOutputRecord);
  }

  @Test
  public void testCopyAsBuilder_createsCopyOnBuilder() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(777)
            .set("pk2")
            .to(timestamp)
            .set("pk3")
            .to("value")
            .set("column1")
            .to("Nito")
            .build();

    Struct.Builder structBuilder = StructHelper.of(record).copyAsBuilder();

    assertThat(structBuilder.build()).isEqualTo(record);
  }

  @Test
  public void testKeyMaker_createKey_forOneKey() {
    Struct record = Struct.newBuilder().set("pk1").to(777).set("xyz").to("Nito").build();
    List<String> primaryKeys = List.of("pk1");
    Key expectedKey = Key.newBuilder().append(777).build();

    Key returnKey = StructHelper.of(record).keyMaker(primaryKeys).createKey();

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKey_forAllDataTypes() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            .set("pkBoolean")
            .to(Value.bool(Boolean.TRUE))
            .set("pkBytes")
            .to(Value.bytes(ByteArray.fromBase64("Tml0bw==")))
            .set("pkDate")
            .to(Value.date(Date.fromYearMonthDay(1990, 7, 14)))
            .set("pkFloat32")
            .to(Value.float32(3.14F))
            .set("pkFloat64")
            .to(Value.float64(3.14))
            .set("pkInt64")
            .to(Value.int64(777L))
            .set("pkJson")
            .to(Value.json("{\"name\": \"Nito\"}"))
            .set("pkNumeric")
            .to(Value.numeric(BigDecimal.valueOf(3.14)))
            .set("pkPgNumeric")
            .to(Value.pgNumeric("3.14"))
            .set("pkJsonb")
            .to(Value.pgJsonb("{\"name\": \"Nito\"}"))
            .set("pkString")
            .to(Value.string("string"))
            .set("pkTimestamp")
            .to(Value.timestamp(timestamp))
            .build();
    List<String> primaryKeys =
        List.of(
            "pkBoolean",
            "pkBytes",
            "pkDate",
            "pkFloat32",
            "pkFloat64",
            "pkInt64",
            "pkJson",
            "pkNumeric",
            "pkPgNumeric",
            "pkJsonb",
            "pkString",
            "pkTimestamp");
    Key expectedKey =
        Key.newBuilder()
            .append(Boolean.TRUE)
            .append(ByteArray.fromBase64("Tml0bw=="))
            .append(Date.fromYearMonthDay(1990, 7, 14))
            .append(3.14F)
            .append(3.14)
            .append(777L)
            .append("{\"name\": \"Nito\"}")
            .append(BigDecimal.valueOf(3.14))
            .append(BigDecimal.valueOf(3.14))
            .append("{\"name\": \"Nito\"}")
            .append("string")
            .append(timestamp)
            .build();

    Key returnKey = StructHelper.of(record).keyMaker(primaryKeys).createKey();

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKey_forMultipleKeys_inOrder() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(777)
            .set("pk2")
            .to(timestamp)
            .set("pk3")
            .to("value")
            .set("column1")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    Key expectedKey = Key.newBuilder().append(777).append(timestamp).append("value").build();

    Key returnKey = StructHelper.of(record).keyMaker(primaryKeys).createKey();

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKey_forMultipleKeys_outOfOrder() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            // Columns are intentionally out of order.
            .set("pk1")
            .to(777)
            .set("column1")
            .to("Nito")
            .set("pk3")
            .to("value")
            .set("pk2")
            .to(timestamp)
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    Key expectedKey = Key.newBuilder().append(777).append(timestamp).append("value").build();

    Key returnKey = StructHelper.of(record).keyMaker(primaryKeys).createKey();

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKey_forMultipleKeys_outOfOrder_withOmittedColumns() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            // Columns are intentionally out of order.
            .set("pk1")
            .to(777)
            .set("column1")
            .to("Nito")
            .set("pk3")
            .to("value")
            .set("pk2")
            .to(timestamp)
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    List<String> omittedColumnNames = List.of("pk3", "nonExistingPk");
    Key expectedKey = Key.newBuilder().append(777).append(timestamp).build();

    Key returnKey = StructHelper.of(record).keyMaker(primaryKeys, omittedColumnNames).createKey();

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKey_throwsForNonExistingPrimaryKey() {
    Struct structField = Struct.newBuilder().set("abc").to(123).build();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(structField) // Struct (Record) is not supported.
            .set("column1")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk2");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> StructHelper.of(record).keyMaker(primaryKeys).createKey());

    assertThat(thrown)
        .hasMessageThat()
        .contains("Primary key name pk2 not found in record. Unable to create Key.");
  }

  @Test
  public void testKeyMaker_createKey_throwsForNonSupportedType() {
    Struct structField = Struct.newBuilder().set("abc").to(123).build();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(structField) // Struct (Record) is not supported.
            .set("column1")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk1");

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () -> StructHelper.of(record).keyMaker(primaryKeys).createKey());

    assertThat(thrown).hasMessageThat().contains("Unsupported Spanner field type STRUCT.");
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_forOneKey_addsExtraValuesInOrder() {
    Struct record = Struct.newBuilder().set("pk1").to(777).set("xyz").to("Nito").build();
    List<String> primaryKeys = List.of("pk1");
    Timestamp extraValue1 = Timestamp.now();
    String extraValue2 = "otherValue";
    Key expectedKey = Key.newBuilder().append(777).append(extraValue1).append(extraValue2).build();

    Key returnKey =
        StructHelper.of(record)
            .keyMaker(primaryKeys)
            .createKeyWithExtraValues(Value.timestamp(extraValue1), Value.string(extraValue2));

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_forMultipleKeys_addsExtraValuesInOrder() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            // Columns are intentionally out of order.
            .set("pk1")
            .to(777)
            .set("column1")
            .to("Nito")
            .set("pk3")
            .to("value")
            .set("pk2")
            .to(timestamp)
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    Timestamp extraValue1 = Timestamp.now();
    String extraValue2 = "otherValue";
    Key expectedKey =
        Key.newBuilder()
            .append(777)
            .append(timestamp)
            .append("value")
            .append(extraValue1)
            .append(extraValue2)
            .build();

    Key returnKey =
        StructHelper.of(record)
            .keyMaker(primaryKeys)
            .createKeyWithExtraValues(Value.timestamp(extraValue1), Value.string(extraValue2));

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_addsValues_forAllDataTypes() {
    Timestamp timestamp = Timestamp.now();
    Struct record = Struct.newBuilder().set("column1").to("Nito").build();
    List<String> primaryKeys = List.of();
    Key expectedKey =
        Key.newBuilder()
            .append(Boolean.TRUE)
            .append(ByteArray.fromBase64("Tml0bw=="))
            .append(Date.fromYearMonthDay(1990, 7, 14))
            .append(3.14F)
            .append(3.14)
            .append(777L)
            .append("{\"name\": \"Nito\"}")
            .append(BigDecimal.valueOf(3.14))
            .append(BigDecimal.valueOf(3.14))
            .append("{\"name\": \"Nito\"}")
            .append("string")
            .append(timestamp)
            .build();

    Key returnKey =
        StructHelper.of(record)
            .keyMaker(primaryKeys)
            .createKeyWithExtraValues(
                Value.bool(Boolean.TRUE),
                Value.bytes(ByteArray.fromBase64("Tml0bw==")),
                Value.date(Date.fromYearMonthDay(1990, 7, 14)),
                Value.float32(3.14F),
                Value.float64(3.14),
                Value.int64(777L),
                Value.json("{\"name\": \"Nito\"}"),
                Value.numeric(BigDecimal.valueOf(3.14)),
                Value.pgNumeric("3.14"),
                Value.pgJsonb("{\"name\": \"Nito\"}"),
                Value.string("string"),
                Value.timestamp(timestamp));

    assertThat(returnKey).isEqualTo(expectedKey);
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_throwsForNonExistingPrimaryKey() {
    Struct structField = Struct.newBuilder().set("abc").to(123).build();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(structField) // Struct (Record) is not supported.
            .set("column1")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk2");
    Timestamp extraValue = Timestamp.now();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                StructHelper.of(record)
                    .keyMaker(primaryKeys)
                    .createKeyWithExtraValues(Value.timestamp(extraValue)));

    assertThat(thrown)
        .hasMessageThat()
        .contains("Primary key name pk2 not found in record. Unable to create Key.");
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_throwsForNonSupportedPrimaryKey() {
    Struct structField = Struct.newBuilder().set("abc").to(123).build();
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(structField) // Struct (Record) is not supported.
            .set("column1")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk1");
    Timestamp extraValue = Timestamp.now();

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                StructHelper.of(record)
                    .keyMaker(primaryKeys)
                    .createKeyWithExtraValues(Value.timestamp(extraValue)));

    assertThat(thrown).hasMessageThat().contains("Unsupported Spanner field type STRUCT.");
  }

  @Test
  public void testKeyMaker_createKeyWithExtraValues_throwsForNonSupportedExtraValue() {
    Struct record = Struct.newBuilder().set("pk1").to(777).set("column1").to("Nito").build();
    List<String> primaryKeys = List.of("pk1");
    Struct extraValue = Struct.newBuilder().set("abc").to(123).build();

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                StructHelper.of(record)
                    .keyMaker(primaryKeys)
                    .createKeyWithExtraValues(Value.struct(extraValue)));

    assertThat(thrown).hasMessageThat().contains("Unsupported Spanner field type STRUCT.");
  }

  @Test
  public void testKeyMaker_createKeyString_castsKeyToString() {
    Struct record = Struct.newBuilder().set("pk1").to(777).set("xyz").to("Nito").build();
    List<String> primaryKeys = List.of("pk1");
    String expectedKeyString = Key.newBuilder().append(777).build().toString();

    String returnKeyString = StructHelper.of(record).keyMaker(primaryKeys).createKeyString();

    assertThat(returnKeyString).isEqualTo(expectedKeyString);
  }

  @Test
  public void testKeyMaker_createKeyString_castsKeyToString_inOrder() {
    Timestamp timestamp = Timestamp.now();
    Struct record =
        Struct.newBuilder()
            // Columns are intentionally out of order.
            .set("pk1")
            .to(777)
            .set("column1")
            .to("Nito")
            .set("pk3")
            .to("value")
            .set("pk2")
            .to(timestamp)
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    String expectedKeyString =
        Key.newBuilder().append(777).append(timestamp).append("value").build().toString();

    String returnKeyString = StructHelper.of(record).keyMaker(primaryKeys).createKeyString();

    assertThat(returnKeyString).isEqualTo(expectedKeyString);
  }

  @Test
  public void testKeyMaker_createKeyString_castsKeyToString_withNullTypes() {
    Struct record =
        Struct.newBuilder()
            .set("pk1")
            .to(777)
            .set("pk2")
            .to(NullTypes.NULL_TIMESTAMP)
            .set("pk3")
            .to(NullTypes.NULL_BOOLEAN)
            .set("xyz")
            .to("Nito")
            .build();
    List<String> primaryKeys = List.of("pk1", "pk2", "pk3");
    String expectedKeyString =
        Key.newBuilder()
            .append(777)
            .append(NullTypes.NULL_TIMESTAMP)
            .append(NullTypes.NULL_BOOLEAN)
            .build()
            .toString();

    String returnKeyString = StructHelper.of(record).keyMaker(primaryKeys).createKeyString();

    assertThat(returnKeyString).isEqualTo(expectedKeyString);
  }

  @Test
  public void testValueHelper_getBoolOrNull_getsBoolean_True() {
    Value boolValue = Value.bool(true);

    Boolean returnValue = ValueHelper.of(boolValue).getBoolOrNull();

    assertThat(returnValue).isTrue();
  }

  @Test
  public void testValueHelper_getBoolOrNull_getsBoolean_False() {
    Value boolValue = Value.bool(false);

    Boolean returnValue = ValueHelper.of(boolValue).getBoolOrNull();

    assertThat(returnValue).isFalse();
    assertThat(returnValue).isNotNull();
  }

  @Test
  public void testValueHelper_getBoolOrNull_getsNullBoolean() {
    Value boolValue = Value.bool(ValueHelper.NullTypes.NULL_BOOLEAN);

    var returnValue = ValueHelper.of(boolValue).getBoolOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_BOOLEAN);
  }

  @Test
  public void testValueHelper_getBytesOrNull_getsBytes() {
    ByteArray bytes = ByteArray.fromBase64("Tml0byBidWlsdCB0aGlzLg==");
    Value bytesValue = Value.bytes(bytes);

    ByteArray returnValue = ValueHelper.of(bytesValue).getBytesOrNull();

    assertThat(returnValue).isEqualTo(bytes);
  }

  @Test
  public void testValueHelper_getBytesOrNull_getsNullBytes() {
    Value bytesValue = Value.bytes(ValueHelper.NullTypes.NULL_BYTES);

    ByteArray returnValue = ValueHelper.of(bytesValue).getBytesOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_BYTES);
  }

  @Test
  public void testValueHelper_getDateOrNull_getsDate() {
    Date date = Date.parseDate("1990-07-14");
    Value dateValue = Value.date(date);

    Date returnValue = ValueHelper.of(dateValue).getDateOrNull();

    assertThat(returnValue).isEqualTo(date);
  }

  @Test
  public void testValueHelper_getDateOrNull_getsNullDate() {
    Value dateValue = Value.date(ValueHelper.NullTypes.NULL_DATE);

    Date returnValue = ValueHelper.of(dateValue).getDateOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_DATE);
  }

  @Test
  public void testValueHelper_getFloat32OrNull_getsFloat() {
    Float floatNumber = 7.0F;
    Value floatValue = Value.float32(floatNumber);

    Float returnValue = ValueHelper.of(floatValue).getFloat32OrNull();

    assertThat(returnValue).isEqualTo(floatNumber);
  }

  @Test
  public void testValueHelper_getFloat32OrNull_getsNull() {
    Value floatValue = Value.float32(ValueHelper.NullTypes.NULL_FLOAT32);

    Float returnValue = ValueHelper.of(floatValue).getFloat32OrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_FLOAT32);
  }

  @Test
  public void testValueHelper_getFloat64OrNull_getsDouble() {
    Double doubleNumber = 7.0;
    Value doubleValue = Value.float64(doubleNumber);

    Double returnValue = ValueHelper.of(doubleValue).getFloat64OrNull();

    assertThat(returnValue).isEqualTo(doubleNumber);
  }

  @Test
  public void testValueHelper_getFloat64OrNull_getsNull() {
    Value doubleValue = Value.float64(ValueHelper.NullTypes.NULL_FLOAT64);

    Double returnValue = ValueHelper.of(doubleValue).getFloat64OrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_FLOAT64);
  }

  @Test
  public void testValueHelper_getInt64OrNull_getsLong() {
    Long longNumber = 7L;
    Value longValue = Value.int64(longNumber);

    Long returnValue = ValueHelper.of(longValue).getInt64OrNull();

    assertThat(returnValue).isEqualTo(longNumber);
  }

  @Test
  public void testValueHelper_getInt64OrNull_getsNull() {
    Value longValue = Value.int64(ValueHelper.NullTypes.NULL_INT64);

    Long returnValue = ValueHelper.of(longValue).getInt64OrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_INT64);
  }

  @Test
  public void testValueHelper_getJsonOrNull_getsJsonString() {
    String json = "{\"name\": \"Nito\"}";
    Value jsonValue = Value.json(json);

    String returnValue = ValueHelper.of(jsonValue).getJsonOrNull();

    assertThat(returnValue).isEqualTo(json);
  }

  @Test
  public void testValueHelper_getJsonOrNull_getsNull() {
    Value jsonValue = Value.json(ValueHelper.NullTypes.NULL_JSON);

    String returnValue = ValueHelper.of(jsonValue).getJsonOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_JSON);
  }

  @Test
  public void testValueHelper_getNumericOrNull_getsDecimal() {
    BigDecimal decimal = BigDecimal.valueOf(7);
    Value decimalValue = Value.numeric(decimal);

    BigDecimal returnValue = ValueHelper.of(decimalValue).getNumericOrNull();

    assertThat(returnValue).isEqualTo(decimal);
  }

  @Test
  public void testValueHelper_getNumericOrNull_getsNull() {
    Value decimalValue = Value.numeric(ValueHelper.NullTypes.NULL_NUMERIC);

    BigDecimal returnValue = ValueHelper.of(decimalValue).getNumericOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_NUMERIC);
  }

  @Test
  public void testValueHelper_getNumericOrNull_pgNumeric_getsDecimal() {
    BigDecimal decimal = BigDecimal.valueOf(7);
    Value pgNumericValue = Value.pgNumeric("7");

    BigDecimal returnValue = ValueHelper.of(pgNumericValue).getNumericOrNull();

    assertThat(returnValue).isEqualTo(decimal);
  }

  @Test
  public void testValueHelper_getNumericOrNull_pgNumeric_getsNull() {
    // pgNumeric takes a String as input.
    Value pgNumericValue = Value.pgNumeric(ValueHelper.NullTypes.NULL_STRING);

    BigDecimal returnValue = ValueHelper.of(pgNumericValue).getNumericOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_NUMERIC);
  }

  @Test
  public void testValueHelper_getPgJsonbOrNull_getsJsonString() {
    String json = "{\"name\": \"Nito\"}";
    Value jsonbValue = Value.pgJsonb(json);

    String returnValue = ValueHelper.of(jsonbValue).getPgJsonbOrNull();

    assertThat(returnValue).isEqualTo(json);
  }

  @Test
  public void testValueHelper_getPgJsonbOrNull_getsNull() {
    Value jsonbValue = Value.pgJsonb(ValueHelper.NullTypes.NULL_JSON);

    String returnValue = ValueHelper.of(jsonbValue).getPgJsonbOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_JSON);
  }

  @Test
  public void testValueHelper_getStringOrNull_getsString() {
    String str = "Nito";
    Value strValue = Value.string(str);

    String returnValue = ValueHelper.of(strValue).getStringOrNull();

    assertThat(returnValue).isEqualTo(str);
  }

  @Test
  public void testValueHelper_getStringOrNull_getsNull() {
    Value strValue = Value.string(ValueHelper.NullTypes.NULL_STRING);

    String returnValue = ValueHelper.of(strValue).getStringOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_STRING);
  }

  @Test
  public void testValueHelper_getTimestampOrNull_getsTimestamp() {
    Timestamp timestamp = Timestamp.now();
    Value timestampValue = Value.timestamp(timestamp);

    Timestamp returnValue = ValueHelper.of(timestampValue).getTimestampOrNull();

    assertThat(returnValue).isEqualTo(timestamp);
  }

  @Test
  public void testValueHelper_getTimestampOrNull_getsNull() {
    Value timestampValue = Value.timestamp(ValueHelper.NullTypes.NULL_TIMESTAMP);

    Timestamp returnValue = ValueHelper.of(timestampValue).getTimestampOrNull();

    assertThat(returnValue).isNull();
    assertThat(returnValue).isEqualTo(ValueHelper.NullTypes.NULL_TIMESTAMP);
  }
}
