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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StructComparatorTest {

  @Test
  public void compareBool() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_BOOLEAN)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder().set("orderColumn").to(Boolean.FALSE).set("other").to("z").build();
    Struct r3 =
        Struct.newBuilder().set("orderColumn").to(Boolean.TRUE).set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareBytes_throws() {}

  @Test
  public void compareDate() {
    Struct r1 =
        Struct.newBuilder().set("orderColumn").to(NullTypes.NULL_DATE).set("other").to("g").build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Date.fromYearMonthDay(1990, 7, 14))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Date.fromYearMonthDay(2024, 1, 1))
            .set("other")
            .to("f")
            .build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareFloat32() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_FLOAT32)
            .set("other")
            .to("g")
            .build();
    Struct r2 = Struct.newBuilder().set("orderColumn").to(2.72F).set("other").to("z").build();
    Struct r3 = Struct.newBuilder().set("orderColumn").to(3.14F).set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareFloat64() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_FLOAT64)
            .set("other")
            .to("g")
            .build();
    Struct r2 = Struct.newBuilder().set("orderColumn").to(2.72).set("other").to("z").build();
    Struct r3 = Struct.newBuilder().set("orderColumn").to(3.14).set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareInt64() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_INT64)
            .set("other")
            .to("g")
            .build();
    Struct r2 = Struct.newBuilder().set("orderColumn").to(-2).set("other").to("z").build();
    Struct r3 = Struct.newBuilder().set("orderColumn").to(0).set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareJson() {
    Struct r1 =
        Struct.newBuilder().set("orderColumn").to(NullTypes.NULL_JSON).set("other").to("g").build();
    Struct r2 =
        Struct.newBuilder().set("orderColumn").to("{\"a\": 1}").set("other").to("z").build();
    Struct r3 =
        Struct.newBuilder().set("orderColumn").to("{\"a\": 2}").set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareNumeric() {
    Struct r1 =
        Struct.newBuilder().set("orderColumn").to(NullTypes.NULL_JSON).set("other").to("g").build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(BigDecimal.valueOf(2.72))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(BigDecimal.valueOf(3.14))
            .set("other")
            .to("f")
            .build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void comparePgNumeric() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_NUMERIC)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Value.pgNumeric("2.72"))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Value.pgNumeric("3.14"))
            .set("other")
            .to("f")
            .build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void comparePgJsonb() {
    Struct r1 =
        Struct.newBuilder().set("orderColumn").to(NullTypes.NULL_JSON).set("other").to("g").build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Value.pgJsonb("{\"a\": 1}"))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Value.pgJsonb("{\"a\": 2}"))
            .set("other")
            .to("f")
            .build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareString() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_STRING)
            .set("other")
            .to("g")
            .build();
    Struct r2 = Struct.newBuilder().set("orderColumn").to("abc").set("other").to("z").build();
    Struct r3 = Struct.newBuilder().set("orderColumn").to("xyz").set("other").to("f").build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }

  @Test
  public void compareTimestamp() {
    Struct r1 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(NullTypes.NULL_TIMESTAMP)
            .set("other")
            .to("g")
            .build();
    Struct r2 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("other")
            .to("z")
            .build();
    Struct r3 =
        Struct.newBuilder()
            .set("orderColumn")
            .to(Timestamp.ofTimeMicroseconds(1000))
            .set("other")
            .to("f")
            .build();
    ArrayList<Struct> inputRecords = new ArrayList<>(List.of(r3, r1, r2)); // Out of order.
    List<Struct> expectedOutput = List.of(r1, r2, r3);

    inputRecords.sort(StructComparator.create("orderColumn"));

    assertThat(inputRecords).containsExactlyElementsIn(expectedOutput).inOrder();
  }
}
