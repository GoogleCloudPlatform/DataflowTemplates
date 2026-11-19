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
package com.google.cloud.teleport.v2.mongodb.validator.comparison;

import static com.google.common.truth.Truth.assertThat;

import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BsonOrderComparator}. */
@RunWith(JUnit4.class)
public class BsonOrderComparatorTest {

  @Test
  public void utf8ByteOrder_ordersBmpAboveSurrogateCorrectly() {
    // U+E000 (UTF-8: EE 80 80) < U+1F600 (UTF-8: F0 9F 98 80), whereas UTF-16 String.compareTo
    // orders surrogate D83D before E000.
    BsonString bmpHigh = new BsonString("\uE000");
    BsonString supplementaryEmoji = new BsonString("\uD83D\uDE00");

    assertThat(BsonOrderComparator.INSTANCE.compare(bmpHigh, supplementaryEmoji)).isLessThan(0);
  }

  @Test
  public void objectIdUnsignedByteOrder_handlesSignBitBoundary() {
    BsonObjectId low = new BsonObjectId(new ObjectId("7fffffffffffffffffffffff"));
    BsonObjectId high = new BsonObjectId(new ObjectId("800000000000000000000000"));

    assertThat(BsonOrderComparator.INSTANCE.compare(low, high)).isLessThan(0);
  }

  @Test
  public void numericCrossTypeOrder_handlesNaNInfinityAndLargeInt64() {
    BsonDouble nan = new BsonDouble(Double.NaN);
    BsonDouble negInf = new BsonDouble(Double.NEGATIVE_INFINITY);
    BsonInt32 fiveInt = new BsonInt32(5);
    BsonDouble fiveDouble = new BsonDouble(5.0);
    BsonInt64 highInt64 = new BsonInt64((1L << 53) + 1L);
    BsonDouble roundedDouble = new BsonDouble((double) (1L << 53));
    BsonDecimal128 hugeDec = new BsonDecimal128(Decimal128.parse("999999999999999999999.99"));

    assertThat(BsonOrderComparator.INSTANCE.compare(nan, negInf)).isLessThan(0);
    assertThat(BsonOrderComparator.INSTANCE.compare(fiveInt, fiveDouble)).isEqualTo(0);
    assertThat(BsonOrderComparator.INSTANCE.compare(roundedDouble, highInt64)).isLessThan(0);
    assertThat(BsonOrderComparator.INSTANCE.compare(highInt64, hugeDec)).isLessThan(0);
  }
}
