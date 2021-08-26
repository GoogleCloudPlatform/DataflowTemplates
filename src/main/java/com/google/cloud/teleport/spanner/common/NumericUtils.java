/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.spanner.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/** Utility class for dealing with Numeric values. */
public final class NumericUtils {

  public static final int PRECISION = 38;
  public static final int SCALE = 9;

  // Convert an Avro-encoded NUMERIC byte array to an readable NUMERIC string value.
  public static String bytesToString(byte[] byteArray) {
    BigInteger unscaledNumeric = new BigInteger(byteArray);
    BigDecimal scaledNumeric = new BigDecimal(unscaledNumeric, SCALE);
    return scaledNumeric.toPlainString();
  }

  // Convert a readable NUMERIC string value to an Avro-encoded NUMERIC byte array.
  public static byte[] stringToBytes(String numeric) {
    BigDecimal scaledNumeric = new BigDecimal(numeric, new MathContext(PRECISION)).setScale(SCALE);
    BigInteger unscaledNumeric = scaledNumeric.unscaledValue();
    return unscaledNumeric.toByteArray();
  }
}
