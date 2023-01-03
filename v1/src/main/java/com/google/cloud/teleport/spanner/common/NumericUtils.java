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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/** Utility class for dealing with Numeric values. */
public final class NumericUtils {

  public static final int PRECISION = 38;
  public static final int SCALE = 9;

  public static final int PG_MAX_PRECISION = 147455;
  public static final int PG_MAX_SCALE = 16383;

  // Convert an Avro-encoded NUMERIC byte array to an readable NUMERIC string value.
  public static String bytesToString(byte[] byteArray) {
    BigInteger unscaledNumeric = new BigInteger(byteArray);
    BigDecimal scaledNumeric = new BigDecimal(unscaledNumeric, SCALE);
    return scaledNumeric.toPlainString();
  }

  // Convert an Avro-encoded PG_NUMERIC byte array to an readable PG_NUMERIC string value.
  public static String pgBytesToString(byte[] byteArray) {
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArray));
      byte specialValue = dis.readByte();
      if (specialValue == (byte) 0x01) {
        return "NaN";
      }
      int scale = dis.readInt();
      byte[] unscaledNumeric = new byte[byteArray.length - 5];
      dis.readFully(unscaledNumeric);
      BigDecimal scaledNumeric = new BigDecimal(new BigInteger(unscaledNumeric), scale);
      return scaledNumeric.toPlainString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Convert a readable NUMERIC string value to an Avro-encoded NUMERIC byte array.
  public static byte[] stringToBytes(String numeric) {
    BigDecimal scaledNumeric = new BigDecimal(numeric, new MathContext(PRECISION)).setScale(SCALE);
    BigInteger unscaledNumeric = scaledNumeric.unscaledValue();
    return unscaledNumeric.toByteArray();
  }

  // Convert a readable PG_NUMERIC string value to an Avro-encoded PG_NUMERIC byte array.
  public static byte[] pgStringToBytes(String numeric) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      // Use a byte to hold special cases like NaN
      if (numeric.equals("NaN")) {
        dos.writeByte(1);
        dos.flush();
        return bos.toByteArray();
      } else {
        dos.writeByte(0);
      }
      BigDecimal scaledNumeric = new BigDecimal(numeric);
      BigInteger unscaledNumeric = scaledNumeric.unscaledValue();
      int scale = scaledNumeric.scale();
      dos.writeInt(scale);
      dos.write(unscaledNumeric.toByteArray());
      dos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
