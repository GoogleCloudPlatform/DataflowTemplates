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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;

/** Provides functionality to interact with Struct values. */
public class StructValueHelper {

  /** Struct types with null values. */
  public static class NullTypes {
    public static final Boolean NULL_BOOLEAN = null;
    public static final ByteArray NULL_BYTES = null;
    public static final Date NULL_DATE = null;
    public static final Float NULL_FLOAT32 = null;
    public static final Double NULL_FLOAT64 = null;
    public static final Long NULL_INT64 = null;
    public static final String NULL_JSON = null;
    public static final BigDecimal NULL_NUMERIC = null;
    public static final String NULL_STRING = null;
    public static final Timestamp NULL_TIMESTAMP = null;
  }

  public static class CommonValues {
    public static Timestamp currentTimestamp() {
      return Timestamp.now();
    }
  }

  public static Boolean getBoolOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_BOOLEAN;
    }
    return value.getBool();
  }

  public static ByteArray getBytesOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_BYTES;
    }
    return value.getBytes();
  }

  public static Date getDateOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_DATE;
    }
    return value.getDate();
  }

  public static Float getFloat32OrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_FLOAT32;
    }
    return value.getFloat32();
  }

  public static Double getFloat64OrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_FLOAT64;
    }
    return value.getFloat64();
  }

  public static Long getInt64OrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_INT64;
    }
    return value.getInt64();
  }

  public static String getJsonOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_JSON;
    }
    return value.getJson();
  }

  public static BigDecimal getNumericOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_NUMERIC;
    }
    return value.getNumeric();
  }

  public static String getPgJsonbOrNull(Value value) {
    if (value.isNull()) {
      // JSON and JSONB are both String.
      return NullTypes.NULL_JSON;
    }
    return value.getPgJsonb();
  }

  public static String getStringOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_STRING;
    }
    return value.getString();
  }

  public static Timestamp getTimestampOrNull(Value value) {
    if (value.isNull()) {
      return NullTypes.NULL_TIMESTAMP;
    }
    return value.getTimestamp();
  }
}
