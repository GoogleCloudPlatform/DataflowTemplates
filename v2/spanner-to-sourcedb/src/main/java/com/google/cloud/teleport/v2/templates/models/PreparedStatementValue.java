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
package com.google.cloud.teleport.v2.templates.models;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A typed value to be bound to a {@link java.sql.PreparedStatement} placeholder.
 *
 * <p>The value is always one of {@code String}, {@code byte[]}, {@code Boolean}, {@code
 * BigDecimal}, {@code Long}, {@code Double} or {@code null}. The {@link #getSqlType() SQL type} is
 * used to bind {@code null} values via {@link java.sql.PreparedStatement#setNull(int, int)}.
 */
public final class PreparedStatementValue {

  @Nullable private final Object value;
  private final int sqlType;

  private PreparedStatementValue(@Nullable Object value, int sqlType) {
    this.value = value;
    this.sqlType = sqlType;
  }

  public static PreparedStatementValue ofString(@Nullable String value) {
    return value == null
        ? nullValue(Types.VARCHAR)
        : new PreparedStatementValue(value, Types.VARCHAR);
  }

  public static PreparedStatementValue ofBytes(@Nullable byte[] value) {
    return value == null
        ? nullValue(Types.VARBINARY)
        : new PreparedStatementValue(value, Types.VARBINARY);
  }

  public static PreparedStatementValue ofBoolean(@Nullable Boolean value) {
    return value == null
        ? nullValue(Types.BOOLEAN)
        : new PreparedStatementValue(value, Types.BOOLEAN);
  }

  public static PreparedStatementValue ofBigDecimal(@Nullable BigDecimal value) {
    return value == null
        ? nullValue(Types.DECIMAL)
        : new PreparedStatementValue(value, Types.DECIMAL);
  }

  public static PreparedStatementValue ofLong(@Nullable Long value) {
    return value == null
        ? nullValue(Types.BIGINT)
        : new PreparedStatementValue(value, Types.BIGINT);
  }

  public static PreparedStatementValue ofDouble(@Nullable Double value) {
    return value == null
        ? nullValue(Types.DOUBLE)
        : new PreparedStatementValue(value, Types.DOUBLE);
  }

  public static PreparedStatementValue nullValue(int sqlType) {
    return new PreparedStatementValue(null, sqlType);
  }

  /**
   * Converts an arbitrary Java object (for example a value returned by a custom transformation) to
   * a typed value, preserving its Java type where possible. Values are always treated as data,
   * never as SQL.
   */
  public static PreparedStatementValue fromObject(@Nullable Object value) {
    if (value == null) {
      return nullValue(Types.NULL);
    }
    if (value instanceof byte[]) {
      return ofBytes((byte[]) value);
    }
    if (value instanceof Boolean) {
      return ofBoolean((Boolean) value);
    }
    if (value instanceof BigDecimal) {
      return ofBigDecimal((BigDecimal) value);
    }
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      return ofLong(((Number) value).longValue());
    }
    if (value instanceof BigInteger) {
      return ofBigDecimal(new BigDecimal((BigInteger) value));
    }
    if (value instanceof Double || value instanceof Float) {
      // Float is widened via its decimal text so that e.g. 1.1f stays 1.1 (not 1.100000023841858).
      double d = (value instanceof Float) ? Double.parseDouble(value.toString()) : (Double) value;
      // Bound as a double so that very large / small magnitudes keep their scientific notation.
      // NaN and Infinity are bound as text (the database then reports the conversion error).
      return Double.isFinite(d) ? ofDouble(d) : ofString(value.toString());
    }
    return ofString(value.toString());
  }

  @Nullable
  public Object getValue() {
    return value;
  }

  public int getSqlType() {
    return sqlType;
  }

  public boolean isNull() {
    return value == null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PreparedStatementValue)) {
      return false;
    }
    PreparedStatementValue that = (PreparedStatementValue) o;
    return sqlType == that.sqlType && Objects.deepEquals(value, that.value);
  }

  @Override
  public int hashCode() {
    int valueHash =
        value instanceof byte[] ? Arrays.hashCode((byte[]) value) : Objects.hashCode(value);
    return 31 * valueHash + sqlType;
  }

  @Override
  public String toString() {
    String valueStr =
        value instanceof byte[] ? Arrays.toString((byte[]) value) : String.valueOf(value);
    return "PreparedStatementValue{value=" + valueStr + ", sqlType=" + sqlType + "}";
  }
}
