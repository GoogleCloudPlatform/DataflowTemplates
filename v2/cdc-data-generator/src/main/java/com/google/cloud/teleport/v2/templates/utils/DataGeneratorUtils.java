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
package com.google.cloud.teleport.v2.templates.utils;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

/**
 * Common utilities for data generation — value synthesis and Beam type mapping.
 *
 * <p>Kept separate from the transform classes so that the generation rules can be unit-tested in
 * isolation and shared across {@code GeneratePrimaryKey}, (future) value-column generators, and any
 * other DoFn that needs to emit a synthetic value for a {@link DataGeneratorColumn}.
 */
public final class DataGeneratorUtils {

  /** Default string length when the column doesn't declare a {@code size}. */
  @VisibleForTesting static final int DEFAULT_STRING_LENGTH = 20;

  /** Default precision/scale when the column doesn't declare them. */
  @VisibleForTesting static final int DEFAULT_NUMERIC_PRECISION = 10;

  @VisibleForTesting static final int DEFAULT_NUMERIC_SCALE = 2;

  private DataGeneratorUtils() {}

  /**
   * Synthesise a value for the given column using {@code faker}. Caller is responsible for seeding
   * the Faker instance.
   *
   * <p>The returned Java type matches what {@link #mapToBeamFieldType(LogicalType)} declares for
   * the same logical type, so the value can be added directly to a {@code Row.Builder}.
   */
  public static Object generateValue(DataGeneratorColumn column, Faker faker) {
    LogicalType type = column.logicalType();
    Long size = column.size();

    if (column.fakerExpression() != null && !column.fakerExpression().isEmpty()) {
      return generateFromExpression(column, faker);
    }

    switch (type) {
      case STRING:
        return faker.lorem().characters(clampStringLength(size));
      case JSON:
        return "{"
            + "\"id\": "
            + faker.number().randomNumber()
            + ", "
            + "\"name\": \""
            + faker.name().fullName()
            + "\", "
            + "\"isActive\": "
            + faker.bool().bool()
            + ", "
            + "\"score\": "
            + faker.number().randomDouble(2, 0, 100)
            + ", "
            + "\"tags\": [\""
            + faker.lorem().word()
            + "\", \""
            + faker.lorem().word()
            + "\"], "
            + "\"address\": {\"city\": \""
            + faker.address().city()
            + "\", \"zip\": \""
            + faker.address().zipCode()
            + "\"}"
            + "}";

      case INT64:
        return (long) java.util.concurrent.ThreadLocalRandom.current().nextInt();
      case FLOAT64:
        {
          int scale = column.scale() != null ? column.scale() : DEFAULT_NUMERIC_SCALE;
          int precision =
              column.precision() != null ? column.precision() : DEFAULT_NUMERIC_PRECISION + 5;
          double maxVal = Math.pow(10, precision - scale) - 1.0 / Math.pow(10, scale);
          double minVal = -maxVal;
          return faker.number().randomDouble(scale, (long) minVal, (long) maxVal);
        }
      case NUMERIC:
        return generateNumeric(column, faker);
      case BOOLEAN:
        return faker.bool().bool();
      case BYTES:
        return faker.lorem().characters(clampStringLength(size)).getBytes(StandardCharsets.UTF_8);
      case DATE:
        {
          long minMillis = -30610224000000L; // Year 1000
          long maxMillis = 253402300799000L; // Year 9999
          long randomMillis =
              java.util.concurrent.ThreadLocalRandom.current().nextLong(minMillis, maxMillis);
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(randomMillis);
          // Zero the time part — DATE is day-granular and TIMESTAMP uses the TIMESTAMP branch.
          cal.set(Calendar.HOUR_OF_DAY, 0);
          cal.set(Calendar.MINUTE, 0);
          cal.set(Calendar.SECOND, 0);
          cal.set(Calendar.MILLISECOND, 0);
          return new Instant(cal.getTimeInMillis());
        }
      case TIMESTAMP:
        {
          long minMillis = -30610224000000L; // Year 1000
          long maxMillis = 253402300799000L; // Year 9999
          return new Instant(
              java.util.concurrent.ThreadLocalRandom.current().nextLong(minMillis, maxMillis));
        }

      default:
        return "unknown";
    }
  }

  /**
   * Map a logical type to the matching Beam {@link Schema.FieldType}. Used to construct the Row
   * schema for synthesised values.
   */
  public static Schema.FieldType mapToBeamFieldType(LogicalType logicalType) {
    switch (logicalType) {
      case STRING:
      case JSON:
        return Schema.FieldType.STRING;
      case INT64:
        return Schema.FieldType.INT64;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case NUMERIC:
        return Schema.FieldType.DECIMAL;
      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;
      case BYTES:
        return Schema.FieldType.BYTES;
      case DATE:
      case TIMESTAMP:
        return Schema.FieldType.DATETIME;
      default:
        return Schema.FieldType.STRING;
    }
  }

  /** Convenience overload accepting the full column — kept so callers don't branch at callsite. */
  public static Schema.FieldType mapToBeamFieldType(DataGeneratorColumn column) {

    return mapToBeamFieldType(column.logicalType());
  }

  /**
   * Generate a {@link BigDecimal} that fits the column's declared precision and scale. Avoids
   * {@code new BigDecimal(faker.number().randomNumber())} which returns an unbounded integer value
   * and overflows most DECIMAL/NUMERIC columns (e.g. {@code DECIMAL(5,2)} tops out at {@code
   * 999.99}).
   *
   * <p>Defaults are precision 10 / scale 2 when the fetcher did not populate them — safe for MySQL
   * {@code DECIMAL} (default precision is 10) and Spanner {@code NUMERIC} (up to 38 precision, 9
   * scale).
   */
  @VisibleForTesting
  static BigDecimal generateNumeric(DataGeneratorColumn column, Faker faker) {
    int prec =
        (column.precision() != null && column.precision() > 0)
            ? column.precision()
            : DEFAULT_NUMERIC_PRECISION;
    int sc =
        (column.scale() != null && column.scale() >= 0) ? column.scale() : DEFAULT_NUMERIC_SCALE;
    if (sc > prec) {
      sc = prec;
    }

    String randomDigits = faker.number().digits(prec);
    BigDecimal value = new BigDecimal(randomDigits).movePointLeft(sc);

    // Normalise the trailing scale so the returned value always reports the
    // declared scale.
    return value.setScale(sc, RoundingMode.HALF_UP);
  }

  /**
   * Clamp user-supplied string lengths into a safe range. Unset or zero values fall back to a
   * reasonable default, while values larger than Integer.MAX_VALUE are capped at Integer.MAX_VALUE
   * so Faker doesn't receive an invalid input.
   */
  private static int clampStringLength(Long size) {
    if (size == null || size <= 0) {
      return DEFAULT_STRING_LENGTH;
    }
    if (size > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return size.intValue();
  }

  /**
   * Evaluates a user-provided Faker expression and converts the resulting String into the column's
   * logical type.
   *
   * <p>{@link Faker#expression(String)} returns a String even when the directive is conceptually
   * typed (e.g. {@code number.numberBetween} returns a long internally but is surfaced as a
   * decimal-string). For columns whose logical type isn't STRING, parsing is the only way to
   * recover the typed value.
   *
   * <p>For ENUM and ARRAY logical types we fall back to the random generator — the user-facing
   * conversion contract for those is fuzzier and the randomised path is type-safe by construction.
   */
  static Object generateFromExpression(DataGeneratorColumn column, Faker faker) {
    String expression = column.fakerExpression();
    if (!expression.startsWith("#{")) {
      return convertToLogicalType(expression, column); // Treat as literal
    }
    String raw;
    try {
      raw = faker.expression(expression);
    } catch (Exception e) {
      throw new RuntimeException(
          "Faker expression for column '" + column.name() + "' failed to evaluate: " + expression,
          e);
    }
    return convertToLogicalType(raw, column);
  }

  private static Object convertToLogicalType(String raw, DataGeneratorColumn column) {
    if (raw == null) {
      throw new IllegalArgumentException("Cannot convert null value for column " + column.name());
    }
    LogicalType type = column.logicalType();
    try {
      switch (type) {
        case STRING:
        case JSON:
          return raw;
        case INT64:
          return Long.parseLong(raw.trim());
        case FLOAT64:
          return Double.parseDouble(raw.trim());
        case NUMERIC:
          {
            BigDecimal bd = new BigDecimal(raw.trim());
            int sc = (column.scale() != null && column.scale() >= 0) ? column.scale() : 2;
            return bd.setScale(sc, java.math.RoundingMode.HALF_UP);
          }
        case BOOLEAN:
          {
            String lower = raw.trim().toLowerCase();
            if ("true".equals(lower) || "false".equals(lower)) {
              return Boolean.parseBoolean(lower);
            }
            throw new IllegalArgumentException("not a boolean: " + raw);
          }
        case BYTES:
          return raw.getBytes(StandardCharsets.UTF_8);
        case DATE:
          return new Instant(java.time.LocalDate.parse(raw.trim()).toEpochDay() * 86400000L);
        case TIMESTAMP:
          return new Instant(java.time.Instant.parse(raw.trim()).toEpochMilli());
        default:
          return raw;
      }
    } catch (IllegalArgumentException | java.time.DateTimeException e) {
      throw new RuntimeException(
          "Generator output for column '"
              + column.name()
              + "' did not parse as "
              + type
              + ": got '"
              + raw
              + "' from expression '"
              + column.fakerExpression()
              + "'",
          e);
    }
  }
}
