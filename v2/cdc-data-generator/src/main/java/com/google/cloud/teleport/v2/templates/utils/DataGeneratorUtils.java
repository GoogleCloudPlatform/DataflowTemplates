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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

/**
 * Common utilities for data generation — value synthesis and Beam type mapping.
 *
 * <p>Kept separate from the transform classes so that the generation rules can be unit-tested in
 * isolation and shared across {@code GeneratePrimaryKey}, (future) value-column generators, and
 * any other DoFn that needs to emit a synthetic value for a {@link DataGeneratorColumn}.
 */
public final class DataGeneratorUtils {

  /** Default string length when the column doesn't declare a {@code size}. */
  @VisibleForTesting static final int DEFAULT_STRING_LENGTH = 20;

  /** Cap on string length for synthesised values. Columns wider than this use the cap. */
  @VisibleForTesting static final int MAX_STRING_LENGTH = 1000;

  /** Default precision/scale when the column doesn't declare them. */
  @VisibleForTesting static final int DEFAULT_NUMERIC_PRECISION = 10;

  @VisibleForTesting static final int DEFAULT_NUMERIC_SCALE = 2;

  /** INT64 sub-range. Deliberately narrow to avoid accidentally producing negative PKs. */
  @VisibleForTesting static final long INT64_MIN = 1_000_000_000L;

  @VisibleForTesting static final long INT64_MAX = 2_147_483_647L;

  /** Number of days back from today that random DATE/TIMESTAMP values can fall on. */
  @VisibleForTesting static final int DATE_RANGE_DAYS = 365 * 2;

  private DataGeneratorUtils() {}

  /**
   * Synthesise a value for the given column using {@code faker}. Caller is responsible for
   * seeding the Faker instance (see {@link SeedUtils}).
   *
   * <p>The returned Java type matches what {@link #mapToBeamFieldType(LogicalType)} declares for
   * the same logical type, so the value can be added directly to a {@code Row.Builder}.
   */
  public static Object generateValue(DataGeneratorColumn column, Faker faker) {
    LogicalType type = column.logicalType();
    Long size = column.size();

    switch (type) {
      case STRING:
        return faker.lorem().characters(clampStringLength(size));
      case JSON:
        // Produce valid JSON so downstream JSON-typed columns don't reject the payload. Use
        // {@code randomNumber()} for uniqueness; the key is fixed because callers typically use
        // JSON as an opaque blob.
        return "{\"id\": " + faker.number().randomNumber() + "}";
      case INT64:
        return faker.number().numberBetween(INT64_MIN, INT64_MAX);
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
          Date pastDate = faker.date().past(DATE_RANGE_DAYS, TimeUnit.DAYS);
          Calendar cal = Calendar.getInstance();
          cal.setTime(pastDate);
          // Zero the time part — DATE is day-granular and TIMESTAMP uses the TIMESTAMP branch.
          cal.set(Calendar.HOUR_OF_DAY, 0);
          cal.set(Calendar.MINUTE, 0);
          cal.set(Calendar.SECOND, 0);
          cal.set(Calendar.MILLISECOND, 0);
          return new Instant(cal.getTimeInMillis());
        }
      case TIMESTAMP:
        return new Instant(faker.date().past(DATE_RANGE_DAYS, TimeUnit.DAYS).getTime());
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
   * {@code new BigDecimal(faker.number().randomNumber())} which returns an unbounded integer
   * value and overflows most DECIMAL/NUMERIC columns (e.g. {@code DECIMAL(5,2)} tops out at
   * {@code 999.99}).
   *
   * <p>Defaults are precision 10 / scale 2 when the fetcher did not populate them — safe for
   * MySQL {@code DECIMAL} (default precision is 10) and Spanner {@code NUMERIC} (up to 38
   * precision, 9 scale).
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
    int intDigits = prec - sc;
    // Cap to 18 digits so the randomised upper bound stays within {@code long}.
    int capIntDigits = Math.min(intDigits, 18);
    long maxIntPart = 1L;
    for (int i = 0; i < capIntDigits; i++) {
      maxIntPart *= 10L;
    }
    long intPart = maxIntPart <= 1L ? 0L : faker.number().numberBetween(0L, maxIntPart);
    BigDecimal value = BigDecimal.valueOf(intPart);
    if (sc > 0) {
      int capScaleDigits = Math.min(sc, 18);
      long maxFrac = 1L;
      for (int i = 0; i < capScaleDigits; i++) {
        maxFrac *= 10L;
      }
      long fracPart = maxFrac <= 1L ? 0L : faker.number().numberBetween(0L, maxFrac);
      value = value.add(BigDecimal.valueOf(fracPart, capScaleDigits));
    }
    // Normalise the trailing scale so the returned value always reports the declared scale.
    return value.setScale(sc, RoundingMode.HALF_UP);
  }

  /**
   * Clamp user-supplied string lengths into a safe range. Unset / zero / absurdly-large values
   * all fall back to a reasonable default so Faker doesn't receive a pathological input.
   */
  private static int clampStringLength(Long size) {
    if (size == null || size <= 0) {
      return DEFAULT_STRING_LENGTH;
    }
    if (size >= MAX_STRING_LENGTH) {
      return MAX_STRING_LENGTH;
    }
    return size.intValue();
  }
}
