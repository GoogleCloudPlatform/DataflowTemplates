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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import net.datafaker.Faker;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.DateTimeConstants;
import org.joda.time.Instant;
import org.json.JSONObject;

/**
 * Common utilities for data generation — value synthesis and Beam type mapping.
 *
 * <p>Kept separate from the transform classes so that the generation rules can be unit-tested in
 * isolation and shared across {@code GeneratePrimaryKey}, (future) value-column generators, and any
 * other DoFn that needs to emit a synthetic value for a {@link DataGeneratorColumn}.
 */
public final class DataGeneratorUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
    if (column.fakerExpression() != null) {
      Object override = column.fakerExpression();
      if (override instanceof String) {
        return generateFromStringExpression((String) override, column, faker);
      }
      if (type == LogicalType.JSON) {
        try {
          Object resolvedTree = resolveJsonTemplateNode(override, faker);
          return MAPPER.writeValueAsString(resolvedTree);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to serialize nested JSON override for column " + column.name(), e);
        }
      }
      throw new IllegalArgumentException(
          "Unsupported nested override for non-JSON column '" + column.name() + "'.");
    } else {
      switch (type) {
        case STRING:
          return faker.lorem().characters(limitStringLength(size));
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
        case UUID:
          return UUID.randomUUID().toString();
        case INT64:
          return (long) ThreadLocalRandom.current().nextInt();
        case FLOAT64:
          {
            int scale = column.scale() != null ? column.scale() : Constants.DEFAULT_NUMERIC_SCALE;
            int precision =
                column.precision() != null
                    ? column.precision()
                    : Constants.DEFAULT_NUMERIC_PRECISION + 5;
            double maxVal = Math.pow(10, precision - scale) - 1.0 / Math.pow(10, scale);
            double minVal = -maxVal;
            return faker.number().randomDouble(scale, (long) minVal, (long) maxVal);
          }
        case NUMERIC:
          return generateNumeric(column, faker);
        case BOOLEAN:
          return faker.bool().bool();
        case BYTES:
          return faker.lorem().characters(limitStringLength(size)).getBytes(StandardCharsets.UTF_8);
        case DATE:
          {
            long minMillis = -2208988800000L; // Year 1900
            long maxMillis = 4133980799000L; // Year 2100
            long randomMillis = ThreadLocalRandom.current().nextLong(minMillis, maxMillis);
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
            long minMillis = -2208988800000L; // Year 1900
            long maxMillis = 4133980799000L; // Year 2100
            return new Instant(ThreadLocalRandom.current().nextLong(minMillis, maxMillis));
          }
        default:
          throw new UnsupportedOperationException(
              "Unsupported logical type for dynamic data generation: " + type);
      }
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
      case UUID:
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
            : Constants.DEFAULT_NUMERIC_PRECISION;
    int sc =
        (column.scale() != null && column.scale() >= 0)
            ? column.scale()
            : Constants.DEFAULT_NUMERIC_SCALE;
    if (sc > prec) {
      throw new IllegalArgumentException(
          "Scale "
              + sc
              + " cannot be greater than precision "
              + prec
              + " for numeric column '"
              + column.name()
              + "'.");
    }

    String randomDigits = faker.number().digits(prec);
    BigDecimal value = new BigDecimal(randomDigits).movePointLeft(sc);

    // Normalise the trailing scale so the returned value always reports the
    // declared scale.
    return value.setScale(sc, RoundingMode.HALF_UP);
  }

  /**
   * Limit user-supplied string lengths into a safe range. Unset / zero / absurdly-large values all
   * fall back to a reasonable default so Faker doesn't receive a pathological input.
   */
  @VisibleForTesting
  static int limitStringLength(Long size) {
    if (size == null || size <= 0) {
      return Constants.DEFAULT_STRING_LENGTH;
    }
    if (size > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return size.intValue();
  }

  private static Object resolveJsonTemplateNode(Object node, Faker faker) {
    if (node instanceof Map) {
      Map<String, Object> srcMap = (Map<String, Object>) node;
      Map<String, Object> resolvedMap = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : srcMap.entrySet()) {
        resolvedMap.put(entry.getKey(), resolveJsonTemplateNode(entry.getValue(), faker));
      }
      return resolvedMap;
    }

    if (node instanceof List) {
      List<Object> srcList = (List<Object>) node;
      List<Object> resolvedList = new ArrayList<>();
      for (Object item : srcList) {
        resolvedList.add(resolveJsonTemplateNode(item, faker));
      }
      return resolvedList;
    }

    if (node instanceof String) {
      String str = (String) node;
      if (str.contains("#{")) {
        return faker.expression(str);
      }
      return str;
    }

    return node;
  }

  static Object generateFromStringExpression(
      String expression, DataGeneratorColumn column, Faker faker) {
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
        case UUID:
          return raw;
        case INT64:
          return Long.parseLong(raw.trim().replace(",", ""));
        case FLOAT64:
          return Double.parseDouble(raw.trim().replace(",", ""));
        case NUMERIC:
          {
            BigDecimal bd = new BigDecimal(raw.trim().replace(",", ""));
            int sc =
                (column.scale() != null && column.scale() >= 0)
                    ? column.scale()
                    : Constants.DEFAULT_NUMERIC_SCALE;
            return bd.setScale(sc, RoundingMode.HALF_UP);
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
          return new Instant(
              LocalDate.parse(raw.trim()).toEpochDay() * DateTimeConstants.MILLIS_PER_DAY);
        case TIMESTAMP:
          return new Instant(java.time.Instant.parse(raw.trim()).toEpochMilli());
        default:
          throw new UnsupportedOperationException("Unsupported logical type: " + type);
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

  public static Object canonicalizeValue(Object v) {
    if (v == null) {
      return JSONObject.NULL;
    }

    // Convert byte[] to standard Base64 string
    if (v instanceof byte[] bytes) {
      return Base64.getEncoder().encodeToString(bytes);
    }

    // Safely extract bytes from ByteBuffer and convert to Base64
    if (v instanceof ByteBuffer bb) {
      return Base64.getEncoder().encodeToString(bb.array());
    }

    return JSONObject.wrap(v);
  }
}
