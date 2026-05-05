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
import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTimeConstants;
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
    if (column.fakerExpression() == null) {
      throw new IllegalArgumentException(
          "Faker expression is required for column '" + column.name() + "' in this branch.");
    }
    Object override = column.fakerExpression();
    if (override instanceof String) {
      return generateFromStringExpression((String) override, column, faker);
    }
    if (column.logicalType() == LogicalType.JSON) {
      try {
        Object resolvedTree = resolveJsonTemplateNode(override, faker);
        return new ObjectMapper().writeValueAsString(resolvedTree);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to serialize rich nested JSON override for column " + column.name(), e);
      }
    }
    throw new IllegalArgumentException(
        "Unsupported rich nested override for non-JSON column '" + column.name() + "'.");
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
          return raw;
        case INT64:
          return Long.parseLong(raw.trim());
        case FLOAT64:
          return Double.parseDouble(raw.trim());
        case NUMERIC:
          {
            BigDecimal bd = new BigDecimal(raw.trim());
            int sc =
                (column.scale() != null && column.scale() >= 0)
                    ? column.scale()
                    : DEFAULT_NUMERIC_SCALE;
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
}
