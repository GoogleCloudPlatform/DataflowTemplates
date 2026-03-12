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
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.util.Locale;
import javax.annotation.Nullable;

/** TypeMapper implementation for Spanner types. */
public class SpannerTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(
      String typeName, @Nullable Object dialectObj, @Nullable Long size) {
    if (!(dialectObj instanceof Dialect)) {
      throw new IllegalArgumentException(
          "Expected com.google.cloud.spanner.Dialect, got "
              + (dialectObj != null ? dialectObj.getClass().getName() : "null"));
    }
    Dialect dialect = (Dialect) dialectObj;
    if (typeName == null) {
      return LogicalType.STRING;
    }

    String upperType = typeName.toUpperCase(Locale.ROOT).trim();
    int splitIdx =
        Math.min(
            upperType.indexOf("(") == -1 ? Integer.MAX_VALUE : upperType.indexOf("("),
            upperType.indexOf("<") == -1 ? Integer.MAX_VALUE : upperType.indexOf("<"));

    if (splitIdx != Integer.MAX_VALUE) {
      upperType = upperType.substring(0, splitIdx).trim();
    }

    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      switch (upperType) {
        case "BOOL":
        case "BOOLEAN":
          return LogicalType.BOOLEAN;
        case "INT64":
        case "INT":
          return LogicalType.INT64;
        case "FLOAT32":
        case "FLOAT64":
          return LogicalType.FLOAT64;
        case "STRING":
          return LogicalType.STRING;
        case "BYTES":
          return LogicalType.BYTES;
        case "DATE":
          return LogicalType.DATE;
        case "TIMESTAMP":
          return LogicalType.TIMESTAMP;
        case "NUMERIC":
        case "BIGNUMERIC":
          return LogicalType.NUMERIC;
        case "JSON":
          return LogicalType.JSON;
          // Todo: Add support for array datatype
        default:
          return LogicalType.STRING;
      }
    } else if (dialect == Dialect.POSTGRESQL) {
      switch (upperType) {
        case "BOOLEAN":
        case "BOOL":
          return LogicalType.BOOLEAN;
        case "BIGINT":
        case "INT8":
        case "INTEGER":
        case "INT":
        case "INT4":
          return LogicalType.INT64;
        case "REAL":
        case "FLOAT4":
        case "DOUBLE PRECISION":
        case "FLOAT8":
          return LogicalType.FLOAT64;
        case "CHARACTER VARYING":
        case "VARCHAR":
        case "TEXT":
        case "CHARACTER":
        case "CHAR":
          return LogicalType.STRING;
        case "BYTEA":
          return LogicalType.BYTES;
        case "DATE":
          return LogicalType.DATE;
        case "TIMESTAMP":
        case "TIMESTAMP WITH TIME ZONE":
        case "TIMESTAMPTZ":
          return LogicalType.TIMESTAMP;
        case "NUMERIC":
        case "DECIMAL":
          return LogicalType.NUMERIC;
        case "JSONB":
          return LogicalType.JSON;
        default:
          return LogicalType.STRING;
      }
    }

    return LogicalType.STRING;
  }
}
