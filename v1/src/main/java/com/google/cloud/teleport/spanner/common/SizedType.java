/*
 * Copyright (C) 2023 Google LLC
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

import com.google.cloud.spanner.Dialect;

/** Describes a type with a size. */
public final class SizedType {
  public final Type type;
  public final Integer size;

  public SizedType(Type type, Integer size) {
    this.type = type;
    this.size = size;
  }

  public static String typeString(Type type, Integer size) {
    switch (type.getCode()) {
      case BOOL:
        return "BOOL";
      case PG_BOOL:
        return "boolean";
      case INT64:
        return "INT64";
      case PG_INT8:
        return "bigint";
      case FLOAT64:
        return "FLOAT64";
      case PG_FLOAT8:
        return "double precision";
      case STRING:
        return "STRING(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_VARCHAR:
        return "character varying" + (size == -1 ? "" : ("(" + Integer.toString(size) + ")"));
      case PG_TEXT:
        return "text";
      case BYTES:
        return "BYTES(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_BYTEA:
        return "bytea";
      case DATE:
        return "DATE";
      case PG_DATE:
        return "date";
      case TIMESTAMP:
        return "TIMESTAMP";
      case PG_TIMESTAMPTZ:
        return "timestamp with time zone";
      case PG_SPANNER_COMMIT_TIMESTAMP:
        return "spanner.commit_timestamp";
      case NUMERIC:
        return "NUMERIC";
      case PG_NUMERIC:
        return "numeric";
      case JSON:
        return "JSON";
      case PG_JSONB:
        return "jsonb";
      case ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return "ARRAY<" + typeString(arrayType, size) + ">";
        }
      case PG_ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return typeString(arrayType, size) + "[]";
        }
    }

    throw new IllegalArgumentException("Unknown type " + type);
  }

  private static SizedType t(Type type, Integer size) {
    return new SizedType(type, size);
  }

  public static SizedType parseSpannerType(String spannerType, Dialect dialect) {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        {
          if (spannerType.equals("BOOL")) {
            return t(Type.bool(), null);
          }
          if (spannerType.equals("INT64")) {
            return t(Type.int64(), null);
          }
          if (spannerType.equals("FLOAT64")) {
            return t(Type.float64(), null);
          }
          if (spannerType.startsWith("STRING")) {
            String sizeStr = spannerType.substring(7, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.string(), size);
          }
          if (spannerType.startsWith("BYTES")) {
            String sizeStr = spannerType.substring(6, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.bytes(), size);
          }
          if (spannerType.equals("TIMESTAMP")) {
            return t(Type.timestamp(), null);
          }
          if (spannerType.equals("DATE")) {
            return t(Type.date(), null);
          }
          if (spannerType.equals("NUMERIC")) {
            return t(Type.numeric(), null);
          }
          if (spannerType.equals("JSON")) {
            return t(Type.json(), null);
          }
          if (spannerType.startsWith("ARRAY")) {
            // Substring "ARRAY<xxx>"
            String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.array(itemType.type), itemType.size);
          }
          break;
        }
      case POSTGRESQL:
        {
          if (spannerType.endsWith("[]")) {
            // Substring "xxx[]"
            // Must check array type first
            String spannerArrayType = spannerType.substring(0, spannerType.length() - 2);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.pgArray(itemType.type), itemType.size);
          }
          if (spannerType.equals("boolean")) {
            return t(Type.pgBool(), null);
          }
          if (spannerType.equals("bigint")) {
            return t(Type.pgInt8(), null);
          }
          if (spannerType.equals("double precision")) {
            return t(Type.pgFloat8(), null);
          }
          if (spannerType.equals("text")) {
            return t(Type.pgText(), -1);
          }
          if (spannerType.startsWith("character varying")) {
            int size = -1;
            if (spannerType.length() > 18) {
              String sizeStr = spannerType.substring(18, spannerType.length() - 1);
              size = Integer.parseInt(sizeStr);
            }
            return t(Type.pgVarchar(), size);
          }
          if (spannerType.equals("bytea")) {
            return t(Type.pgBytea(), -1);
          }
          if (spannerType.equals("timestamp with time zone")) {
            return t(Type.pgTimestamptz(), null);
          }
          if (spannerType.equals("numeric")) {
            return t(Type.pgNumeric(), null);
          }
          if (spannerType.equals("jsonb")) {
            return t(Type.pgJsonb(), null);
          }
          if (spannerType.equals("date")) {
            return t(Type.pgDate(), null);
          }
          if (spannerType.equals("spanner.commit_timestamp")) {
            return t(Type.pgSpannerCommitTimestamp(), null);
          }
          break;
        }
      default:
        break;
    }
    throw new IllegalArgumentException("Unknown spanner type " + spannerType);
  }
}
